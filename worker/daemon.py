"""
TRAID Privileged Worker Daemon

Runs as root. Binds a Unix Domain Socket, accepts JSON requests from the
unprivileged FastAPI process, validates each request against the command
whitelist, executes system tools, and returns results.

Never eval()s or shell()s user input. All subprocess execution goes through
disk_ops.run_privileged() which uses asyncio.create_subprocess_exec.

Socket permissions: 0o660, group=www-data, so only the API process can write.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
import uuid
from pathlib import Path

from pythonjsonlogger import jsonlogger

from shared.protocol import send_message, recv_message, ProtocolError
from worker.command_validator import validate_request, ValidationError
from worker import disk_ops
from worker import fs_ops
from worker import vm_ops
from worker import docker_ops
from worker import backup_scheduler
from worker import sharing_ops
from worker.mdstat_reader import MdstatReader
from worker.traid_algorithm import calculate_traid
from worker.traid_algorithm import (
    generate_parted_commands,
    generate_mdadm_commands,
    generate_lvm_commands,
)

SOCKET_PATH = Path("/run/traid.sock")
SOCKET_GROUP = "www-data"
REPORT_DIR = disk_ops.REPORT_DIR


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler])


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

_mdstat_reader: MdstatReader = MdstatReader()
_active_jobs: dict[str, asyncio.Task] = {}
_job_history: dict[str, dict] = {}

# One destructive operation at a time.  SMART tests bypass this lock.
_array_lock: asyncio.Lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Job helpers
# ---------------------------------------------------------------------------

def _new_job(operation: str, **extra) -> str:
    job_id = str(uuid.uuid4())
    _job_history[job_id] = {
        "job_id": job_id,
        "status": "accepted",
        "step": "queued",
        "operation": operation,
        "type": extra.pop("type", operation),
        "disks": extra.pop("disks", []),
        "vg_name": extra.pop("vg_name", None),
        "started_at": None,
        "finished_at": None,
        "error": None,
        "accepted_at": time.time(),
        "progress_pct": 0,
        "report_url": None,
        **extra,
    }
    return job_id


def _make_updater(job_id: str):
    def update(step: str, pct: float = None):
        entry = _job_history.get(job_id)
        if entry:
            entry["step"] = step
            if pct is not None:
                entry["progress_pct"] = round(min(100, max(0, pct)))
    return update


async def _exec_job(job_id: str, coro) -> None:
    """Run coro, updating job history with status/timestamps/error."""
    _job_history[job_id]["status"] = "running"
    _job_history[job_id]["started_at"] = time.time()
    try:
        await coro
        _job_history[job_id]["status"] = "complete"
        _job_history[job_id]["step"] = "done"
        _job_history[job_id]["progress_pct"] = 100
    except asyncio.CancelledError:
        _job_history[job_id]["status"] = "cancelled"
        _job_history[job_id]["step"] = "Cancelled"
        raise
    except Exception as exc:
        logger.exception("job %s failed", job_id)
        _job_history[job_id]["status"] = "failed"
        _job_history[job_id]["error"] = str(exc)
    finally:
        _job_history[job_id]["finished_at"] = time.time()
        _active_jobs.pop(job_id, None)


def _launch_locked(job_id: str, coro) -> bool:
    """
    Try to launch a job that needs _array_lock.
    Returns False (job NOT started) if the lock is already held.
    The lock is acquired inside the spawned task, so the caller never blocks.
    """
    if _array_lock.locked():
        return False

    async def _wrapper():
        await _array_lock.acquire()
        try:
            await _exec_job(job_id, coro)
        finally:
            try:
                _array_lock.release()
            except RuntimeError:
                pass

    task = asyncio.create_task(_wrapper(), name=f"job-{job_id[:8]}")
    _active_jobs[job_id] = task
    return True


def _launch_free(job_id: str, coro) -> None:
    """Launch a job that does NOT need the array lock (e.g. SMART test)."""
    task = asyncio.create_task(
        _exec_job(job_id, coro), name=f"job-{job_id[:8]}"
    )
    _active_jobs[job_id] = task


def _busy_response():
    return {"accepted": False, "reason": "ARRAY_BUSY",
            "message": "Another operation is in progress. Try again when the current job finishes."}


# ---------------------------------------------------------------------------
# Handler: disk_scan
# ---------------------------------------------------------------------------

async def _handle_disk_scan(_params: dict) -> dict:
    disks = await disk_ops.scan_disks()
    return {"disks": disks}


# ---------------------------------------------------------------------------
# Handler: array_detail
# ---------------------------------------------------------------------------

async def _handle_array_detail(params: dict) -> dict:
    return await disk_ops.get_array_detail(params["device"])


# ---------------------------------------------------------------------------
# Handler: lvm_report
# ---------------------------------------------------------------------------

async def _handle_lvm_report(_params: dict) -> dict:
    return await disk_ops.get_lvm_report()


# ---------------------------------------------------------------------------
# Handler: array_create
# ---------------------------------------------------------------------------

async def _execute_creation(
    job_id: str, disks: list[str], redundancy: int, vg_name: str
) -> None:
    upd = _make_updater(job_id)
    sizes = []
    for disk in disks:
        sizes.append(await disk_ops.get_disk_size(disk))

    md_start = await disk_ops.next_free_md_index()
    plan = calculate_traid(sizes, redundancy=redundancy, vg_name=vg_name, md_start=md_start)

    upd("Partitioning disks…", 5.0)
    for i, disk in enumerate(disks):
        for cmd in generate_parted_commands(disk, i, plan):
            rc, _, err = await disk_ops.run_privileged(cmd[0], cmd[1:])
            if rc != 0:
                raise RuntimeError(f"parted: {err.strip()}")

    await asyncio.sleep(1)
    for disk in disks:
        await disk_ops.run_privileged("partprobe", [disk])
    await asyncio.sleep(2)

    upd("Creating RAID arrays…", 40.0)
    for cmd in generate_mdadm_commands(plan, disks):
        rc, _, err = await disk_ops.run_privileged(cmd[0], cmd[1:])
        if rc != 0:
            raise RuntimeError(f"mdadm: {err.strip()}")
    await asyncio.sleep(2)

    upd("Creating LVM volumes…", 70.0)
    for cmd in generate_lvm_commands(plan):
        rc, _, err = await disk_ops.run_privileged(cmd[0], cmd[1:])
        if rc != 0:
            raise RuntimeError(f"lvm: {err.strip()}")

    upd("Done", 100.0)


async def _handle_array_create(params: dict) -> dict:
    disks: list[str] = params["disks"]
    redundancy = 1 if params["type"] == "traid1" else 2
    vg_name: str = params.get("vg_name", "traid_vg")

    job_id = _new_job(
        "array_create", disks=disks, type=params["type"], vg_name=vg_name
    )
    coro = _execute_creation(job_id, disks, redundancy, vg_name)

    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    logger.info("job %s: accepted array_create vg=%s", job_id, vg_name)
    return {"accepted": True, "job_id": job_id}


# ---------------------------------------------------------------------------
# Handler: array_delete
# ---------------------------------------------------------------------------

async def _handle_array_delete(params: dict) -> dict:
    vg_name = params["vg_name"]
    job_id = _new_job("array_delete", vg_name=vg_name, type="array_delete")
    upd = _make_updater(job_id)

    async def _delete():
        upd("Tearing down array…", 10.0)
        await disk_ops.delete_array(vg_name)
        upd("Done", 100.0)

    if not _launch_locked(job_id, _delete()):
        del _job_history[job_id]
        return _busy_response()

    logger.info("job %s: accepted array_delete vg=%s", job_id, vg_name)
    return {"accepted": True, "job_id": job_id, "vg_name": vg_name}


# ---------------------------------------------------------------------------
# Handler: vg_rename
# ---------------------------------------------------------------------------

async def _handle_vg_rename(params: dict) -> dict:
    return await disk_ops.rename_vg(params["vg_name"], params["new_name"])


# ---------------------------------------------------------------------------
# Handler: jobs_list / job_delete
# ---------------------------------------------------------------------------

async def _handle_jobs_list(_params: dict) -> dict:
    return {"jobs": list(_job_history.values())}


async def _handle_job_delete(params: dict) -> dict:
    job_id = params["job_id"]
    if job_id not in _job_history:
        return {"deleted": False, "reason": "not found"}
    task = _active_jobs.get(job_id)
    if task and not task.done():
        task.cancel()
        return {"cancelled": True, "job_id": job_id}
    del _job_history[job_id]
    return {"deleted": True, "job_id": job_id}


# ---------------------------------------------------------------------------
# Handler: mdstat_subscribe
# ---------------------------------------------------------------------------

async def _handle_mdstat_subscribe(
    _params: dict,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    q = _mdstat_reader.subscribe()
    try:
        while True:
            try:
                event = await asyncio.wait_for(q.get(), timeout=5.0)
            except asyncio.TimeoutError:
                event = {"event": "keepalive"}
            try:
                await send_message(writer, event)
            except (ConnectionResetError, BrokenPipeError, OSError):
                break
            if writer.is_closing():
                break
    finally:
        _mdstat_reader.unsubscribe(q)


# ---------------------------------------------------------------------------
# New operation handlers
# ---------------------------------------------------------------------------

async def _handle_array_migrate(params: dict) -> dict:
    vg_name = params["vg_name"]
    direction = params["direction"]
    new_disk = params.get("new_disk")

    if direction == "traid1_to_traid2" and not new_disk:
        raise ValidationError("new_disk is required for traid1_to_traid2 migration")

    job_id = _new_job(
        "array_migrate", vg_name=vg_name,
        disks=([new_disk] if new_disk else []),
        type="array_migrate", direction=direction,
    )
    upd = _make_updater(job_id)

    if direction == "traid1_to_traid2":
        coro = disk_ops.migrate_to_traid2(vg_name, new_disk, upd)
    else:
        coro = disk_ops.migrate_to_traid1(vg_name, upd)

    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    logger.info("job %s: accepted migrate %s %s", job_id, vg_name, direction)
    return {"accepted": True, "job_id": job_id}


async def _handle_disk_replace(params: dict) -> dict:
    vg_name = params["vg_name"]
    old_disk = params["old_disk"]
    new_disk = params["new_disk"]

    job_id = _new_job(
        "disk_replace", vg_name=vg_name,
        disks=[old_disk, new_disk], type="disk_replace",
    )
    coro = disk_ops.disk_replace(vg_name, old_disk, new_disk, _make_updater(job_id))

    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    return {"accepted": True, "job_id": job_id}


async def _handle_array_grow(params: dict) -> dict:
    vg_name = params["vg_name"]
    new_disk = params["new_disk"]

    job_id = _new_job(
        "array_grow", vg_name=vg_name, disks=[new_disk], type="array_grow"
    )
    coro = disk_ops.array_grow(vg_name, new_disk, _make_updater(job_id))

    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    return {"accepted": True, "job_id": job_id}


async def _handle_array_shrink(params: dict) -> dict:
    vg_name = params["vg_name"]
    disk = params["disk_to_remove"]

    job_id = _new_job(
        "array_shrink", vg_name=vg_name, disks=[disk], type="array_shrink"
    )
    coro = disk_ops.array_shrink(vg_name, disk, _make_updater(job_id))

    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    return {"accepted": True, "job_id": job_id}


async def _handle_volume_clone(params: dict) -> dict:
    vg_name = params["vg_name"]
    target_disk = params["target_disk"]

    job_id = _new_job(
        "volume_clone", vg_name=vg_name, disks=[target_disk], type="volume_clone"
    )
    coro = disk_ops.volume_clone(vg_name, target_disk, _make_updater(job_id))

    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    return {"accepted": True, "job_id": job_id}


async def _handle_volume_backup(params: dict) -> dict:
    vg_name = params["vg_name"]
    job_id = _new_job(
        "volume_backup", vg_name=vg_name, disks=[], type="volume_backup",
        host=params["host"], remote_path=params["remote_path"],
        protocol=params["protocol"],
    )
    coro = disk_ops.volume_backup(
        vg_name,
        params["protocol"], params["host"], params["remote_path"],
        params.get("cifs_user", ""), params.get("cifs_pass", ""),
        _make_updater(job_id),
    )
    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    return {"accepted": True, "job_id": job_id}


async def _handle_smart_test(params: dict) -> dict:
    """SMART tests are read-only — no array lock needed."""
    disk = params["disk"]
    test_type = params["test_type"]
    job_id = _new_job(
        "smart_test", disks=[disk], type=f"smart_{test_type}", target_disk=disk
    )
    report_path = REPORT_DIR / f"{job_id}_smart_{test_type}.txt"
    _job_history[job_id]["report_url"] = f"/api/reports/{report_path.name}"
    upd = _make_updater(job_id)

    _launch_free(job_id, disk_ops.smart_test(disk, test_type, report_path, upd))
    return {"accepted": True, "job_id": job_id}


async def _handle_badblocks_test(params: dict) -> dict:
    disk = params["disk"]
    job_id = _new_job("badblocks_test", disks=[disk], type="badblocks", target_disk=disk)
    report_path = REPORT_DIR / f"{job_id}_badblocks.txt"
    _job_history[job_id]["report_url"] = f"/api/reports/{report_path.name}"

    coro = disk_ops.badblocks_test(disk, report_path, _make_updater(job_id))
    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    return {"accepted": True, "job_id": job_id}


async def _handle_disk_erase(params: dict) -> dict:
    disk = params["disk"]
    mode = params.get("mode", "dod_short")
    job_id = _new_job("disk_erase", disks=[disk], type="disk_erase", target_disk=disk)
    report_path = REPORT_DIR / f"{job_id}_erase.txt"
    _job_history[job_id]["report_url"] = f"/api/reports/{report_path.name}"

    coro = disk_ops.disk_erase(disk, report_path, _make_updater(job_id), mode=mode)
    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()

    return {"accepted": True, "job_id": job_id}


# ---------------------------------------------------------------------------
# Handlers: filesystem management
# ---------------------------------------------------------------------------

async def _handle_fs_format(params: dict) -> dict:
    vg_name = params["vg_name"]
    fstype = params["fstype"]
    job_id = _new_job("fs_format", vg_name=vg_name, type="fs_format")
    coro = fs_ops.format_volume(
        vg_name, fstype,
        label=params.get("label", ""),
        compression=params.get("compression", ""),
    )
    if not _launch_locked(job_id, coro):
        del _job_history[job_id]
        return _busy_response()
    return {"accepted": True, "job_id": job_id}


async def _handle_fs_mount(params: dict) -> dict:
    return await fs_ops.mount_volume(params["vg_name"])


async def _handle_fs_unmount(params: dict) -> dict:
    return await fs_ops.unmount_volume(params["vg_name"])


async def _handle_fs_info(params: dict) -> dict:
    return await fs_ops.get_fs_info(params["vg_name"])


async def _handle_fs_set_compression(params: dict) -> dict:
    return await fs_ops.btrfs_set_compression(params["vg_name"], params["compression"])


# ---------------------------------------------------------------------------
# Handlers: btrfs subvolumes / snapshots
# ---------------------------------------------------------------------------

async def _handle_btrfs_subvol_list(params: dict) -> dict:
    subvols = await fs_ops.btrfs_list_subvolumes(params["vg_name"])
    return {"subvolumes": subvols}


async def _handle_btrfs_subvol_create(params: dict) -> dict:
    return await fs_ops.btrfs_create_subvolume(params["vg_name"], params["name"])


async def _handle_btrfs_subvol_delete(params: dict) -> dict:
    return await fs_ops.btrfs_delete_subvolume(
        params["vg_name"], params["path"],
        recursive=params.get("recursive", False),
    )


async def _handle_btrfs_snapshot_create(params: dict) -> dict:
    return await fs_ops.btrfs_create_snapshot(
        params["vg_name"], params["source_path"], params["dest_path"],
        readonly=params.get("readonly", False),
    )


async def _handle_btrfs_subvol_set_default(params: dict) -> dict:
    return await fs_ops.btrfs_set_default_subvolume(
        params["vg_name"], params["subvol_id"]
    )


# ---------------------------------------------------------------------------
# Handlers: btrfs maintenance (scrub / balance / defrag / dedup)
# ---------------------------------------------------------------------------

async def _handle_btrfs_scrub_start(params: dict) -> dict:
    vg_name = params["vg_name"]
    job_id = _new_job("btrfs_scrub", vg_name=vg_name, type="btrfs_scrub")
    coro = fs_ops.btrfs_scrub_start(vg_name, _make_updater(job_id))
    _launch_free(job_id, coro)
    return {"accepted": True, "job_id": job_id}


async def _handle_btrfs_scrub_status(params: dict) -> dict:
    return await fs_ops.btrfs_scrub_status(params["vg_name"])


async def _handle_btrfs_scrub_cancel(params: dict) -> dict:
    return await fs_ops.btrfs_scrub_cancel(params["vg_name"])


async def _handle_btrfs_scrub_pause(params: dict) -> dict:
    return await fs_ops.btrfs_scrub_pause(params["vg_name"])


async def _handle_btrfs_scrub_resume(params: dict) -> dict:
    return await fs_ops.btrfs_scrub_resume(params["vg_name"])


async def _handle_btrfs_scrub_last_result(params: dict) -> dict:
    return await fs_ops.btrfs_scrub_last_result(params["vg_name"])


async def _handle_btrfs_balance_start(params: dict) -> dict:
    vg_name = params["vg_name"]
    job_id = _new_job("btrfs_balance", vg_name=vg_name, type="btrfs_balance")
    coro = fs_ops.btrfs_balance_start(
        vg_name,
        usage_filter=params.get("usage_filter"),
        metadata_usage=params.get("metadata_usage"),
        update_fn=_make_updater(job_id),
    )
    _launch_free(job_id, coro)
    return {"accepted": True, "job_id": job_id}


async def _handle_btrfs_balance_status(params: dict) -> dict:
    return await fs_ops.btrfs_balance_status(params["vg_name"])


async def _handle_btrfs_balance_cancel(params: dict) -> dict:
    return await fs_ops.btrfs_balance_cancel(params["vg_name"])


async def _handle_btrfs_defrag(params: dict) -> dict:
    vg_name = params["vg_name"]
    job_id = _new_job("btrfs_defrag", vg_name=vg_name, type="btrfs_defrag")
    coro = fs_ops.btrfs_defrag(
        vg_name,
        path=params.get("path", ""),
        recursive=params.get("recursive", True),
        compression=params.get("compression", ""),
        update_fn=_make_updater(job_id),
    )
    _launch_free(job_id, coro)
    return {"accepted": True, "job_id": job_id}


async def _handle_btrfs_dedup(params: dict) -> dict:
    vg_name = params["vg_name"]
    job_id = _new_job("btrfs_dedup", vg_name=vg_name, type="btrfs_dedup")
    coro = fs_ops.btrfs_dedup(
        vg_name, path=params.get("path", ""), update_fn=_make_updater(job_id)
    )
    _launch_free(job_id, coro)
    return {"accepted": True, "job_id": job_id}


# ---------------------------------------------------------------------------
# Handlers: btrfs quotas
# ---------------------------------------------------------------------------

async def _handle_btrfs_quota_enable(params: dict) -> dict:
    return await fs_ops.btrfs_quota_enable(params["vg_name"])


async def _handle_btrfs_quota_list(params: dict) -> dict:
    quotas = await fs_ops.btrfs_quota_list(params["vg_name"])
    return {"quotas": quotas}


async def _handle_btrfs_quota_set(params: dict) -> dict:
    return await fs_ops.btrfs_quota_set(
        params["vg_name"], params["qgroup"], params["limit_bytes"]
    )


# ---------------------------------------------------------------------------
# Handlers: btrfs usage / stats
# ---------------------------------------------------------------------------

async def _handle_btrfs_usage_detail(params: dict) -> dict:
    return await fs_ops.btrfs_usage_detail(params["vg_name"])


# ---------------------------------------------------------------------------
# Handlers: btrfs send / receive
# ---------------------------------------------------------------------------

async def _handle_btrfs_send(params: dict) -> dict:
    vg_name = params["vg_name"]
    job_id = _new_job("btrfs_send", vg_name=vg_name, type="btrfs_send")
    coro = fs_ops.btrfs_send(
        vg_name,
        snapshot_path=params["snapshot_path"],
        dest_file=params["dest_file"],
        parent_path=params.get("parent_path"),
        update_fn=_make_updater(job_id),
    )
    _launch_free(job_id, coro)
    return {"accepted": True, "job_id": job_id}


async def _handle_btrfs_receive(params: dict) -> dict:
    vg_name = params["vg_name"]
    job_id = _new_job("btrfs_receive", vg_name=vg_name, type="btrfs_receive")
    coro = fs_ops.btrfs_receive(
        vg_name, source_file=params["source_file"], update_fn=_make_updater(job_id)
    )
    _launch_free(job_id, coro)
    return {"accepted": True, "job_id": job_id}


# ---------------------------------------------------------------------------
# Handlers: VM management
# ---------------------------------------------------------------------------

async def _handle_vm_install_iso(params: dict) -> dict:
    import os
    import shutil
    src = Path(params["src_path"])
    if not src.exists():
        raise FileNotFoundError(f"Temp ISO not found: {src}")
    dest = vm_ops.ISO_DIR / params["filename"]
    vm_ops.ISO_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest))
    # Make the ISO world-readable and root-owned so libvirt/qemu can
    # read it — the upload spool was www-data-owned with 0600 perms.
    os.chown(dest, 0, 0)
    os.chmod(dest, 0o644)
    return {"filename": params["filename"], "ok": True}


async def _handle_vm_list(_params: dict) -> dict:
    return {"vms": await vm_ops.list_vms()}


async def _handle_vm_info(params: dict) -> dict:
    return await vm_ops.vm_info(params["name"])


async def _handle_vm_action(params: dict) -> dict:
    return await vm_ops.vm_action(params["name"], params["action"])


async def _handle_vm_list_isos(_params: dict) -> dict:
    return {"isos": await vm_ops.list_isos()}


async def _handle_vm_create(params: dict) -> dict:
    name = params["name"]
    job_id = _new_job("vm_create", type="vm_create", vg_name=None)
    coro = vm_ops.create_vm(
        name, params["iso"], params["ram_mb"],
        params["vcpus"], params["disk_gb"],
        update_fn=_make_updater(job_id),
    )
    _launch_free(job_id, coro)
    return {"accepted": True, "job_id": job_id}


async def _handle_vm_delete(params: dict) -> dict:
    return await vm_ops.vm_delete(
        params["name"], keep_storage=params.get("keep_storage", False)
    )


# ---------------------------------------------------------------------------
# Handlers: Docker
# ---------------------------------------------------------------------------

async def _handle_docker_create_container(params: dict) -> dict:
    return await docker_ops.create_container(
        image=params["image"],
        name=params.get("name", ""),
        ports=params.get("ports", []),
        restart=params.get("restart", "no"),
        env_vars=params.get("env_vars", []),
    )


async def _handle_docker_list_containers(params: dict) -> dict:
    containers = await docker_ops.list_containers(
        all_containers=params.get("all", True)
    )
    return {"containers": containers}


async def _handle_docker_container_action(params: dict) -> dict:
    return await docker_ops.container_action(params["container_id"], params["action"])


async def _handle_docker_container_logs(params: dict) -> dict:
    return await docker_ops.container_logs(
        params["container_id"], lines=params.get("lines", 200)
    )


async def _handle_docker_list_images(_params: dict) -> dict:
    return {"images": await docker_ops.list_images()}


async def _handle_docker_pull_image(params: dict) -> dict:
    image = params["image"]
    job_id = _new_job("docker_pull", type="docker_pull")
    _launch_free(job_id, docker_ops.pull_image(image, _make_updater(job_id)))
    return {"accepted": True, "job_id": job_id}


async def _handle_docker_remove_image(params: dict) -> dict:
    return await docker_ops.remove_image(
        params["image_id"], force=params.get("force", False)
    )


async def _handle_docker_system_prune(_params: dict) -> dict:
    job_id = _new_job("docker_prune", type="docker_prune")
    _launch_free(job_id, docker_ops.system_prune(_make_updater(job_id)))
    return {"accepted": True, "job_id": job_id}


# ---------------------------------------------------------------------------
# Handlers: Backup
# ---------------------------------------------------------------------------

async def _handle_backup_list_jobs(_params: dict) -> dict:
    return {"jobs": backup_scheduler.list_jobs()}


async def _handle_backup_create_job(params: dict) -> dict:
    record = backup_scheduler.create_job(
        name=params["name"],
        source_vg=params["source_vg"],
        dest_protocol=params["dest_protocol"],
        dest_path=params["dest_path"],
        interval_hours=params["interval_hours"],
        dest_host=params.get("dest_host", ""),
        dest_cifs_user=params.get("dest_cifs_user", ""),
        dest_cifs_pass=params.get("dest_cifs_pass", ""),
    )
    return record


async def _handle_backup_delete_job(params: dict) -> dict:
    return backup_scheduler.delete_job(params["backup_id"])


async def _handle_backup_run_now(params: dict) -> dict:
    backup_id = params["backup_id"]
    job_id = _new_job(
        "backup_run", type="backup_run",
        vg_name=None, backup_id=backup_id,
    )
    _launch_free(job_id, backup_scheduler.run_backup(backup_id, _make_updater(job_id)))
    return {"accepted": True, "job_id": job_id}


async def _handle_backup_job_history(params: dict) -> dict:
    return {"history": backup_scheduler.get_history(params["backup_id"])}


# ---------------------------------------------------------------------------
# Handlers: File sharing
# ---------------------------------------------------------------------------

async def _handle_nfs_list_exports(_params: dict) -> dict:
    return {"exports": await sharing_ops.nfs_list_exports()}


async def _handle_nfs_add_export(params: dict) -> dict:
    return await sharing_ops.nfs_add_export(
        params["path"], params["clients"], params["options"]
    )


async def _handle_nfs_remove_export(params: dict) -> dict:
    return await sharing_ops.nfs_remove_export(params["path"])


async def _handle_samba_list_shares(_params: dict) -> dict:
    return {"shares": await sharing_ops.samba_list_shares()}


async def _handle_samba_add_share(params: dict) -> dict:
    return await sharing_ops.samba_add_share(
        params["name"], params["path"],
        comment=params.get("comment", ""),
        public=params.get("public", False),
        writable=params.get("writable", True),
    )


async def _handle_samba_remove_share(params: dict) -> dict:
    return await sharing_ops.samba_remove_share(params["name"])


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_HANDLERS = {
    "disk_scan":       _handle_disk_scan,
    "array_detail":    _handle_array_detail,
    "lvm_report":      _handle_lvm_report,
    "array_create":    _handle_array_create,
    "array_delete":    _handle_array_delete,
    "vg_rename":       _handle_vg_rename,
    "jobs_list":       _handle_jobs_list,
    "job_delete":      _handle_job_delete,
    # new
    "array_migrate":   _handle_array_migrate,
    "disk_replace":    _handle_disk_replace,
    "array_grow":      _handle_array_grow,
    "array_shrink":    _handle_array_shrink,
    "volume_clone":    _handle_volume_clone,
    "volume_backup":   _handle_volume_backup,
    "smart_test":      _handle_smart_test,
    "badblocks_test":  _handle_badblocks_test,
    "disk_erase":      _handle_disk_erase,
    # filesystem management
    "fs_format":              _handle_fs_format,
    "fs_mount":               _handle_fs_mount,
    "fs_unmount":             _handle_fs_unmount,
    "fs_info":                _handle_fs_info,
    "fs_set_compression":     _handle_fs_set_compression,
    # btrfs subvolumes / snapshots
    "btrfs_subvol_list":      _handle_btrfs_subvol_list,
    "btrfs_subvol_create":    _handle_btrfs_subvol_create,
    "btrfs_subvol_delete":    _handle_btrfs_subvol_delete,
    "btrfs_snapshot_create":  _handle_btrfs_snapshot_create,
    "btrfs_subvol_set_default": _handle_btrfs_subvol_set_default,
    # btrfs maintenance
    "btrfs_scrub_start":         _handle_btrfs_scrub_start,
    "btrfs_scrub_status":        _handle_btrfs_scrub_status,
    "btrfs_scrub_cancel":        _handle_btrfs_scrub_cancel,
    "btrfs_scrub_pause":         _handle_btrfs_scrub_pause,
    "btrfs_scrub_resume":        _handle_btrfs_scrub_resume,
    "btrfs_scrub_last_result":   _handle_btrfs_scrub_last_result,
    "btrfs_balance_start":    _handle_btrfs_balance_start,
    "btrfs_balance_status":   _handle_btrfs_balance_status,
    "btrfs_balance_cancel":   _handle_btrfs_balance_cancel,
    "btrfs_defrag":           _handle_btrfs_defrag,
    "btrfs_dedup":            _handle_btrfs_dedup,
    # btrfs quotas
    "btrfs_quota_enable":     _handle_btrfs_quota_enable,
    "btrfs_quota_list":       _handle_btrfs_quota_list,
    "btrfs_quota_set":        _handle_btrfs_quota_set,
    # btrfs usage / stats
    "btrfs_usage_detail":     _handle_btrfs_usage_detail,
    # btrfs send / receive
    "btrfs_send":             _handle_btrfs_send,
    "btrfs_receive":          _handle_btrfs_receive,
    # VM management
    "vm_install_iso":         _handle_vm_install_iso,
    "vm_list":                _handle_vm_list,
    "vm_info":                _handle_vm_info,
    "vm_action":              _handle_vm_action,
    "vm_list_isos":           _handle_vm_list_isos,
    "vm_create":              _handle_vm_create,
    "vm_delete":              _handle_vm_delete,
    # Docker
    "docker_list_containers": _handle_docker_list_containers,
    "docker_create_container":_handle_docker_create_container,
    "docker_container_action":_handle_docker_container_action,
    "docker_container_logs":  _handle_docker_container_logs,
    "docker_list_images":     _handle_docker_list_images,
    "docker_pull_image":      _handle_docker_pull_image,
    "docker_remove_image":    _handle_docker_remove_image,
    "docker_system_prune":    _handle_docker_system_prune,
    # Backup
    "backup_list_jobs":       _handle_backup_list_jobs,
    "backup_create_job":      _handle_backup_create_job,
    "backup_delete_job":      _handle_backup_delete_job,
    "backup_run_now":         _handle_backup_run_now,
    "backup_job_history":     _handle_backup_job_history,
    # File sharing
    "nfs_list_exports":       _handle_nfs_list_exports,
    "nfs_add_export":         _handle_nfs_add_export,
    "nfs_remove_export":      _handle_nfs_remove_export,
    "samba_list_shares":      _handle_samba_list_shares,
    "samba_add_share":        _handle_samba_add_share,
    "samba_remove_share":     _handle_samba_remove_share,
}


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    peer = writer.get_extra_info("peername", "unknown")
    try:
        payload = await asyncio.wait_for(recv_message(reader), timeout=10.0)
    except (ProtocolError, asyncio.TimeoutError) as exc:
        logger.warning("bad request from %s: %s", peer, exc)
        writer.close()
        return

    request_id = payload.get("id", str(uuid.uuid4()))

    try:
        action, validated_params = validate_request(payload)
    except ValidationError as exc:
        logger.warning("validation error from %s: %s", peer, exc)
        await send_message(writer, {
            "id": request_id, "status": "error", "data": None,
            "error": {"code": "VALIDATION_ERROR", "message": str(exc)},
        })
        writer.close()
        return

    if action == "mdstat_subscribe":
        await send_message(writer, {
            "id": request_id, "status": "ok",
            "data": {"subscribed": True}, "error": None,
        })
        await _handle_mdstat_subscribe(validated_params, reader, writer)
        writer.close()
        return

    handler = _HANDLERS[action]
    try:
        data = await handler(validated_params)
        # Handlers that are busy return {"accepted": False, "reason": "ARRAY_BUSY"}
        if isinstance(data, dict) and data.get("reason") == "ARRAY_BUSY":
            await send_message(writer, {
                "id": request_id, "status": "error", "data": None,
                "error": {"code": "ARRAY_BUSY", "message": data.get("message", "Array busy")},
            })
        else:
            await send_message(writer, {
                "id": request_id, "status": "ok", "data": data, "error": None,
            })
    except Exception as exc:
        logger.exception("handler %s error: %s", action, exc)
        await send_message(writer, {
            "id": request_id, "status": "error", "data": None,
            "error": {"code": "SUBPROCESS_FAILED", "message": str(exc)},
        })
    finally:
        writer.close()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    _setup_logging()
    logger.info("TRAID worker daemon starting")

    if os.geteuid() != 0:
        logger.error("worker daemon must run as root")
        sys.exit(1)

    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    await fs_ops.remount_all()

    # Wire backup scheduler into the job system
    backup_scheduler.init_scheduler(
        launch_free=_launch_free,
        active_jobs=_active_jobs,
        job_history=_job_history,
        new_job=_new_job,
        make_updater=_make_updater,
    )
    asyncio.create_task(backup_scheduler.tick_scheduler(), name="backup-scheduler")

    server = await asyncio.start_unix_server(_handle_client, path=str(SOCKET_PATH))

    try:
        import grp
        gid = grp.getgrnam(SOCKET_GROUP).gr_gid
        os.chown(str(SOCKET_PATH), 0, gid)
    except KeyError:
        logger.warning("group %r not found; socket accessible only by root", SOCKET_GROUP)

    os.chmod(str(SOCKET_PATH), 0o660)
    logger.info("listening on %s", SOCKET_PATH)

    await _mdstat_reader.start()

    loop = asyncio.get_running_loop()

    def _shutdown():
        logger.info("shutdown signal received")
        server.close()

    loop.add_signal_handler(signal.SIGTERM, _shutdown)
    loop.add_signal_handler(signal.SIGINT, _shutdown)

    async with server:
        await server.serve_forever()

    await _mdstat_reader.stop()
    logger.info("daemon stopped")


if __name__ == "__main__":
    asyncio.run(main())
