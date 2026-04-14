"""
Scheduled backup jobs for TRAID volumes.

Jobs are persisted in /var/lib/traid/backup_jobs.json.
Supported destinations: rsync_local, nfs, cifs, btrfs_send.

The tick_scheduler() coroutine runs as a background asyncio task inside
the daemon and fires due jobs every 60 seconds.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)

BACKUP_JOBS_FILE = Path("/var/lib/traid/backup_jobs.json")
BTRFS_STREAM_DIR = Path("/var/lib/traid/btrfs-streams")
MAX_HISTORY = 20

# Will be set by daemon.py at startup so the scheduler can launch jobs
_launch_free_fn = None
_active_jobs_ref = None
_job_history_ref = None
_new_job_fn = None
_make_updater_fn = None


def init_scheduler(launch_free, active_jobs, job_history, new_job, make_updater):
    """Called once by daemon.py to wire up job-system references."""
    global _launch_free_fn, _active_jobs_ref, _job_history_ref, _new_job_fn, _make_updater_fn
    _launch_free_fn   = launch_free
    _active_jobs_ref  = active_jobs
    _job_history_ref  = job_history
    _new_job_fn       = new_job
    _make_updater_fn  = make_updater


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def _load_jobs() -> dict:
    if BACKUP_JOBS_FILE.exists():
        try:
            return json.loads(BACKUP_JOBS_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_jobs(jobs: dict) -> None:
    BACKUP_JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = BACKUP_JOBS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(jobs, indent=2))
    os.replace(tmp, BACKUP_JOBS_FILE)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def list_jobs() -> list:
    return list(_load_jobs().values())


def create_job(name: str, source_vg: str, dest_protocol: str, dest_path: str,
               interval_hours: int, dest_host: str = "",
               dest_cifs_user: str = "", dest_cifs_pass: str = "") -> dict:
    jobs = _load_jobs()
    backup_id = str(uuid.uuid4())
    record = {
        "backup_id":       backup_id,
        "name":            name,
        "source_vg":       source_vg,
        "dest_protocol":   dest_protocol,
        "dest_host":       dest_host,
        "dest_path":       dest_path,
        "dest_cifs_user":  dest_cifs_user,
        "dest_cifs_pass":  dest_cifs_pass,
        "interval_hours":  interval_hours,
        "last_run":        None,
        "last_status":     "never",
        "last_error":      None,
        "history":         [],
    }
    jobs[backup_id] = record
    _save_jobs(jobs)
    return record


def delete_job(backup_id: str) -> dict:
    jobs = _load_jobs()
    if backup_id not in jobs:
        return {"deleted": False, "reason": "not found"}
    del jobs[backup_id]
    _save_jobs(jobs)
    return {"deleted": True, "backup_id": backup_id}


def get_history(backup_id: str) -> list:
    jobs = _load_jobs()
    if backup_id not in jobs:
        raise RuntimeError(f"backup job {backup_id!r} not found")
    return jobs[backup_id].get("history", [])


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

async def _run(*cmd: str) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")


async def run_backup(backup_id: str, update_fn) -> None:
    jobs = _load_jobs()
    if backup_id not in jobs:
        raise RuntimeError(f"backup job {backup_id!r} not found")
    job = jobs[backup_id]

    source_vg    = job["source_vg"]
    protocol     = job["dest_protocol"]
    dest_path    = job["dest_path"]
    dest_host    = job.get("dest_host", "")
    cifs_user    = job.get("dest_cifs_user", "")
    cifs_pass    = job.get("dest_cifs_pass", "")
    mount_point  = f"/mnt/traid/{source_vg}"

    start_ts = time.time()
    try:
        update_fn(f"Starting backup of {source_vg}…", 5)

        if protocol == "rsync_local":
            rc, _, err = await _run(
                "rsync", "-aH", "--delete", f"{mount_point}/", dest_path
            )
            if rc != 0:
                raise RuntimeError(f"rsync failed: {err.strip()}")

        elif protocol in ("nfs", "cifs"):
            # Mount destination, rsync, unmount
            tmp_mount = f"/tmp/traid_backup_{backup_id[:8]}"
            Path(tmp_mount).mkdir(parents=True, exist_ok=True)
            try:
                if protocol == "nfs":
                    mount_cmd = ["mount", "-t", "nfs", f"{dest_host}:{dest_path}", tmp_mount]
                else:
                    opts = f"username={cifs_user},password={cifs_pass},vers=3.0"
                    mount_cmd = ["mount", "-t", "cifs",
                                 f"//{dest_host}/{dest_path}", tmp_mount,
                                 "-o", opts]
                rc, _, err = await _run(*mount_cmd)
                if rc != 0:
                    raise RuntimeError(f"mount failed: {err.strip()}")
                update_fn("Syncing files…", 30)
                rc, _, err = await _run(
                    "rsync", "-aH", "--delete", f"{mount_point}/", f"{tmp_mount}/"
                )
                if rc != 0:
                    raise RuntimeError(f"rsync failed: {err.strip()}")
            finally:
                await _run("umount", tmp_mount)

        elif protocol == "btrfs_send":
            # Stream btrfs snapshot to dest_path file
            BTRFS_STREAM_DIR.mkdir(parents=True, exist_ok=True)
            out_file = dest_path
            proc = await asyncio.create_subprocess_exec(
                "btrfs", "send", mount_point,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            data, err_b = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"btrfs send failed: {err_b.decode(errors='replace').strip()}")
            Path(out_file).write_bytes(data)

        else:
            raise RuntimeError(f"Unknown backup protocol: {protocol!r}")

        update_fn("Backup complete", 100)
        duration = time.time() - start_ts
        _update_job_record(backup_id, status="ok", error=None, duration_s=duration)

    except Exception as exc:
        duration = time.time() - start_ts
        _update_job_record(backup_id, status="failed", error=str(exc), duration_s=duration)
        raise


def _update_job_record(backup_id: str, status: str, error, duration_s: float) -> None:
    jobs = _load_jobs()
    if backup_id not in jobs:
        return
    job = jobs[backup_id]
    now = time.time()
    job["last_run"]    = now
    job["last_status"] = status
    job["last_error"]  = error
    history_entry = {
        "run_at":     now,
        "status":     status,
        "error":      error,
        "duration_s": round(duration_s, 1),
    }
    job.setdefault("history", []).insert(0, history_entry)
    job["history"] = job["history"][:MAX_HISTORY]
    _save_jobs(jobs)


# ---------------------------------------------------------------------------
# Scheduler tick (runs as daemon background task)
# ---------------------------------------------------------------------------

async def tick_scheduler() -> None:
    """Background loop: fires backup jobs that are due."""
    while True:
        try:
            await asyncio.sleep(60)
            jobs = _load_jobs()
            now = time.time()
            for backup_id, job in jobs.items():
                interval_s = job.get("interval_hours", 24) * 3600
                last_run   = job.get("last_run") or 0
                if now - last_run >= interval_s:
                    # Don't double-launch if already running
                    if any(
                        (h.get("backup_id") == backup_id)
                        for h in (_job_history_ref or {}).values()
                        if h.get("status") in ("running", "accepted")
                    ):
                        continue
                    logger.info("scheduler: firing backup job %s (%s)", backup_id, job["name"])
                    job_id = _new_job_fn(
                        "backup_run", vg_name=job["source_vg"],
                        type="backup_run", backup_id=backup_id,
                    )
                    _launch_free_fn(
                        job_id,
                        run_backup(backup_id, _make_updater_fn(job_id)),
                    )
        except Exception as exc:
            logger.warning("tick_scheduler error: %s", exc)
