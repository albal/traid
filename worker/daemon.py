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
import logging.config
import os
import shutil
import signal
import sys
import uuid
from pathlib import Path

from pythonjsonlogger import jsonlogger

from shared.protocol import send_message, recv_message, ProtocolError
from worker.command_validator import validate_request, ValidationError
from worker import disk_ops
from worker.mdstat_reader import MdstatReader
from worker.traid_algorithm import capacity_preview, calculate_traid
from worker.traid_algorithm import generate_parted_commands, generate_mdadm_commands, generate_lvm_commands

SOCKET_PATH = Path("/run/traid.sock")
SOCKET_GROUP = "www-data"


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_mdstat_reader: MdstatReader = MdstatReader()

# Active creation tasks: job_id -> asyncio.Task
_active_jobs: dict[str, asyncio.Task] = {}


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
    detail = await disk_ops.get_array_detail(params["device"])
    return detail


# ---------------------------------------------------------------------------
# Handler: lvm_report
# ---------------------------------------------------------------------------

async def _handle_lvm_report(_params: dict) -> dict:
    return await disk_ops.get_lvm_report()


# ---------------------------------------------------------------------------
# Handler: array_create (async job)
# ---------------------------------------------------------------------------

async def _execute_creation(job_id: str, disks: list[str], redundancy: int) -> None:
    logger.info("job %s: starting array creation for %s", job_id, disks)
    try:
        sizes = []
        for disk in disks:
            rc, out, _ = await disk_ops.run_privileged(
                "lsblk", ["--bytes", "--nodeps", "--output", "SIZE", "--noheadings", disk]
            )
            if rc != 0:
                logger.error("job %s: lsblk failed for %s", job_id, disk)
                return
            sizes.append(int(out.strip()))

        plan = calculate_traid(sizes, redundancy=redundancy)

        # Step 1: Partition each disk
        for i, disk in enumerate(disks):
            cmds = generate_parted_commands(disk, i, plan)
            for cmd in cmds:
                rc, _, err = await disk_ops.run_privileged(cmd[0], cmd[1:])
                if rc != 0:
                    logger.error("job %s: parted failed: %s", job_id, err)
                    return

        # Allow kernel to re-read partition tables
        await asyncio.sleep(1)
        for disk in disks:
            await disk_ops.run_privileged("partprobe", [disk])
        await asyncio.sleep(2)

        # Step 2: Create RAID arrays
        mdadm_cmds = generate_mdadm_commands(plan, disks)
        for cmd in mdadm_cmds:
            rc, _, err = await disk_ops.run_privileged(cmd[0], cmd[1:])
            if rc != 0:
                logger.error("job %s: mdadm failed: %s", job_id, err)
                return

        # Wait briefly for md devices to settle
        await asyncio.sleep(2)

        # Step 3: LVM
        lvm_cmds = generate_lvm_commands(plan)
        for cmd in lvm_cmds:
            rc, _, err = await disk_ops.run_privileged(cmd[0], cmd[1:])
            if rc != 0:
                logger.error("job %s: lvm command failed: %s", job_id, err)
                return

        logger.info("job %s: array creation complete", job_id)

    except asyncio.CancelledError:
        logger.warning("job %s: cancelled", job_id)
        raise
    except Exception:
        logger.exception("job %s: unexpected error", job_id)
    finally:
        _active_jobs.pop(job_id, None)


async def _handle_array_create(params: dict) -> dict:
    disks: list[str] = params["disks"]
    raid_type: str = params["type"]
    redundancy = 1 if raid_type == "traid1" else 2

    job_id = str(uuid.uuid4())
    task = asyncio.create_task(
        _execute_creation(job_id, disks, redundancy),
        name=f"create-{job_id[:8]}",
    )
    _active_jobs[job_id] = task
    logger.info("job %s: accepted, disks=%s type=%s", job_id, disks, raid_type)
    return {"accepted": True, "job_id": job_id}


# ---------------------------------------------------------------------------
# Handler: mdstat_subscribe (long-lived streaming connection)
# ---------------------------------------------------------------------------

async def _handle_mdstat_subscribe(
    _params: dict,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """
    Send an initial ok, then stream mdstat events until the client disconnects.
    This handler is special: it keeps the connection open.
    """
    q = _mdstat_reader.subscribe()
    try:
        while True:
            try:
                event = await asyncio.wait_for(q.get(), timeout=5.0)
            except asyncio.TimeoutError:
                # Send a keepalive ping so we detect dead connections
                event = {"event": "keepalive"}

            try:
                await send_message(writer, event)
            except (ConnectionResetError, BrokenPipeError, OSError):
                break

            # Check if client closed its side
            if writer.is_closing():
                break
    finally:
        _mdstat_reader.unsubscribe(q)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_HANDLERS = {
    "disk_scan": _handle_disk_scan,
    "array_detail": _handle_array_detail,
    "lvm_report": _handle_lvm_report,
    "array_create": _handle_array_create,
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
            "id": request_id,
            "status": "error",
            "data": None,
            "error": {"code": "VALIDATION_ERROR", "message": str(exc)},
        })
        writer.close()
        return

    # Special case: mdstat_subscribe needs the stream objects
    if action == "mdstat_subscribe":
        await send_message(writer, {
            "id": request_id,
            "status": "ok",
            "data": {"subscribed": True},
            "error": None,
        })
        await _handle_mdstat_subscribe(validated_params, reader, writer)
        writer.close()
        return

    handler = _HANDLERS[action]
    try:
        data = await handler(validated_params)
        await send_message(writer, {
            "id": request_id,
            "status": "ok",
            "data": data,
            "error": None,
        })
    except Exception as exc:
        logger.exception("handler %s error: %s", action, exc)
        await send_message(writer, {
            "id": request_id,
            "status": "error",
            "data": None,
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

    # Remove stale socket
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()

    server = await asyncio.start_unix_server(_handle_client, path=str(SOCKET_PATH))

    # Set socket ownership and permissions
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

    def _shutdown() -> None:
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
