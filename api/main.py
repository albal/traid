"""
TRAID Web API — FastAPI application.

Runs as www-data (unprivileged). All privileged operations are proxied
through the Unix Domain Socket to the worker daemon.

capacity_preview (GET /api/preview) is the one exception: it calls
traid_algorithm.py directly since it is pure calculation with no I/O.
"""

import logging
import re
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Response, WebSocket, WebSocketDisconnect
from pythonjsonlogger import jsonlogger

from api import uds_client
from api.models import (
    ArrayCreationRequest,
    BackupRequest,
    BadblocksRequest,
    CapacityPreview,
    CloneRequest,
    CreateAccepted,
    DiskInfo,
    EraseRequest,
    GrowRequest,
    JobAccepted,
    MigrateRequest,
    RaidGroupPreview,
    ReplaceRequest,
    ShrinkRequest,
    SmartTestRequest,
    VolumeRenameRequest,
)
from api.websocket_manager import WebSocketManager
from worker.traid_algorithm import capacity_preview as _calc_preview

REPORT_DIR = Path("/var/lib/traid/reports")
_REPORT_FILENAME_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"_(smart_short|smart_long|badblocks|erase)\.txt$"
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(
    jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
)
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App + lifespan
# ---------------------------------------------------------------------------

ws_manager = WebSocketManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ws_manager.start_mdstat_stream()
    logger.info("TRAID API started")
    yield
    await ws_manager.shutdown()
    logger.info("TRAID API stopped")


app = FastAPI(title="TRAID Web API", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------


def _worker_error_to_http(exc: uds_client.WorkerError) -> HTTPException:
    if exc.code == "ARRAY_BUSY":
        return HTTPException(status_code=409, detail={"code": exc.code, "message": exc.message})
    status = 400 if exc.code == "VALIDATION_ERROR" else 500
    return HTTPException(status_code=status, detail={"code": exc.code, "message": exc.message})


def _job_response(data: dict) -> JobAccepted:
    """Convert worker job response; raise 409 if ARRAY_BUSY."""
    if not data.get("accepted"):
        raise HTTPException(
            status_code=409,
            detail={"code": "ARRAY_BUSY",
                    "message": "Another operation is in progress. Retry when the current job finishes."},
        )
    return JobAccepted(**data)


# ---------------------------------------------------------------------------
# Disk routes
# ---------------------------------------------------------------------------


@app.get("/api/disks", response_model=list[DiskInfo])
async def get_disks():
    try:
        data = await uds_client.send_request("disk_scan")
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    disks = data.get("disks", []) if isinstance(data, dict) else []
    return [DiskInfo(**d) for d in disks]


@app.post("/api/disks/smart", response_model=JobAccepted, status_code=202)
async def start_smart_test(request: SmartTestRequest):
    try:
        data = await uds_client.send_request(
            "smart_test", {"disk": request.disk, "test_type": request.test_type}
        )
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return JobAccepted(**data)


@app.post("/api/disks/badblocks", response_model=JobAccepted, status_code=202)
async def start_badblocks(request: BadblocksRequest):
    try:
        data = await uds_client.send_request("badblocks_test", {"disk": request.disk})
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return _job_response(data)


@app.post("/api/disks/erase", response_model=JobAccepted, status_code=202)
async def erase_disk(request: EraseRequest):
    try:
        data = await uds_client.send_request("disk_erase", {"disk": request.disk, "mode": request.mode})
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return _job_response(data)


# ---------------------------------------------------------------------------
# Volume / array routes
# ---------------------------------------------------------------------------


@app.get("/api/volumes")
async def get_volumes():
    try:
        return await uds_client.send_request("lvm_report")
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)


@app.get("/api/preview", response_model=CapacityPreview)
async def preview_capacity(
    disks: Annotated[list[str], Query()],
    type: Annotated[str, Query()] = "traid1",
):
    if type not in ("traid1", "traid2"):
        raise HTTPException(status_code=400, detail="type must be traid1 or traid2")
    if not disks:
        raise HTTPException(status_code=400, detail="disks must not be empty")

    try:
        scan_data = await uds_client.send_request("disk_scan")
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)

    disk_map = {
        d["name"]: d["size"]
        for d in (scan_data.get("disks", []) if isinstance(scan_data, dict) else [])
    }
    sizes: list[int] = []
    for disk in disks:
        if disk not in disk_map:
            raise HTTPException(status_code=404, detail=f"disk {disk!r} not found")
        sizes.append(disk_map[disk])

    redundancy = 1 if type == "traid1" else 2
    try:
        result = _calc_preview(sizes, redundancy=redundancy)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return CapacityPreview(
        usable_bytes=result["usable_bytes"],
        redundancy_disks=result["redundancy_disks"],
        raid_groups=[RaidGroupPreview(**g) for g in result["raid_groups"]],
    )


@app.post("/api/create", response_model=CreateAccepted, status_code=202)
async def create_array(request: ArrayCreationRequest):
    try:
        data = await uds_client.send_request(
            "array_create",
            {"disks": request.disks, "type": request.type, "vg_name": request.vg_name},
        )
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    if not data.get("accepted"):
        raise HTTPException(status_code=409, detail={"code": "ARRAY_BUSY",
                            "message": "Another operation is in progress"})
    return CreateAccepted(**data)


@app.post("/api/volumes/{vg_name}/migrate", response_model=JobAccepted, status_code=202)
async def migrate_volume(vg_name: str, request: MigrateRequest):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$", vg_name):
        raise HTTPException(status_code=400, detail="invalid volume group name")
    params = {"vg_name": vg_name, "direction": request.direction}
    if request.new_disk:
        params["new_disk"] = request.new_disk
    try:
        data = await uds_client.send_request("array_migrate", params)
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return _job_response(data)


@app.post("/api/volumes/{vg_name}/replace", response_model=JobAccepted, status_code=202)
async def replace_disk(vg_name: str, request: ReplaceRequest):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$", vg_name):
        raise HTTPException(status_code=400, detail="invalid volume group name")
    try:
        data = await uds_client.send_request(
            "disk_replace",
            {"vg_name": vg_name, "old_disk": request.old_disk, "new_disk": request.new_disk},
        )
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return _job_response(data)


@app.post("/api/volumes/{vg_name}/grow", response_model=JobAccepted, status_code=202)
async def grow_array(vg_name: str, request: GrowRequest):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$", vg_name):
        raise HTTPException(status_code=400, detail="invalid volume group name")
    try:
        data = await uds_client.send_request(
            "array_grow", {"vg_name": vg_name, "new_disk": request.new_disk}
        )
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return _job_response(data)


@app.post("/api/volumes/{vg_name}/shrink", response_model=JobAccepted, status_code=202)
async def shrink_array(vg_name: str, request: ShrinkRequest):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$", vg_name):
        raise HTTPException(status_code=400, detail="invalid volume group name")
    try:
        data = await uds_client.send_request(
            "array_shrink",
            {"vg_name": vg_name, "disk_to_remove": request.disk_to_remove},
        )
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return _job_response(data)


@app.post("/api/volumes/{vg_name}/clone", response_model=JobAccepted, status_code=202)
async def clone_volume(vg_name: str, request: CloneRequest):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$", vg_name):
        raise HTTPException(status_code=400, detail="invalid volume group name")
    try:
        data = await uds_client.send_request(
            "volume_clone", {"vg_name": vg_name, "target_disk": request.target_disk}
        )
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return _job_response(data)


@app.post("/api/volumes/{vg_name}/backup", response_model=JobAccepted, status_code=202)
async def backup_volume(vg_name: str, request: BackupRequest):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$", vg_name):
        raise HTTPException(status_code=400, detail="invalid volume group name")
    try:
        data = await uds_client.send_request("volume_backup", {
            "vg_name": vg_name,
            "protocol": request.protocol,
            "host": request.host,
            "remote_path": request.remote_path,
            "cifs_user": request.cifs_user,
            "cifs_pass": request.cifs_pass,
        })
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return _job_response(data)


@app.patch("/api/volumes/{vg_name}", status_code=200)
async def rename_volume(vg_name: str, request: VolumeRenameRequest):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$", vg_name):
        raise HTTPException(status_code=400, detail="invalid volume group name")
    try:
        return await uds_client.send_request(
            "vg_rename", {"vg_name": vg_name, "new_name": request.new_name}
        )
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)


@app.delete("/api/volumes/{vg_name}", status_code=202)
async def delete_volume(vg_name: str):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$", vg_name):
        raise HTTPException(status_code=400, detail="invalid volume group name")
    try:
        data = await uds_client.send_request("array_delete", {"vg_name": vg_name})
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)
    return data


# ---------------------------------------------------------------------------
# Job routes
# ---------------------------------------------------------------------------


@app.get("/api/jobs")
async def list_jobs():
    try:
        return await uds_client.send_request("jobs_list")
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    if not re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", job_id
    ):
        raise HTTPException(status_code=400, detail="invalid job id")
    try:
        return await uds_client.send_request("job_delete", {"job_id": job_id})
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


# ---------------------------------------------------------------------------
# Report serving
# ---------------------------------------------------------------------------


@app.get("/api/reports/{filename}")
async def get_report(filename: str, download: bool = False):
    if not _REPORT_FILENAME_RE.match(filename):
        raise HTTPException(status_code=404, detail="report not found")
    path = REPORT_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="report not found")
    content = path.read_text(errors="replace")
    disposition = f'attachment; filename="{filename}"' if download else "inline"
    return Response(
        content=content,
        media_type="text/plain",
        headers={"Content-Disposition": disposition},
    )


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------


@app.websocket("/ws/progress")
async def ws_progress(websocket: WebSocket):
    await websocket.accept()
    ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_manager.disconnect(websocket)
