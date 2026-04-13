"""
TRAID Web API — FastAPI application.

Runs as www-data (unprivileged). All privileged operations are proxied
through the Unix Domain Socket to the worker daemon.

capacity_preview (GET /api/preview) is the one exception: it calls
traid_algorithm.py directly since it is pure calculation with no I/O.
"""

import logging
import sys
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pythonjsonlogger import jsonlogger

from api import uds_client
from api.models import (
    ArrayCreationRequest,
    CapacityPreview,
    CreateAccepted,
    DiskInfo,
    RaidGroupPreview,
)
from api.websocket_manager import WebSocketManager
from worker.traid_algorithm import capacity_preview as _calc_preview

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
# Error handling helpers
# ---------------------------------------------------------------------------

def _worker_error_to_http(exc: uds_client.WorkerError) -> HTTPException:
    status = 400 if exc.code == "VALIDATION_ERROR" else 500
    return HTTPException(status_code=status, detail={"code": exc.code, "message": exc.message})


# ---------------------------------------------------------------------------
# Routes
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
    """
    Pure-calculation endpoint: returns usable capacity estimate without
    touching the worker daemon. Sizes are fetched from disk_scan first.
    """
    if type not in ("traid1", "traid2"):
        raise HTTPException(status_code=400, detail="type must be traid1 or traid2")
    if not disks:
        raise HTTPException(status_code=400, detail="disks must not be empty")

    # Fetch current disk sizes from the worker
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
            {"disks": request.disks, "type": request.type},
        )
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        raise _worker_error_to_http(exc)

    return CreateAccepted(**data)


@app.websocket("/ws/progress")
async def ws_progress(websocket: WebSocket):
    await websocket.accept()
    ws_manager.connect(websocket)
    try:
        while True:
            # Keep connection open; all data is pushed via ws_manager.broadcast
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_manager.disconnect(websocket)


