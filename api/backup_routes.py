"""
Scheduled backup routes — /api/backup
"""

import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import Literal, Optional

from api import uds_client
from api.models import JobAccepted

router = APIRouter(prefix="/api/backup", tags=["backup"])

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_VG_RE   = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$")
_LABEL_RE = re.compile(r"^[a-zA-Z0-9_.+-]{1,127}$")


class BackupJobCreateRequest(BaseModel):
    name:           str
    source_vg:      str
    dest_protocol:  Literal["rsync_local", "nfs", "cifs", "btrfs_send"]
    dest_path:      str
    interval_hours: int = 24
    dest_host:      str = ""
    dest_cifs_user: str = ""
    dest_cifs_pass: str = ""

    @field_validator("name")
    @classmethod
    def _name(cls, v):
        if not _LABEL_RE.match(v):
            raise ValueError("invalid job name")
        return v

    @field_validator("source_vg")
    @classmethod
    def _vg(cls, v):
        if not _VG_RE.match(v):
            raise ValueError("invalid volume group name")
        return v

    @field_validator("interval_hours")
    @classmethod
    def _interval(cls, v):
        if not (1 <= v <= 8760):
            raise ValueError("interval_hours must be 1–8760")
        return v


async def _send(action: str, params: dict = {}) -> dict:
    try:
        return await uds_client.send_request(action, params)
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        status = 400 if exc.code == "VALIDATION_ERROR" else 500
        raise HTTPException(status_code=status, detail={"code": exc.code, "message": exc.message})


def _check_backup_id(backup_id: str) -> None:
    if not _UUID_RE.match(backup_id):
        raise HTTPException(status_code=400, detail="invalid backup job ID")


@router.get("/jobs")
async def list_backup_jobs():
    data = await _send("backup_list_jobs")
    return data.get("jobs", []) if isinstance(data, dict) else data


@router.post("/jobs", status_code=201)
async def create_backup_job(request: BackupJobCreateRequest):
    return await _send("backup_create_job", {
        "name":           request.name,
        "source_vg":      request.source_vg,
        "dest_protocol":  request.dest_protocol,
        "dest_path":      request.dest_path,
        "interval_hours": request.interval_hours,
        "dest_host":      request.dest_host,
        "dest_cifs_user": request.dest_cifs_user,
        "dest_cifs_pass": request.dest_cifs_pass,
    })


@router.delete("/jobs/{backup_id}")
async def delete_backup_job(backup_id: str):
    _check_backup_id(backup_id)
    return await _send("backup_delete_job", {"backup_id": backup_id})


@router.post("/jobs/{backup_id}/run", status_code=202, response_model=JobAccepted)
async def run_backup_now(backup_id: str):
    _check_backup_id(backup_id)
    data = await _send("backup_run_now", {"backup_id": backup_id})
    return JobAccepted(**data)


@router.get("/jobs/{backup_id}/history")
async def backup_job_history(backup_id: str):
    _check_backup_id(backup_id)
    data = await _send("backup_job_history", {"backup_id": backup_id})
    return data.get("history", []) if isinstance(data, dict) else data
