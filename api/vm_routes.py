"""
VM management routes — /api/vms
"""

import os
import re
import tempfile
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel, field_validator

from api import uds_client
from api.models import JobAccepted

router = APIRouter(prefix="/api/vms", tags=["vms"])

_VM_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.+-]{0,63}$")
_ISO_RE     = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.@-]{0,200}\.iso$")


class VmActionRequest(BaseModel):
    action: Literal["start", "shutdown", "destroy", "suspend", "resume"]


class VmCreateRequest(BaseModel):
    name:    str
    iso:     str
    ram_mb:  int = 2048
    vcpus:   int = 2
    disk_gb: int = 20

    @field_validator("name")
    @classmethod
    def _name(cls, v):
        if not _VM_NAME_RE.match(v):
            raise ValueError("invalid VM name")
        return v

    @field_validator("iso")
    @classmethod
    def _iso(cls, v):
        if not _ISO_RE.match(v):
            raise ValueError("invalid ISO filename")
        return v

    @field_validator("ram_mb")
    @classmethod
    def _ram(cls, v):
        if not (64 <= v <= 65536):
            raise ValueError("ram_mb must be 64–65536")
        return v

    @field_validator("vcpus")
    @classmethod
    def _vcpus(cls, v):
        if not (1 <= v <= 64):
            raise ValueError("vcpus must be 1–64")
        return v

    @field_validator("disk_gb")
    @classmethod
    def _disk(cls, v):
        if not (1 <= v <= 32768):
            raise ValueError("disk_gb must be 1–32768")
        return v


async def _send(action: str, params: dict = {}) -> dict:
    try:
        return await uds_client.send_request(action, params)
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        status = 400 if exc.code == "VALIDATION_ERROR" else 500
        raise HTTPException(status_code=status, detail={"code": exc.code, "message": exc.message})


@router.get("")
async def list_vms():
    data = await _send("vm_list")
    return data.get("vms", []) if isinstance(data, dict) else data


@router.get("/isos")
async def list_isos():
    data = await _send("vm_list_isos")
    return data.get("isos", []) if isinstance(data, dict) else data


@router.post("/isos/upload", status_code=201)
async def upload_iso(file: UploadFile = File(...)):
    filename = Path(file.filename or "").name
    if not _ISO_RE.match(filename):
        raise HTTPException(status_code=400,
                            detail="Filename must match [a-zA-Z0-9][a-zA-Z0-9_.@-]{0,200}.iso")
    # Write to a temp file — www-data has write access to /tmp
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".iso", dir="/tmp")
    try:
        with os.fdopen(tmp_fd, "wb") as f:
            while True:
                chunk = await file.read(65536)
                if not chunk:
                    break
                f.write(chunk)
        # Ask the worker (root) to move it into the ISO directory
        return await _send("vm_install_iso", {"src_path": tmp_path, "filename": filename})
    except Exception:
        # Clean up temp file on error
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


@router.get("/{name}")
async def get_vm(name: str):
    if not _VM_NAME_RE.match(name):
        raise HTTPException(status_code=400, detail="invalid VM name")
    return await _send("vm_info", {"name": name})


@router.post("", status_code=202, response_model=JobAccepted)
async def create_vm(request: VmCreateRequest):
    data = await _send("vm_create", {
        "name": request.name, "iso": request.iso,
        "ram_mb": request.ram_mb, "vcpus": request.vcpus,
        "disk_gb": request.disk_gb,
    })
    return JobAccepted(**data)


@router.post("/{name}/action")
async def vm_action(name: str, request: VmActionRequest):
    if not _VM_NAME_RE.match(name):
        raise HTTPException(status_code=400, detail="invalid VM name")
    return await _send("vm_action", {"name": name, "action": request.action})


@router.delete("/{name}")
async def delete_vm(name: str, keep_storage: bool = False):
    if not _VM_NAME_RE.match(name):
        raise HTTPException(status_code=400, detail="invalid VM name")
    return await _send("vm_delete", {"name": name, "keep_storage": keep_storage})
