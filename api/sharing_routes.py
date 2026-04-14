"""
File sharing routes — /api/sharing/nfs and /api/sharing/smb
"""

import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from api import uds_client

router = APIRouter(prefix="/api/sharing", tags=["sharing"])

_SHARE_PATH_RE  = re.compile(
    r"^/(?:srv/traid|mnt/traid|var/lib/traid)(?!.*\.\.)[/a-zA-Z0-9_.@-]{0,255}$"
)
_NFS_CLIENTS_RE = re.compile(r"^[a-zA-Z0-9.*,/_()\-]{1,200}$")
_NFS_OPTIONS_RE = re.compile(r"^[a-zA-Z0-9_,=]{0,200}$")
_SMB_NAME_RE    = re.compile(r"^[a-zA-Z0-9_-]{1,50}$")


class NfsExportRequest(BaseModel):
    path:    str
    clients: str
    options: str = "rw,sync,no_subtree_check"

    @field_validator("path")
    @classmethod
    def _path(cls, v):
        if not _SHARE_PATH_RE.match(v):
            raise ValueError("path must be under /srv/traid, /mnt/traid, or /var/lib/traid")
        return v

    @field_validator("clients")
    @classmethod
    def _clients(cls, v):
        if not _NFS_CLIENTS_RE.match(v):
            raise ValueError("invalid NFS clients specification")
        return v

    @field_validator("options")
    @classmethod
    def _options(cls, v):
        if not _NFS_OPTIONS_RE.match(v):
            raise ValueError("invalid NFS options")
        return v


class NfsRemoveRequest(BaseModel):
    path: str

    @field_validator("path")
    @classmethod
    def _path(cls, v):
        if not _SHARE_PATH_RE.match(v):
            raise ValueError("invalid export path")
        return v


class SambaShareRequest(BaseModel):
    name:     str
    path:     str
    comment:  str = ""
    public:   bool = False
    writable: bool = True

    @field_validator("name")
    @classmethod
    def _name(cls, v):
        if not _SMB_NAME_RE.match(v):
            raise ValueError("invalid Samba share name")
        return v

    @field_validator("path")
    @classmethod
    def _path(cls, v):
        if not _SHARE_PATH_RE.match(v):
            raise ValueError("path must be under /srv/traid, /mnt/traid, or /var/lib/traid")
        return v

    @field_validator("comment")
    @classmethod
    def _comment(cls, v):
        if len(v) > 200:
            raise ValueError("comment too long")
        return v


async def _send(action: str, params: dict = {}) -> dict:
    try:
        return await uds_client.send_request(action, params)
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        status = 400 if exc.code == "VALIDATION_ERROR" else 500
        raise HTTPException(status_code=status, detail={"code": exc.code, "message": exc.message})


# ---------------------------------------------------------------------------
# NFS
# ---------------------------------------------------------------------------

@router.get("/nfs")
async def list_nfs_exports():
    data = await _send("nfs_list_exports")
    return data.get("exports", []) if isinstance(data, dict) else data


@router.post("/nfs", status_code=201)
async def add_nfs_export(request: NfsExportRequest):
    return await _send("nfs_add_export", {
        "path": request.path, "clients": request.clients, "options": request.options,
    })


@router.delete("/nfs")
async def remove_nfs_export(request: NfsRemoveRequest):
    return await _send("nfs_remove_export", {"path": request.path})


# ---------------------------------------------------------------------------
# Samba
# ---------------------------------------------------------------------------

@router.get("/smb")
async def list_smb_shares():
    data = await _send("samba_list_shares")
    return data.get("shares", []) if isinstance(data, dict) else data


@router.post("/smb", status_code=201)
async def add_smb_share(request: SambaShareRequest):
    return await _send("samba_add_share", {
        "name": request.name, "path": request.path,
        "comment": request.comment, "public": request.public, "writable": request.writable,
    })


@router.delete("/smb/{name}")
async def remove_smb_share(name: str):
    if not _SMB_NAME_RE.match(name):
        raise HTTPException(status_code=400, detail="invalid share name")
    return await _send("samba_remove_share", {"name": name})
