"""Pydantic v2 request/response schemas for the TRAID API."""

import re
from typing import Literal
from pydantic import BaseModel, field_validator

_VG_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.+-]{0,126}$")
_DEV_RE = re.compile(r"^/dev/[a-z]{2,8}[0-9]{0,3}(p[0-9]{1,3})?$")
_CRED_RE = re.compile(r"^[^\n\r;&|`$<>]{0,256}$")
_RPATH_RE = re.compile(r"^[a-zA-Z0-9._-]{1,253}[:/].{1,255}$")


class DiskInfo(BaseModel):
    name: str
    size: int
    type: str
    mountpoint: str | None
    fstype: str | None
    available: bool
    used_by: str | None = None


class ArrayCreationRequest(BaseModel):
    disks: list[str]
    type: Literal["traid1", "traid2"]
    vg_name: str = "traid_vg"

    @field_validator("disks")
    @classmethod
    def disks_not_empty(cls, v):
        if not v:
            raise ValueError("disks must not be empty")
        return v

    @field_validator("vg_name")
    @classmethod
    def vg_name_valid(cls, v):
        if not _VG_RE.match(v):
            raise ValueError("invalid volume group name")
        return v


class VolumeRenameRequest(BaseModel):
    new_name: str

    @field_validator("new_name")
    @classmethod
    def new_name_valid(cls, v):
        if not _VG_RE.match(v):
            raise ValueError("invalid volume group name")
        return v


class MigrateRequest(BaseModel):
    direction: Literal["traid1_to_traid2", "traid2_to_traid1"]
    new_disk: str | None = None

    @field_validator("new_disk")
    @classmethod
    def new_disk_valid(cls, v):
        if v is not None and not _DEV_RE.match(v):
            raise ValueError("invalid device path")
        return v


class ReplaceRequest(BaseModel):
    old_disk: str
    new_disk: str

    @field_validator("old_disk", "new_disk")
    @classmethod
    def disk_valid(cls, v):
        if not _DEV_RE.match(v):
            raise ValueError("invalid device path")
        return v


class GrowRequest(BaseModel):
    new_disk: str

    @field_validator("new_disk")
    @classmethod
    def disk_valid(cls, v):
        if not _DEV_RE.match(v):
            raise ValueError("invalid device path")
        return v


class ShrinkRequest(BaseModel):
    disk_to_remove: str

    @field_validator("disk_to_remove")
    @classmethod
    def disk_valid(cls, v):
        if not _DEV_RE.match(v):
            raise ValueError("invalid device path")
        return v


class CloneRequest(BaseModel):
    target_disk: str

    @field_validator("target_disk")
    @classmethod
    def disk_valid(cls, v):
        if not _DEV_RE.match(v):
            raise ValueError("invalid device path")
        return v


class BackupRequest(BaseModel):
    protocol: Literal["cifs", "nfs"]
    host: str
    remote_path: str
    cifs_user: str = ""
    cifs_pass: str = ""

    @field_validator("host", "cifs_user", "cifs_pass")
    @classmethod
    def cred_valid(cls, v):
        if not _CRED_RE.match(v):
            raise ValueError("invalid value")
        return v

    @field_validator("remote_path")
    @classmethod
    def path_valid(cls, v):
        if not _RPATH_RE.match(v):
            raise ValueError("invalid remote path")
        return v


class SmartTestRequest(BaseModel):
    disk: str
    test_type: Literal["short", "long"]

    @field_validator("disk")
    @classmethod
    def disk_valid(cls, v):
        if not _DEV_RE.match(v):
            raise ValueError("invalid device path")
        return v


class BadblocksRequest(BaseModel):
    disk: str

    @field_validator("disk")
    @classmethod
    def disk_valid(cls, v):
        if not _DEV_RE.match(v):
            raise ValueError("invalid device path")
        return v


class EraseRequest(BaseModel):
    disk: str
    mode: Literal["quick", "dod_short", "dod_7"] = "dod_short"

    @field_validator("disk")
    @classmethod
    def disk_valid(cls, v):
        if not _DEV_RE.match(v):
            raise ValueError("invalid device path")
        return v


class RaidGroupPreview(BaseModel):
    tier: int
    level: str
    members: int
    stripe_size_bytes: int
    usable_bytes: int


class CapacityPreview(BaseModel):
    usable_bytes: int
    redundancy_disks: int
    raid_groups: list[RaidGroupPreview]


class ProgressEvent(BaseModel):
    event: str
    arrays: list[dict] = []


class JobAccepted(BaseModel):
    accepted: bool
    job_id: str | None = None
    reason: str | None = None
    message: str | None = None


class CreateAccepted(BaseModel):
    accepted: bool
    job_id: str


class ErrorDetail(BaseModel):
    code: str
    message: str


class WorkerResponse(BaseModel):
    id: str
    status: Literal["ok", "error"]
    data: dict | list | None
    error: ErrorDetail | None
