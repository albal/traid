"""Pydantic v2 request/response schemas for the TRAID API."""

from typing import Literal
from pydantic import BaseModel, field_validator


class DiskInfo(BaseModel):
    name: str
    size: int           # bytes
    type: str
    mountpoint: str | None
    fstype: str | None
    available: bool
    used_by: str | None = None   # VG name if disk is part of a TRAID volume


class ArrayCreationRequest(BaseModel):
    disks: list[str]
    type: Literal["traid1", "traid2"]
    vg_name: str = "traid_vg"

    @field_validator("disks")
    @classmethod
    def disks_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("disks must not be empty")
        return v

    @field_validator("vg_name")
    @classmethod
    def vg_name_valid(cls, v: str) -> str:
        import re
        if not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9_.+-]{0,126}$", v):
            raise ValueError("invalid volume group name")
        return v


class VolumeRenameRequest(BaseModel):
    new_name: str

    @field_validator("new_name")
    @classmethod
    def new_name_valid(cls, v: str) -> str:
        import re
        if not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9_.+-]{0,126}$", v):
            raise ValueError("invalid volume group name")
        return v


class RaidGroupPreview(BaseModel):
    tier: int
    level: str          # e.g. "raid5"
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
