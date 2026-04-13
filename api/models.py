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


class ArrayCreationRequest(BaseModel):
    disks: list[str]
    type: Literal["traid1", "traid2"]

    @field_validator("disks")
    @classmethod
    def disks_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("disks must not be empty")
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
