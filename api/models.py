"""Pydantic v2 request/response schemas for the TRAID API."""

import re
from typing import Literal
from pydantic import BaseModel, field_validator

# VG names must start with a letter or underscore
_VG_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$")

# Block device paths: traditional (sda/vda/xvda) and NVMe (nvme0n1/nvme0n1p1)
_DEV_RE = re.compile(
    r"^/dev/(?:"
    r"[a-z]{2,8}[0-9]{0,3}(?:p[0-9]{1,3})?"   # sda, vda, xvda, sda1, etc.
    r"|nvme[0-9]{1,4}n[0-9]{1,4}(?:p[0-9]{1,3})?"  # nvme0n1, nvme0n1p1
    r"|mmcblk[0-9]{1,4}(?:p[0-9]{1,3})?"           # mmcblk0p1
    r")$"
)
# Dangerous pseudo-devices that must never be erased/scanned
_BLOCKED_DEVS = frozenset({
    "/dev/null", "/dev/zero", "/dev/full", "/dev/random", "/dev/urandom",
})
# MD virtual devices (rejected wherever only physical disks are expected)
_MD_DEV_RE = re.compile(r"^/dev/md[0-9]+$")

_CRED_RE = re.compile(r"^[^\n\r;&|`$<>]{0,256}$")
# NFS: host:/path  or  CIFS: //host/share
_RPATH_RE = re.compile(r"^(?://|[a-zA-Z0-9._-]{1,253}[:/]).{1,255}$")


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


def _check_dev_path(v: str) -> str:
    """Shared device path validator: must match _DEV_RE and not be a blocked pseudo-device."""
    if not _DEV_RE.match(v):
        raise ValueError("invalid device path")
    if v in _BLOCKED_DEVS:
        raise ValueError("device not allowed")
    return v


class MigrateRequest(BaseModel):
    direction: Literal["traid1_to_traid2", "traid2_to_traid1"]
    new_disk: str | None = None

    @field_validator("new_disk")
    @classmethod
    def new_disk_valid(cls, v):
        if v is not None:
            _check_dev_path(v)
        return v


class ReplaceRequest(BaseModel):
    old_disk: str
    new_disk: str

    @field_validator("old_disk", "new_disk")
    @classmethod
    def disk_valid(cls, v):
        return _check_dev_path(v)


class GrowRequest(BaseModel):
    new_disk: str

    @field_validator("new_disk")
    @classmethod
    def disk_valid(cls, v):
        return _check_dev_path(v)


class ShrinkRequest(BaseModel):
    disk_to_remove: str

    @field_validator("disk_to_remove")
    @classmethod
    def disk_valid(cls, v):
        _check_dev_path(v)
        if _MD_DEV_RE.match(v):
            raise ValueError("md virtual devices cannot be removed; specify a physical disk")
        return v


class CloneRequest(BaseModel):
    target_disk: str

    @field_validator("target_disk")
    @classmethod
    def disk_valid(cls, v):
        return _check_dev_path(v)


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
        return _check_dev_path(v)


class BadblocksRequest(BaseModel):
    disk: str

    @field_validator("disk")
    @classmethod
    def disk_valid(cls, v):
        return _check_dev_path(v)


class EraseRequest(BaseModel):
    disk: str
    mode: Literal["quick", "dod_short", "dod_7"] = "dod_short"

    @field_validator("disk")
    @classmethod
    def disk_valid(cls, v):
        return _check_dev_path(v)


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


_COMPRESS_RE = re.compile(r"^(zstd|lzo|zlib|none|)$")
_SUBVOL_PATH_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_./@-]{0,254}$")
_STREAM_FILE_RE = re.compile(r"^[a-zA-Z0-9_.@-]{1,200}\.btrfs$")
_QGROUP_RE = re.compile(r"^\d+/\d+$")


class FormatRequest(BaseModel):
    fstype: Literal["ext4", "btrfs"]
    label: str = ""
    compression: str = ""   # btrfs only: zstd | lzo | zlib | none

    @field_validator("compression")
    @classmethod
    def compression_valid(cls, v):
        if not _COMPRESS_RE.match(v):
            raise ValueError("compression must be zstd, lzo, zlib, none, or ''")
        return v


class BtrfsSubvolCreateRequest(BaseModel):
    name: str

    @field_validator("name")
    @classmethod
    def name_valid(cls, v):
        if not _SUBVOL_PATH_RE.match(v) or ".." in v.split("/"):
            raise ValueError("invalid subvolume name")
        return v


class BtrfsSubvolDeleteRequest(BaseModel):
    path: str
    recursive: bool = False

    @field_validator("path")
    @classmethod
    def path_valid(cls, v):
        if not _SUBVOL_PATH_RE.match(v) or ".." in v.split("/"):
            raise ValueError("invalid path")
        return v


class BtrfsSnapshotRequest(BaseModel):
    source_path: str
    dest_path: str
    readonly: bool = False

    @field_validator("source_path", "dest_path")
    @classmethod
    def path_valid(cls, v):
        if not _SUBVOL_PATH_RE.match(v) or ".." in v.split("/"):
            raise ValueError("invalid path")
        return v


class BtrfsBalanceRequest(BaseModel):
    usage_filter: int | None = None     # % 0-100: only balance chunks < N% full
    metadata_usage: int | None = None


class BtrfsDefragRequest(BaseModel):
    path: str = ""
    recursive: bool = True
    compression: str = ""

    @field_validator("path")
    @classmethod
    def path_valid(cls, v):
        if v and (not _SUBVOL_PATH_RE.match(v) or ".." in v.split("/")):
            raise ValueError("invalid path")
        return v

    @field_validator("compression")
    @classmethod
    def compression_valid(cls, v):
        if not _COMPRESS_RE.match(v):
            raise ValueError("invalid compression")
        return v


class BtrfsQuotaSetRequest(BaseModel):
    qgroup: str
    limit_bytes: int

    @field_validator("qgroup")
    @classmethod
    def qgroup_valid(cls, v):
        if not _QGROUP_RE.match(v):
            raise ValueError("qgroup must match N/N format")
        return v

    @field_validator("limit_bytes")
    @classmethod
    def limit_valid(cls, v):
        if v < 0:
            raise ValueError("limit_bytes must be non-negative")
        return v


class BtrfsSendRequest(BaseModel):
    snapshot_path: str
    dest_file: str
    parent_path: str | None = None

    @field_validator("snapshot_path")
    @classmethod
    def snap_valid(cls, v):
        if not _SUBVOL_PATH_RE.match(v) or ".." in v.split("/"):
            raise ValueError("invalid snapshot_path")
        return v

    @field_validator("dest_file")
    @classmethod
    def file_valid(cls, v):
        if not _STREAM_FILE_RE.match(v):
            raise ValueError("dest_file must be a .btrfs filename")
        return v

    @field_validator("parent_path")
    @classmethod
    def parent_valid(cls, v):
        if v is not None and (not _SUBVOL_PATH_RE.match(v) or ".." in v.split("/")):
            raise ValueError("invalid parent_path")
        return v


class BtrfsReceiveRequest(BaseModel):
    source_file: str

    @field_validator("source_file")
    @classmethod
    def file_valid(cls, v):
        if not _STREAM_FILE_RE.match(v):
            raise ValueError("source_file must be a .btrfs filename")
        return v


class CompressionRequest(BaseModel):
    compression: str

    @field_validator("compression")
    @classmethod
    def compression_valid(cls, v):
        if not _COMPRESS_RE.match(v):
            raise ValueError("invalid compression value")
        return v


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
