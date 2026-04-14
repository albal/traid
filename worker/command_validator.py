"""
Command whitelist enforcement — the security kernel of the daemon.

Never accepts arbitrary shell strings. validate_request() returns a
(action, validated_params) tuple; all subprocess execution goes through
asyncio.create_subprocess_exec, so no shell interpolation is possible.
"""

import re
from typing import Any

# Subvolume / snapshot relative paths: no shell metacharacters, no ..
_SUBVOL_PATH_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_./@-]{0,254}$")
# Filesystem label
_FS_LABEL_RE = re.compile(r"^[a-zA-Z0-9_.+-]{0,255}$")
# Filename for btrfs send streams: UUID-style or simple name, no path separators
_STREAM_FILE_RE = re.compile(r"^[a-zA-Z0-9_.@-]{1,200}\.btrfs$")
# Btrfs qgroup ID (e.g. "0/256")
_QGROUP_RE = re.compile(r"^\d+/\d+$")

# Block device paths: traditional (sda/vda/xvda) and NVMe (nvme0n1/nvme0n1p1)
_DEV_PATH_RE = re.compile(
    r"^/dev/(?:"
    r"[a-z]{2,8}[0-9]{0,3}(?:p[0-9]{1,3})?"   # sda, vda, xvda, sda1, etc.
    r"|nvme[0-9]{1,4}n[0-9]{1,4}(?:p[0-9]{1,3})?"  # nvme0n1, nvme0n1p1
    r"|mmcblk[0-9]{1,4}(?:p[0-9]{1,3})?"           # mmcblk0p1
    r")$"
)

# /dev/mdX device paths
_MD_PATH_RE = re.compile(r"^/dev/md[0-9]{1,4}$")

# LVM volume group names — must start with a letter or underscore
_VG_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.+-]{0,126}$")

# UUID job IDs
_JOB_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

# Remote path for backup: NFS host:/path or CIFS //host/share
_REMOTE_PATH_RE = re.compile(r"^(?://|[a-zA-Z0-9._-]{1,253}[:/]).{1,255}$")

# CIFS credential: no newlines or shell metacharacters
_CRED_RE = re.compile(r"^[^\n\r;&|`$<>]{0,256}$")

# Dangerous pseudo-devices that must never be used as targets
_BLOCKED_DEV_PATHS = frozenset({
    "/dev/null", "/dev/zero", "/dev/full", "/dev/random", "/dev/urandom",
})


class ValidationError(Exception):
    pass


def _validate_dev_path(value: Any, field: str, *, allow_md: bool = False) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{field}: expected string, got {type(value).__name__}")
    if not _DEV_PATH_RE.match(value):
        raise ValidationError(f"{field}: invalid device path {value!r}")
    if value in _BLOCKED_DEV_PATHS:
        raise ValidationError(f"{field}: device not allowed {value!r}")
    if not allow_md and _MD_PATH_RE.match(value):
        raise ValidationError(f"{field}: md virtual devices not allowed here {value!r}")
    return value


def _validate_md_path(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{field}: expected string, got {type(value).__name__}")
    if not _MD_PATH_RE.match(value):
        raise ValidationError(f"{field}: invalid md device path {value!r}")
    return value


def _validate_vg_name(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{field}: expected string, got {type(value).__name__}")
    if not _VG_NAME_RE.match(value):
        raise ValidationError(f"{field}: invalid volume group name {value!r}")
    return value


def _validate_raid_type(value: Any, field: str) -> str:
    allowed = {"traid1", "traid2"}
    if value not in allowed:
        raise ValidationError(f"{field}: must be one of {sorted(allowed)}, got {value!r}")
    return value


def _validate_direction(value: Any, field: str) -> str:
    allowed = {"traid1_to_traid2", "traid2_to_traid1"}
    if value not in allowed:
        raise ValidationError(f"{field}: must be one of {sorted(allowed)}, got {value!r}")
    return value


def _validate_test_type(value: Any, field: str) -> str:
    allowed = {"short", "long"}
    if value not in allowed:
        raise ValidationError(f"{field}: must be 'short' or 'long', got {value!r}")
    return value


def _validate_protocol(value: Any, field: str) -> str:
    allowed = {"cifs", "nfs"}
    if value not in allowed:
        raise ValidationError(f"{field}: must be 'cifs' or 'nfs', got {value!r}")
    return value


def _validate_remote_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _REMOTE_PATH_RE.match(value):
        raise ValidationError(f"{field}: invalid remote path {value!r}")
    return value


def _validate_cred(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _CRED_RE.match(value):
        raise ValidationError(f"{field}: invalid credential value")
    return value


_ALLOWED_ACTIONS: dict[str, dict] = {
    # ---- existing ----
    "disk_scan":         {"required": [], "optional": []},
    "array_detail":      {"required": ["device"], "optional": []},
    "lvm_report":        {"required": [], "optional": []},
    "array_create":      {"required": ["disks", "type"], "optional": ["vg_name"]},
    "mdstat_subscribe":  {"required": [], "optional": []},
    "array_delete":      {"required": ["vg_name"], "optional": []},
    "vg_rename":         {"required": ["vg_name", "new_name"], "optional": []},
    "jobs_list":         {"required": [], "optional": []},
    "job_delete":        {"required": ["job_id"], "optional": []},
    # ---- new ----
    "array_migrate":     {"required": ["vg_name", "direction"], "optional": ["new_disk"]},
    "disk_replace":      {"required": ["vg_name", "old_disk", "new_disk"], "optional": []},
    "array_grow":        {"required": ["vg_name", "new_disk"], "optional": []},
    "array_shrink":      {"required": ["vg_name", "disk_to_remove"], "optional": []},
    "volume_clone":      {"required": ["vg_name", "target_disk"], "optional": []},
    "volume_backup":     {"required": ["vg_name", "protocol", "host", "remote_path"],
                          "optional": ["cifs_user", "cifs_pass"]},
    "smart_test":        {"required": ["disk", "test_type"], "optional": []},
    "badblocks_test":    {"required": ["disk"], "optional": []},
    "disk_erase":        {"required": ["disk"], "optional": ["mode"]},
    # ---- filesystem management ----
    "fs_format":         {"required": ["vg_name", "fstype"], "optional": ["label", "compression"]},
    "fs_mount":          {"required": ["vg_name"], "optional": []},
    "fs_unmount":        {"required": ["vg_name"], "optional": []},
    "fs_info":           {"required": ["vg_name"], "optional": []},
    "fs_set_compression":{"required": ["vg_name", "compression"], "optional": []},
    # ---- btrfs subvolumes / snapshots ----
    "btrfs_subvol_list":    {"required": ["vg_name"], "optional": []},
    "btrfs_subvol_create":  {"required": ["vg_name", "name"], "optional": []},
    "btrfs_subvol_delete":  {"required": ["vg_name", "path"], "optional": ["recursive"]},
    "btrfs_snapshot_create":{"required": ["vg_name", "source_path", "dest_path"],
                              "optional": ["readonly"]},
    "btrfs_subvol_set_default": {"required": ["vg_name", "subvol_id"], "optional": []},
    # ---- btrfs maintenance ----
    "btrfs_scrub_start":    {"required": ["vg_name"], "optional": []},
    "btrfs_scrub_status":   {"required": ["vg_name"], "optional": []},
    "btrfs_scrub_cancel":   {"required": ["vg_name"], "optional": []},
    "btrfs_balance_start":  {"required": ["vg_name"],
                              "optional": ["usage_filter", "metadata_usage"]},
    "btrfs_balance_status": {"required": ["vg_name"], "optional": []},
    "btrfs_balance_cancel": {"required": ["vg_name"], "optional": []},
    "btrfs_defrag":         {"required": ["vg_name"],
                              "optional": ["path", "recursive", "compression"]},
    "btrfs_dedup":          {"required": ["vg_name"], "optional": ["path"]},
    # ---- btrfs quotas ----
    "btrfs_quota_enable":   {"required": ["vg_name"], "optional": []},
    "btrfs_quota_list":     {"required": ["vg_name"], "optional": []},
    "btrfs_quota_set":      {"required": ["vg_name", "qgroup", "limit_bytes"], "optional": []},
    # ---- btrfs usage / stats ----
    "btrfs_usage_detail":   {"required": ["vg_name"], "optional": []},
    # ---- btrfs send / receive ----
    "btrfs_send":           {"required": ["vg_name", "snapshot_path", "dest_file"],
                              "optional": ["parent_path"]},
    "btrfs_receive":        {"required": ["vg_name", "source_file"], "optional": []},
}


def validate_request(payload: dict) -> tuple[str, dict]:
    """
    Validate an incoming request payload against the action whitelist.
    Returns (action, validated_params). Raises ValidationError on any violation.
    """
    if not isinstance(payload, dict):
        raise ValidationError("payload must be a JSON object")

    action = payload.get("action")
    if not isinstance(action, str) or action not in _ALLOWED_ACTIONS:
        raise ValidationError(f"unknown action: {action!r}")

    raw_params = payload.get("params", {})
    if not isinstance(raw_params, dict):
        raise ValidationError("params must be a JSON object")

    schema = _ALLOWED_ACTIONS[action]
    required = set(schema["required"])
    optional = set(schema["optional"])
    allowed_keys = required | optional

    extra = set(raw_params.keys()) - allowed_keys
    if extra:
        raise ValidationError(f"unexpected param keys for {action!r}: {sorted(extra)}")

    missing = required - set(raw_params.keys())
    if missing:
        raise ValidationError(f"missing required params for {action!r}: {sorted(missing)}")

    validated: dict = {}

    if action == "array_detail":
        validated["device"] = _validate_md_path(raw_params["device"], "device")

    elif action == "array_create":
        disks_raw = raw_params["disks"]
        if not isinstance(disks_raw, list) or len(disks_raw) == 0:
            raise ValidationError("disks: must be a non-empty list")
        if len(disks_raw) > 24:
            raise ValidationError("disks: too many devices (max 24)")
        validated["disks"] = [
            _validate_dev_path(d, f"disks[{i}]") for i, d in enumerate(disks_raw)
        ]
        if len(set(validated["disks"])) != len(validated["disks"]):
            raise ValidationError("disks: duplicate device paths")
        validated["type"] = _validate_raid_type(raw_params["type"], "type")
        if "vg_name" in raw_params:
            validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")

    elif action == "array_delete":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")

    elif action == "vg_rename":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        validated["new_name"] = _validate_vg_name(raw_params["new_name"], "new_name")

    elif action == "job_delete":
        job_id = raw_params.get("job_id", "")
        if not isinstance(job_id, str) or not _JOB_ID_RE.match(job_id):
            raise ValidationError(f"job_id: invalid UUID {job_id!r}")
        validated["job_id"] = job_id

    elif action == "array_migrate":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        validated["direction"] = _validate_direction(raw_params["direction"], "direction")
        if "new_disk" in raw_params:
            validated["new_disk"] = _validate_dev_path(raw_params["new_disk"], "new_disk")

    elif action == "disk_replace":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        validated["old_disk"] = _validate_dev_path(raw_params["old_disk"], "old_disk")
        validated["new_disk"] = _validate_dev_path(raw_params["new_disk"], "new_disk")

    elif action == "array_grow":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        validated["new_disk"] = _validate_dev_path(raw_params["new_disk"], "new_disk")

    elif action == "array_shrink":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        validated["disk_to_remove"] = _validate_dev_path(raw_params["disk_to_remove"], "disk_to_remove")

    elif action == "volume_clone":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        validated["target_disk"] = _validate_dev_path(raw_params["target_disk"], "target_disk")

    elif action == "volume_backup":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        validated["protocol"] = _validate_protocol(raw_params["protocol"], "protocol")
        validated["host"] = _validate_cred(raw_params["host"], "host")
        validated["remote_path"] = _validate_remote_path(raw_params["remote_path"], "remote_path")
        validated["cifs_user"] = _validate_cred(raw_params.get("cifs_user", ""), "cifs_user")
        validated["cifs_pass"] = _validate_cred(raw_params.get("cifs_pass", ""), "cifs_pass")

    elif action == "smart_test":
        validated["disk"] = _validate_dev_path(raw_params["disk"], "disk")
        validated["test_type"] = _validate_test_type(raw_params["test_type"], "test_type")

    elif action == "badblocks_test":
        validated["disk"] = _validate_dev_path(raw_params["disk"], "disk")

    elif action == "disk_erase":
        validated["disk"] = _validate_dev_path(raw_params["disk"], "disk")
        if "mode" in raw_params:
            allowed_modes = {"quick", "dod_short", "dod_7"}
            if raw_params["mode"] not in allowed_modes:
                raise ValidationError(f"mode: must be one of {sorted(allowed_modes)}")
            validated["mode"] = raw_params["mode"]

    # ---- filesystem management ----
    elif action == "fs_format":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        fstype = raw_params["fstype"]
        if fstype not in ("ext4", "btrfs"):
            raise ValidationError("fstype: must be 'ext4' or 'btrfs'")
        validated["fstype"] = fstype
        if "label" in raw_params:
            label = raw_params["label"]
            if not isinstance(label, str) or not _FS_LABEL_RE.match(label):
                raise ValidationError("label: invalid filesystem label")
            validated["label"] = label
        if "compression" in raw_params:
            comp = raw_params["compression"]
            if comp not in ("zstd", "lzo", "zlib", "none", ""):
                raise ValidationError("compression: must be zstd, lzo, zlib, none, or ''")
            validated["compression"] = comp

    elif action in ("fs_mount", "fs_unmount", "fs_info",
                    "btrfs_subvol_list", "btrfs_scrub_start", "btrfs_scrub_status",
                    "btrfs_scrub_cancel", "btrfs_balance_status", "btrfs_balance_cancel",
                    "btrfs_quota_enable", "btrfs_quota_list", "btrfs_usage_detail"):
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")

    elif action == "fs_set_compression":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        comp = raw_params["compression"]
        if comp not in ("zstd", "lzo", "zlib", "none", ""):
            raise ValidationError("compression: must be zstd, lzo, zlib, none, or ''")
        validated["compression"] = comp

    elif action == "btrfs_subvol_create":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        name = raw_params["name"]
        if not isinstance(name, str) or not _SUBVOL_PATH_RE.match(name):
            raise ValidationError("name: invalid subvolume name")
        if ".." in name.split("/"):
            raise ValidationError("name: path traversal not allowed")
        validated["name"] = name

    elif action == "btrfs_subvol_delete":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        path = raw_params["path"]
        if not isinstance(path, str) or not _SUBVOL_PATH_RE.match(path):
            raise ValidationError("path: invalid subvolume path")
        if ".." in path.split("/"):
            raise ValidationError("path: path traversal not allowed")
        validated["path"] = path
        if "recursive" in raw_params:
            validated["recursive"] = bool(raw_params["recursive"])

    elif action == "btrfs_snapshot_create":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        for field in ("source_path", "dest_path"):
            v = raw_params[field]
            if not isinstance(v, str) or not _SUBVOL_PATH_RE.match(v):
                raise ValidationError(f"{field}: invalid subvolume path")
            if ".." in v.split("/"):
                raise ValidationError(f"{field}: path traversal not allowed")
            validated[field] = v
        if "readonly" in raw_params:
            validated["readonly"] = bool(raw_params["readonly"])

    elif action == "btrfs_subvol_set_default":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        subvol_id = raw_params["subvol_id"]
        if not isinstance(subvol_id, int) or subvol_id < 0:
            raise ValidationError("subvol_id: must be a non-negative integer")
        validated["subvol_id"] = subvol_id

    elif action == "btrfs_balance_start":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        for field in ("usage_filter", "metadata_usage"):
            if field in raw_params:
                v = raw_params[field]
                if not isinstance(v, int) or not (0 <= v <= 100):
                    raise ValidationError(f"{field}: must be 0-100")
                validated[field] = v

    elif action == "btrfs_defrag":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        if "path" in raw_params:
            path = raw_params["path"]
            if not isinstance(path, str) or not _SUBVOL_PATH_RE.match(path):
                raise ValidationError("path: invalid path")
            if ".." in path.split("/"):
                raise ValidationError("path: path traversal not allowed")
            validated["path"] = path
        if "recursive" in raw_params:
            validated["recursive"] = bool(raw_params["recursive"])
        if "compression" in raw_params:
            comp = raw_params["compression"]
            if comp not in ("zstd", "lzo", "zlib", "none", ""):
                raise ValidationError("compression: invalid value")
            validated["compression"] = comp

    elif action == "btrfs_dedup":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        if "path" in raw_params:
            path = raw_params["path"]
            if not isinstance(path, str) or not _SUBVOL_PATH_RE.match(path):
                raise ValidationError("path: invalid path")
            validated["path"] = path

    elif action == "btrfs_quota_set":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        qgroup = raw_params["qgroup"]
        if not isinstance(qgroup, str) or not _QGROUP_RE.match(qgroup):
            raise ValidationError("qgroup: must match N/N format")
        validated["qgroup"] = qgroup
        limit = raw_params["limit_bytes"]
        if not isinstance(limit, int) or limit < 0:
            raise ValidationError("limit_bytes: must be a non-negative integer")
        validated["limit_bytes"] = limit

    elif action == "btrfs_send":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        for field in ("snapshot_path",):
            v = raw_params[field]
            if not isinstance(v, str) or not _SUBVOL_PATH_RE.match(v):
                raise ValidationError(f"{field}: invalid path")
            if ".." in v.split("/"):
                raise ValidationError(f"{field}: path traversal not allowed")
            validated[field] = v
        dest_file = raw_params["dest_file"]
        if not isinstance(dest_file, str) or not _STREAM_FILE_RE.match(dest_file):
            raise ValidationError("dest_file: must be a .btrfs filename")
        validated["dest_file"] = dest_file
        if "parent_path" in raw_params:
            p = raw_params["parent_path"]
            if not isinstance(p, str) or not _SUBVOL_PATH_RE.match(p):
                raise ValidationError("parent_path: invalid path")
            if ".." in p.split("/"):
                raise ValidationError("parent_path: path traversal not allowed")
            validated["parent_path"] = p

    elif action == "btrfs_receive":
        validated["vg_name"] = _validate_vg_name(raw_params["vg_name"], "vg_name")
        source_file = raw_params["source_file"]
        if not isinstance(source_file, str) or not _STREAM_FILE_RE.match(source_file):
            raise ValidationError("source_file: must be a .btrfs filename")
        validated["source_file"] = source_file

    return action, validated
