"""
Command whitelist enforcement — the security kernel of the daemon.

Never accepts arbitrary shell strings. validate_request() returns a
(executable, args) tuple consumed directly by asyncio.create_subprocess_exec,
so no shell interpolation is possible at any stage.
"""

import re
from typing import Any

# Regex for valid block device paths: /dev/sdX, /dev/nvme0n1, /dev/vda, etc.
# Rejects path traversal, /proc/, embedded whitespace, shell metacharacters.
_DEV_PATH_RE = re.compile(r"^/dev/[a-z]{2,8}[0-9]{0,3}(p[0-9]{1,3})?$")

# Regex for valid /dev/mdX device paths
_MD_PATH_RE = re.compile(r"^/dev/md[0-9]{1,4}$")


class ValidationError(Exception):
    pass


def _validate_dev_path(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{field}: expected string, got {type(value).__name__}")
    if not _DEV_PATH_RE.match(value):
        raise ValidationError(f"{field}: invalid device path {value!r}")
    return value


def _validate_md_path(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{field}: expected string, got {type(value).__name__}")
    if not _MD_PATH_RE.match(value):
        raise ValidationError(f"{field}: invalid md device path {value!r}")
    return value


def _validate_raid_type(value: Any, field: str) -> str:
    allowed = {"traid1", "traid2"}
    if value not in allowed:
        raise ValidationError(f"{field}: must be one of {sorted(allowed)}, got {value!r}")
    return value


# Each action entry: required_params and optional_params define the schema.
# Values are callables (validator, field_name) -> validated_value.
_ALLOWED_ACTIONS: dict[str, dict] = {
    "disk_scan": {
        "required": [],
        "optional": [],
    },
    "array_detail": {
        "required": ["device"],
        "optional": [],
    },
    "lvm_report": {
        "required": [],
        "optional": [],
    },
    "array_create": {
        "required": ["disks", "type"],
        "optional": [],
    },
    "mdstat_subscribe": {
        "required": [],
        "optional": [],
    },
}


def validate_request(payload: dict) -> tuple[str, dict]:
    """
    Validate an incoming request payload against the action whitelist.

    Returns (action, validated_params). Raises ValidationError on any
    schema violation, unknown action, extra keys, or bad parameter values.
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

    # Reject unexpected keys
    extra = set(raw_params.keys()) - allowed_keys
    if extra:
        raise ValidationError(f"unexpected param keys for {action!r}: {sorted(extra)}")

    # Check all required keys are present
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
        # Reject duplicates
        if len(set(validated["disks"])) != len(validated["disks"]):
            raise ValidationError("disks: duplicate device paths")
        validated["type"] = _validate_raid_type(raw_params["type"], "type")

    return action, validated
