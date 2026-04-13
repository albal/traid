"""Unit tests for command whitelist enforcement."""

import pytest
from worker.command_validator import validate_request, ValidationError


# ---------------------------------------------------------------------------
# Valid requests
# ---------------------------------------------------------------------------

def test_disk_scan_valid():
    action, params = validate_request({"action": "disk_scan", "params": {}})
    assert action == "disk_scan"
    assert params == {}


def test_disk_scan_no_params_key():
    # params key is optional
    action, params = validate_request({"action": "disk_scan"})
    assert action == "disk_scan"


def test_array_detail_valid():
    action, params = validate_request({
        "action": "array_detail",
        "params": {"device": "/dev/md0"},
    })
    assert action == "array_detail"
    assert params["device"] == "/dev/md0"


def test_array_create_valid():
    action, params = validate_request({
        "action": "array_create",
        "params": {"disks": ["/dev/sdb", "/dev/sdc", "/dev/sdd"], "type": "traid1"},
    })
    assert action == "array_create"
    assert params["disks"] == ["/dev/sdb", "/dev/sdc", "/dev/sdd"]
    assert params["type"] == "traid1"


def test_array_create_traid2():
    action, params = validate_request({
        "action": "array_create",
        "params": {"disks": ["/dev/sdb", "/dev/sdc"], "type": "traid2"},
    })
    assert params["type"] == "traid2"


def test_lvm_report_valid():
    action, _ = validate_request({"action": "lvm_report", "params": {}})
    assert action == "lvm_report"


def test_mdstat_subscribe_valid():
    action, _ = validate_request({"action": "mdstat_subscribe"})
    assert action == "mdstat_subscribe"


# ---------------------------------------------------------------------------
# Unknown / missing action
# ---------------------------------------------------------------------------

def test_unknown_action():
    with pytest.raises(ValidationError, match="unknown action"):
        validate_request({"action": "rm_rf", "params": {}})


def test_missing_action():
    with pytest.raises(ValidationError, match="unknown action"):
        validate_request({"params": {}})


def test_non_string_action():
    with pytest.raises(ValidationError, match="unknown action"):
        validate_request({"action": 42})


# ---------------------------------------------------------------------------
# Extra / missing params
# ---------------------------------------------------------------------------

def test_extra_param_rejected():
    with pytest.raises(ValidationError, match="unexpected param keys"):
        validate_request({
            "action": "disk_scan",
            "params": {"evil": "value"},
        })


def test_missing_required_param():
    with pytest.raises(ValidationError, match="missing required params"):
        validate_request({
            "action": "array_detail",
            "params": {},
        })


def test_missing_disks_param():
    with pytest.raises(ValidationError, match="missing required params"):
        validate_request({
            "action": "array_create",
            "params": {"type": "traid1"},
        })


# ---------------------------------------------------------------------------
# Injection / path traversal attempts
# ---------------------------------------------------------------------------

def test_shell_injection_in_disk_path():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["/dev/sdb; rm -rf /"], "type": "traid1"},
        })


def test_proc_path_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["/proc/self/mem"], "type": "traid1"},
        })


def test_path_traversal_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["../../etc/passwd"], "type": "traid1"},
        })


def test_md_path_injection_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_detail",
            "params": {"device": "/dev/md0; cat /etc/shadow"},
        })


# ---------------------------------------------------------------------------
# Empty disk list
# ---------------------------------------------------------------------------

def test_empty_disks_list():
    with pytest.raises(ValidationError, match="non-empty"):
        validate_request({
            "action": "array_create",
            "params": {"disks": [], "type": "traid1"},
        })


# ---------------------------------------------------------------------------
# Duplicate disks
# ---------------------------------------------------------------------------

def test_duplicate_disks_rejected():
    with pytest.raises(ValidationError, match="duplicate"):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["/dev/sdb", "/dev/sdb"], "type": "traid1"},
        })


# ---------------------------------------------------------------------------
# Invalid RAID type
# ---------------------------------------------------------------------------

def test_invalid_raid_type():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["/dev/sdb", "/dev/sdc"], "type": "shr1"},
        })


# ---------------------------------------------------------------------------
# Payload type check
# ---------------------------------------------------------------------------

def test_non_dict_payload():
    with pytest.raises(ValidationError, match="JSON object"):
        validate_request("not a dict")


def test_non_dict_params():
    with pytest.raises(ValidationError, match="JSON object"):
        validate_request({"action": "disk_scan", "params": "bad"})
