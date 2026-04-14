"""Unit tests for command whitelist enforcement."""

import pytest
from worker.command_validator import validate_request, ValidationError


# ---------------------------------------------------------------------------
# Valid no-param actions
# ---------------------------------------------------------------------------

def test_disk_scan_valid():
    action, params = validate_request({"action": "disk_scan", "params": {}})
    assert action == "disk_scan"
    assert params == {}

def test_disk_scan_no_params_key():
    action, params = validate_request({"action": "disk_scan"})
    assert action == "disk_scan"

def test_lvm_report_valid():
    action, _ = validate_request({"action": "lvm_report", "params": {}})
    assert action == "lvm_report"

def test_jobs_list_valid():
    action, _ = validate_request({"action": "jobs_list"})
    assert action == "jobs_list"

def test_mdstat_subscribe_valid():
    action, _ = validate_request({"action": "mdstat_subscribe"})
    assert action == "mdstat_subscribe"


# ---------------------------------------------------------------------------
# array_detail
# ---------------------------------------------------------------------------

def test_array_detail_valid():
    action, params = validate_request({
        "action": "array_detail",
        "params": {"device": "/dev/md0"},
    })
    assert action == "array_detail"
    assert params["device"] == "/dev/md0"

def test_array_detail_md127():
    _, params = validate_request({"action": "array_detail", "params": {"device": "/dev/md127"}})
    assert params["device"] == "/dev/md127"

def test_array_detail_non_md_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "array_detail", "params": {"device": "/dev/sdb"}})

def test_array_detail_injection_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "array_detail", "params": {"device": "/dev/md0; cat /etc/shadow"}})


# ---------------------------------------------------------------------------
# array_create
# ---------------------------------------------------------------------------

def test_array_create_traid1():
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

def test_array_create_with_vg_name():
    _, params = validate_request({
        "action": "array_create",
        "params": {"disks": ["/dev/sdb", "/dev/sdc"], "type": "traid1", "vg_name": "myarray"},
    })
    assert params["vg_name"] == "myarray"

def test_array_create_nvme_disks():
    _, params = validate_request({
        "action": "array_create",
        "params": {"disks": ["/dev/nvme0n1", "/dev/nvme1n1"], "type": "traid1"},
    })
    assert "/dev/nvme0n1" in params["disks"]

def test_array_create_empty_disks():
    with pytest.raises(ValidationError, match="non-empty"):
        validate_request({"action": "array_create", "params": {"disks": [], "type": "traid1"}})

def test_array_create_too_many_disks():
    # 25 disks exceeds the max of 24
    disks = [f"/dev/sd{chr(ord('b') + i)}" for i in range(25)]
    with pytest.raises(ValidationError, match="too many"):
        validate_request({"action": "array_create", "params": {"disks": disks, "type": "traid1"}})

def test_array_create_duplicate_disks():
    with pytest.raises(ValidationError, match="duplicate"):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["/dev/sdb", "/dev/sdb"], "type": "traid1"},
        })

def test_array_create_invalid_type():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["/dev/sdb", "/dev/sdc"], "type": "shr1"},
        })

def test_array_create_shell_injection():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["/dev/sdb; rm -rf /"], "type": "traid1"},
        })

def test_array_create_proc_path_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["/proc/self/mem"], "type": "traid1"},
        })

def test_array_create_path_traversal_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_create",
            "params": {"disks": ["../../etc/passwd"], "type": "traid1"},
        })


# ---------------------------------------------------------------------------
# array_delete / vg_rename
# ---------------------------------------------------------------------------

def test_array_delete_valid():
    action, params = validate_request({"action": "array_delete", "params": {"vg_name": "traid_vg"}})
    assert action == "array_delete"
    assert params["vg_name"] == "traid_vg"

def test_vg_rename_valid():
    _, params = validate_request({
        "action": "vg_rename",
        "params": {"vg_name": "old_name", "new_name": "new_name"},
    })
    assert params["new_name"] == "new_name"


# ---------------------------------------------------------------------------
# VG name edge cases
# ---------------------------------------------------------------------------

def test_vg_name_alphanumeric():
    _, params = validate_request({"action": "array_delete", "params": {"vg_name": "traid1"}})
    assert params["vg_name"] == "traid1"

def test_vg_name_with_underscore():
    _, params = validate_request({"action": "array_delete", "params": {"vg_name": "my_array"}})
    assert params["vg_name"] == "my_array"

def test_vg_name_with_dot():
    _, params = validate_request({"action": "array_delete", "params": {"vg_name": "my.array"}})
    assert params["vg_name"] == "my.array"

def test_vg_name_with_plus():
    _, params = validate_request({"action": "array_delete", "params": {"vg_name": "array+01"}})
    assert params["vg_name"] == "array+01"

def test_vg_name_starts_with_digit_invalid():
    with pytest.raises(ValidationError):
        validate_request({"action": "array_delete", "params": {"vg_name": "0traid"}})

def test_vg_name_with_slash_invalid():
    with pytest.raises(ValidationError):
        validate_request({"action": "array_delete", "params": {"vg_name": "traid/vg"}})

def test_vg_name_empty_invalid():
    with pytest.raises(ValidationError):
        validate_request({"action": "array_delete", "params": {"vg_name": ""}})

def test_vg_name_space_invalid():
    with pytest.raises(ValidationError):
        validate_request({"action": "array_delete", "params": {"vg_name": "my array"}})


# ---------------------------------------------------------------------------
# job_delete
# ---------------------------------------------------------------------------

def test_job_delete_valid_uuid():
    _, params = validate_request({
        "action": "job_delete",
        "params": {"job_id": "550e8400-e29b-41d4-a716-446655440000"},
    })
    assert params["job_id"] == "550e8400-e29b-41d4-a716-446655440000"

def test_job_delete_invalid_uuid():
    with pytest.raises(ValidationError):
        validate_request({"action": "job_delete", "params": {"job_id": "not-a-uuid"}})

def test_job_delete_injection_uuid():
    with pytest.raises(ValidationError):
        validate_request({"action": "job_delete", "params": {"job_id": "'; DROP TABLE jobs;--"}})


# ---------------------------------------------------------------------------
# array_migrate
# ---------------------------------------------------------------------------

def test_array_migrate_traid1_to_traid2():
    action, params = validate_request({
        "action": "array_migrate",
        "params": {"vg_name": "traid_vg", "direction": "traid1_to_traid2"},
    })
    assert action == "array_migrate"
    assert params["direction"] == "traid1_to_traid2"

def test_array_migrate_traid2_to_traid1():
    _, params = validate_request({
        "action": "array_migrate",
        "params": {"vg_name": "traid_vg", "direction": "traid2_to_traid1"},
    })
    assert params["direction"] == "traid2_to_traid1"

def test_array_migrate_with_new_disk():
    _, params = validate_request({
        "action": "array_migrate",
        "params": {"vg_name": "traid_vg", "direction": "traid1_to_traid2", "new_disk": "/dev/sde"},
    })
    assert params["new_disk"] == "/dev/sde"

def test_array_migrate_invalid_direction():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_migrate",
            "params": {"vg_name": "traid_vg", "direction": "traid2_to_traid3"},
        })

def test_array_migrate_invalid_disk():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_migrate",
            "params": {"vg_name": "traid_vg", "direction": "traid1_to_traid2", "new_disk": "sde"},
        })


# ---------------------------------------------------------------------------
# disk_replace
# ---------------------------------------------------------------------------

def test_disk_replace_valid():
    action, params = validate_request({
        "action": "disk_replace",
        "params": {"vg_name": "traid_vg", "old_disk": "/dev/sdb", "new_disk": "/dev/sde"},
    })
    assert action == "disk_replace"
    assert params["old_disk"] == "/dev/sdb"
    assert params["new_disk"] == "/dev/sde"

def test_disk_replace_nvme():
    _, params = validate_request({
        "action": "disk_replace",
        "params": {"vg_name": "traid_vg", "old_disk": "/dev/sdb", "new_disk": "/dev/nvme0n1"},
    })
    assert params["new_disk"] == "/dev/nvme0n1"

def test_disk_replace_injection_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "disk_replace",
            "params": {"vg_name": "traid_vg", "old_disk": "/dev/sdb", "new_disk": "/dev/sdc && rm -rf /"},
        })


# ---------------------------------------------------------------------------
# array_grow / array_shrink
# ---------------------------------------------------------------------------

def test_array_grow_valid():
    action, params = validate_request({
        "action": "array_grow",
        "params": {"vg_name": "traid_vg", "new_disk": "/dev/sde"},
    })
    assert action == "array_grow"
    assert params["new_disk"] == "/dev/sde"

def test_array_shrink_valid():
    action, params = validate_request({
        "action": "array_shrink",
        "params": {"vg_name": "traid_vg", "disk_to_remove": "/dev/sdb"},
    })
    assert action == "array_shrink"
    assert params["disk_to_remove"] == "/dev/sdb"

def test_array_shrink_invalid_path():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "array_shrink",
            "params": {"vg_name": "traid_vg", "disk_to_remove": "/dev/md0"},
        })


# ---------------------------------------------------------------------------
# volume_clone
# ---------------------------------------------------------------------------

def test_volume_clone_valid():
    action, params = validate_request({
        "action": "volume_clone",
        "params": {"vg_name": "traid_vg", "target_disk": "/dev/sdf"},
    })
    assert action == "volume_clone"
    assert params["target_disk"] == "/dev/sdf"

def test_volume_clone_nvme_target():
    _, params = validate_request({
        "action": "volume_clone",
        "params": {"vg_name": "traid_vg", "target_disk": "/dev/nvme0n1"},
    })
    assert params["target_disk"] == "/dev/nvme0n1"


# ---------------------------------------------------------------------------
# volume_backup
# ---------------------------------------------------------------------------

def test_volume_backup_nfs_valid():
    action, params = validate_request({
        "action": "volume_backup",
        "params": {
            "vg_name": "traid_vg",
            "protocol": "nfs",
            "host": "192.168.1.10",
            "remote_path": "192.168.1.10:/backup",
        },
    })
    assert action == "volume_backup"
    assert params["protocol"] == "nfs"

def test_volume_backup_cifs_valid():
    _, params = validate_request({
        "action": "volume_backup",
        "params": {
            "vg_name": "traid_vg",
            "protocol": "cifs",
            "host": "nas.local",
            "remote_path": "//nas.local/backup",
            "cifs_user": "admin",
            "cifs_pass": "s3cret",
        },
    })
    assert params["cifs_user"] == "admin"
    assert params["cifs_pass"] == "s3cret"

def test_volume_backup_defaults_empty_creds():
    _, params = validate_request({
        "action": "volume_backup",
        "params": {
            "vg_name": "traid_vg",
            "protocol": "nfs",
            "host": "192.168.1.10",
            "remote_path": "192.168.1.10:/backup",
        },
    })
    assert params["cifs_user"] == ""
    assert params["cifs_pass"] == ""

def test_volume_backup_invalid_protocol():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "volume_backup",
            "params": {
                "vg_name": "traid_vg", "protocol": "ftp",
                "host": "192.168.1.10", "remote_path": "192.168.1.10:/backup",
            },
        })

def test_volume_backup_host_injection_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "volume_backup",
            "params": {
                "vg_name": "traid_vg", "protocol": "nfs",
                "host": "host; rm -rf /",
                "remote_path": "host:/path",
            },
        })

def test_volume_backup_cred_newline_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "volume_backup",
            "params": {
                "vg_name": "traid_vg", "protocol": "cifs",
                "host": "nas", "remote_path": "//nas/share",
                "cifs_user": "admin\nrm -rf /", "cifs_pass": "",
            },
        })

def test_volume_backup_invalid_remote_path():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "volume_backup",
            "params": {
                "vg_name": "traid_vg", "protocol": "nfs",
                "host": "192.168.1.10", "remote_path": "nodivider",
            },
        })


# ---------------------------------------------------------------------------
# smart_test
# ---------------------------------------------------------------------------

def test_smart_test_short_valid():
    action, params = validate_request({
        "action": "smart_test",
        "params": {"disk": "/dev/sdb", "test_type": "short"},
    })
    assert action == "smart_test"
    assert params["test_type"] == "short"

def test_smart_test_long_valid():
    _, params = validate_request({
        "action": "smart_test",
        "params": {"disk": "/dev/nvme0n1", "test_type": "long"},
    })
    assert params["test_type"] == "long"

def test_smart_test_invalid_type():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "smart_test",
            "params": {"disk": "/dev/sdb", "test_type": "full"},
        })

def test_smart_test_invalid_disk():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "smart_test",
            "params": {"disk": "sdb", "test_type": "short"},
        })


# ---------------------------------------------------------------------------
# badblocks_test
# ---------------------------------------------------------------------------

def test_badblocks_test_valid():
    action, params = validate_request({
        "action": "badblocks_test",
        "params": {"disk": "/dev/sdb"},
    })
    assert action == "badblocks_test"
    assert params["disk"] == "/dev/sdb"

def test_badblocks_test_virtio():
    _, params = validate_request({
        "action": "badblocks_test",
        "params": {"disk": "/dev/vdb"},
    })
    assert params["disk"] == "/dev/vdb"


# ---------------------------------------------------------------------------
# disk_erase
# ---------------------------------------------------------------------------

def test_disk_erase_no_mode():
    action, params = validate_request({
        "action": "disk_erase",
        "params": {"disk": "/dev/sdb"},
    })
    assert action == "disk_erase"
    assert "mode" not in params  # mode is optional

def test_disk_erase_quick_mode():
    _, params = validate_request({
        "action": "disk_erase",
        "params": {"disk": "/dev/sdb", "mode": "quick"},
    })
    assert params["mode"] == "quick"

def test_disk_erase_dod_short_mode():
    _, params = validate_request({
        "action": "disk_erase",
        "params": {"disk": "/dev/sdb", "mode": "dod_short"},
    })
    assert params["mode"] == "dod_short"

def test_disk_erase_dod7_mode():
    _, params = validate_request({
        "action": "disk_erase",
        "params": {"disk": "/dev/sdb", "mode": "dod_7"},
    })
    assert params["mode"] == "dod_7"

def test_disk_erase_invalid_mode():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "disk_erase",
            "params": {"disk": "/dev/sdb", "mode": "gutmann"},
        })

def test_disk_erase_invalid_disk():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "disk_erase",
            "params": {"disk": "/etc/passwd"},
        })


# ---------------------------------------------------------------------------
# Unknown / missing / malformed action
# ---------------------------------------------------------------------------

def test_unknown_action():
    with pytest.raises(ValidationError, match="unknown action"):
        validate_request({"action": "rm_rf", "params": {}})

def test_missing_action():
    with pytest.raises(ValidationError, match="unknown action"):
        validate_request({"params": {}})

def test_none_action():
    with pytest.raises(ValidationError, match="unknown action"):
        validate_request({"action": None})

def test_non_string_action():
    with pytest.raises(ValidationError, match="unknown action"):
        validate_request({"action": 42})


# ---------------------------------------------------------------------------
# Extra / missing params
# ---------------------------------------------------------------------------

def test_extra_param_rejected():
    with pytest.raises(ValidationError, match="unexpected param keys"):
        validate_request({"action": "disk_scan", "params": {"evil": "value"}})

def test_missing_required_param_device():
    with pytest.raises(ValidationError, match="missing required params"):
        validate_request({"action": "array_detail", "params": {}})

def test_missing_required_param_disks():
    with pytest.raises(ValidationError, match="missing required params"):
        validate_request({"action": "array_create", "params": {"type": "traid1"}})

def test_missing_required_param_type():
    with pytest.raises(ValidationError, match="missing required params"):
        validate_request({"action": "array_create", "params": {"disks": ["/dev/sdb", "/dev/sdc"]}})


# ---------------------------------------------------------------------------
# Payload type checks
# ---------------------------------------------------------------------------

def test_non_dict_payload():
    with pytest.raises(ValidationError, match="JSON object"):
        validate_request("not a dict")

def test_list_payload():
    with pytest.raises(ValidationError, match="JSON object"):
        validate_request(["disk_scan"])

def test_non_dict_params():
    with pytest.raises(ValidationError, match="JSON object"):
        validate_request({"action": "disk_scan", "params": "bad"})

def test_list_params():
    with pytest.raises(ValidationError, match="JSON object"):
        validate_request({"action": "disk_scan", "params": []})


# ---------------------------------------------------------------------------
# Device path variations
# ---------------------------------------------------------------------------

def test_sata_disk_valid():
    _, params = validate_request({"action": "badblocks_test", "params": {"disk": "/dev/sda"}})
    assert params["disk"] == "/dev/sda"

def test_virtio_disk_valid():
    _, params = validate_request({"action": "disk_erase", "params": {"disk": "/dev/vda"}})
    assert params["disk"] == "/dev/vda"

def test_nvme_disk_valid():
    _, params = validate_request({"action": "smart_test", "params": {"disk": "/dev/nvme0n1", "test_type": "short"}})
    assert params["disk"] == "/dev/nvme0n1"

def test_nvme_partition_valid_for_grow():
    _, params = validate_request({"action": "array_grow", "params": {"vg_name": "vg", "new_disk": "/dev/nvme0n1"}})
    assert params["new_disk"] == "/dev/nvme0n1"

def test_dev_null_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "disk_erase", "params": {"disk": "/dev/null"}})

def test_dev_zero_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "disk_erase", "params": {"disk": "/dev/zero"}})
