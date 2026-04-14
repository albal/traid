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


# ---------------------------------------------------------------------------
# Filesystem actions
# ---------------------------------------------------------------------------

def test_fs_format_ext4():
    action, params = validate_request({"action": "fs_format",
                                       "params": {"vg_name": "traid_vg", "fstype": "ext4"}})
    assert action == "fs_format"
    assert params["vg_name"] == "traid_vg"
    assert params["fstype"] == "ext4"

def test_fs_format_btrfs_with_compression():
    _, params = validate_request({"action": "fs_format",
                                  "params": {"vg_name": "traid_vg", "fstype": "btrfs",
                                             "label": "data", "compression": "zstd"}})
    assert params["fstype"] == "btrfs"
    assert params["compression"] == "zstd"

def test_fs_format_invalid_fstype_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "fs_format",
                          "params": {"vg_name": "traid_vg", "fstype": "xfs"}})

def test_fs_format_invalid_vg_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "fs_format",
                          "params": {"vg_name": "0bad", "fstype": "ext4"}})

def test_fs_format_invalid_compression_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "fs_format",
                          "params": {"vg_name": "traid_vg", "fstype": "btrfs",
                                     "compression": "gzip"}})

def test_fs_mount_valid():
    action, params = validate_request({"action": "fs_mount",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "fs_mount"
    assert params["vg_name"] == "traid_vg"

def test_fs_unmount_valid():
    action, params = validate_request({"action": "fs_unmount",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "fs_unmount"
    assert params["vg_name"] == "traid_vg"

def test_fs_info_valid():
    action, params = validate_request({"action": "fs_info",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "fs_info"
    assert params["vg_name"] == "traid_vg"

def test_fs_set_compression_valid():
    _, params = validate_request({"action": "fs_set_compression",
                                  "params": {"vg_name": "traid_vg", "compression": "zstd"}})
    assert params["compression"] == "zstd"

def test_fs_set_compression_invalid():
    with pytest.raises(ValidationError):
        validate_request({"action": "fs_set_compression",
                          "params": {"vg_name": "traid_vg", "compression": "bzip2"}})


# ---------------------------------------------------------------------------
# Btrfs subvolume actions
# ---------------------------------------------------------------------------

def test_btrfs_subvol_list():
    action, params = validate_request({"action": "btrfs_subvol_list",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_subvol_list"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_subvol_create_valid():
    _, params = validate_request({"action": "btrfs_subvol_create",
                                  "params": {"vg_name": "traid_vg", "name": "snapshots"}})
    assert params["name"] == "snapshots"

def test_btrfs_subvol_create_traversal_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_subvol_create",
                          "params": {"vg_name": "traid_vg", "name": "../etc"}})

def test_btrfs_subvol_delete_valid():
    _, params = validate_request({"action": "btrfs_subvol_delete",
                                  "params": {"vg_name": "traid_vg", "path": "snapshots/snap1"}})
    assert params["path"] == "snapshots/snap1"

def test_btrfs_subvol_delete_recursive():
    _, params = validate_request({"action": "btrfs_subvol_delete",
                                  "params": {"vg_name": "traid_vg", "path": "snapshots",
                                             "recursive": True}})
    assert params["recursive"] is True

def test_btrfs_subvol_delete_traversal_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_subvol_delete",
                          "params": {"vg_name": "traid_vg", "path": "../../etc"}})

def test_btrfs_snapshot_create_valid():
    _, params = validate_request({"action": "btrfs_snapshot_create",
                                  "params": {"vg_name": "traid_vg",
                                             "source_path": "data",
                                             "dest_path": "snaps/s1",
                                             "readonly": True}})
    assert params["readonly"] is True

def test_btrfs_snapshot_traversal_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_snapshot_create",
                          "params": {"vg_name": "traid_vg",
                                     "source_path": "../escape",
                                     "dest_path": "snaps/s1"}})

def test_btrfs_subvol_set_default_valid():
    _, params = validate_request({"action": "btrfs_subvol_set_default",
                                  "params": {"vg_name": "traid_vg", "subvol_id": 256}})
    assert params["subvol_id"] == 256


# ---------------------------------------------------------------------------
# Btrfs scrub / balance / defrag / dedup
# ---------------------------------------------------------------------------

def test_btrfs_scrub_start():
    action, params = validate_request({"action": "btrfs_scrub_start",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_scrub_start"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_scrub_status():
    action, params = validate_request({"action": "btrfs_scrub_status",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_scrub_status"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_scrub_cancel():
    action, params = validate_request({"action": "btrfs_scrub_cancel",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_scrub_cancel"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_balance_start_no_filters():
    action, params = validate_request({"action": "btrfs_balance_start",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_balance_start"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_balance_start_with_filter():
    _, params = validate_request({"action": "btrfs_balance_start",
                                  "params": {"vg_name": "traid_vg",
                                             "usage_filter": 50, "metadata_usage": 80}})
    assert params["usage_filter"] == 50

def test_btrfs_balance_status():
    action, params = validate_request({"action": "btrfs_balance_status",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_balance_status"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_balance_cancel():
    action, params = validate_request({"action": "btrfs_balance_cancel",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_balance_cancel"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_defrag_defaults():
    action, params = validate_request({"action": "btrfs_defrag",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_defrag"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_defrag_with_path():
    _, params = validate_request({"action": "btrfs_defrag",
                                  "params": {"vg_name": "traid_vg", "path": "subvol1",
                                             "compression": "zstd", "recursive": False}})
    assert params["compression"] == "zstd"

def test_btrfs_defrag_traversal_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_defrag",
                          "params": {"vg_name": "traid_vg", "path": "../secret"}})

def test_btrfs_defrag_bad_compression_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_defrag",
                          "params": {"vg_name": "traid_vg", "compression": "bzip2"}})

def test_btrfs_dedup_valid():
    action, params = validate_request({"action": "btrfs_dedup",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_dedup"
    assert params["vg_name"] == "traid_vg"


# ---------------------------------------------------------------------------
# Btrfs quota / usage / send / receive
# ---------------------------------------------------------------------------

def test_btrfs_quota_enable():
    action, params = validate_request({"action": "btrfs_quota_enable",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_quota_enable"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_quota_list():
    action, params = validate_request({"action": "btrfs_quota_list",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_quota_list"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_quota_set_valid():
    _, params = validate_request({"action": "btrfs_quota_set",
                                  "params": {"vg_name": "traid_vg",
                                             "qgroup": "0/256", "limit_bytes": 1073741824}})
    assert params["qgroup"] == "0/256"
    assert params["limit_bytes"] == 1073741824

def test_btrfs_quota_set_invalid_qgroup():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_quota_set",
                          "params": {"vg_name": "traid_vg",
                                     "qgroup": "bad", "limit_bytes": 1024}})

def test_btrfs_quota_set_negative_limit_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_quota_set",
                          "params": {"vg_name": "traid_vg",
                                     "qgroup": "0/5", "limit_bytes": -1}})

def test_btrfs_usage_detail():
    action, params = validate_request({"action": "btrfs_usage_detail",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_usage_detail"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_send_valid():
    _, params = validate_request({"action": "btrfs_send",
                                  "params": {"vg_name": "traid_vg",
                                             "snapshot_path": "snaps/ro1",
                                             "dest_file": "backup.btrfs"}})
    assert params["dest_file"] == "backup.btrfs"
    assert params.get("parent_path") is None

def test_btrfs_send_with_parent():
    _, params = validate_request({"action": "btrfs_send",
                                  "params": {"vg_name": "traid_vg",
                                             "snapshot_path": "snaps/ro2",
                                             "dest_file": "incr.btrfs",
                                             "parent_path": "snaps/ro1"}})
    assert params["parent_path"] == "snaps/ro1"

def test_btrfs_send_bad_dest_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_send",
                          "params": {"vg_name": "traid_vg",
                                     "snapshot_path": "snaps/ro1",
                                     "dest_file": "backup.tar"}})

def test_btrfs_send_snapshot_traversal_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_send",
                          "params": {"vg_name": "traid_vg",
                                     "snapshot_path": "../etc",
                                     "dest_file": "out.btrfs"}})

def test_btrfs_receive_valid():
    _, params = validate_request({"action": "btrfs_receive",
                                  "params": {"vg_name": "traid_vg",
                                             "source_file": "backup.btrfs"}})
    assert params["source_file"] == "backup.btrfs"

def test_btrfs_receive_bad_file_rejected():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_receive",
                          "params": {"vg_name": "traid_vg",
                                     "source_file": "backup.zip"}})


# ---------------------------------------------------------------------------
# Btrfs scrub pause / resume / last_result
# ---------------------------------------------------------------------------

def test_btrfs_scrub_pause_valid():
    action, params = validate_request({"action": "btrfs_scrub_pause",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_scrub_pause"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_scrub_resume_valid():
    action, params = validate_request({"action": "btrfs_scrub_resume",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_scrub_resume"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_scrub_last_result_valid():
    action, params = validate_request({"action": "btrfs_scrub_last_result",
                                       "params": {"vg_name": "traid_vg"}})
    assert action == "btrfs_scrub_last_result"
    assert params["vg_name"] == "traid_vg"

def test_btrfs_scrub_pause_invalid_vg():
    with pytest.raises(ValidationError):
        validate_request({"action": "btrfs_scrub_pause",
                          "params": {"vg_name": "0bad"}})

def test_btrfs_scrub_resume_extra_param_rejected():
    with pytest.raises(ValidationError, match="unexpected param keys"):
        validate_request({"action": "btrfs_scrub_resume",
                          "params": {"vg_name": "traid_vg", "evil": "x"}})


# ---------------------------------------------------------------------------
# VM management
# ---------------------------------------------------------------------------

def test_vm_list_valid():
    action, params = validate_request({"action": "vm_list"})
    assert action == "vm_list"
    assert params == {}

def test_vm_list_isos_valid():
    action, _ = validate_request({"action": "vm_list_isos"})
    assert action == "vm_list_isos"

def test_vm_info_valid():
    action, params = validate_request({"action": "vm_info",
                                       "params": {"name": "debian12"}})
    assert action == "vm_info"
    assert params["name"] == "debian12"

def test_vm_info_invalid_name():
    with pytest.raises(ValidationError):
        validate_request({"action": "vm_info",
                          "params": {"name": "bad name!"}})

def test_vm_info_name_too_long():
    with pytest.raises(ValidationError):
        validate_request({"action": "vm_info",
                          "params": {"name": "a" * 65}})

def test_vm_action_start():
    action, params = validate_request({"action": "vm_action",
                                       "params": {"name": "vm1", "action": "start"}})
    assert action == "vm_action"
    assert params["action"] == "start"

def test_vm_action_shutdown():
    _, params = validate_request({"action": "vm_action",
                                  "params": {"name": "vm1", "action": "shutdown"}})
    assert params["action"] == "shutdown"

def test_vm_action_destroy():
    _, params = validate_request({"action": "vm_action",
                                  "params": {"name": "vm1", "action": "destroy"}})
    assert params["action"] == "destroy"

def test_vm_action_suspend():
    _, params = validate_request({"action": "vm_action",
                                  "params": {"name": "vm1", "action": "suspend"}})
    assert params["action"] == "suspend"

def test_vm_action_resume():
    _, params = validate_request({"action": "vm_action",
                                  "params": {"name": "vm1", "action": "resume"}})
    assert params["action"] == "resume"

def test_vm_action_invalid_action():
    with pytest.raises(ValidationError):
        validate_request({"action": "vm_action",
                          "params": {"name": "vm1", "action": "explode"}})

def test_vm_action_injection_in_name():
    with pytest.raises(ValidationError):
        validate_request({"action": "vm_action",
                          "params": {"name": "vm1; rm -rf /", "action": "start"}})

def test_vm_create_valid():
    action, params = validate_request({
        "action": "vm_create",
        "params": {"name": "myvm", "iso": "debian12.iso",
                   "ram_mb": 2048, "vcpus": 2, "disk_gb": 20},
    })
    assert action == "vm_create"
    assert params["name"] == "myvm"
    assert params["iso"] == "debian12.iso"
    assert params["ram_mb"] == 2048
    assert params["vcpus"] == 2
    assert params["disk_gb"] == 20

def test_vm_create_invalid_iso_no_extension():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "vm_create",
            "params": {"name": "vm", "iso": "debian12",
                       "ram_mb": 2048, "vcpus": 2, "disk_gb": 20},
        })

def test_vm_create_iso_path_traversal_rejected():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "vm_create",
            "params": {"name": "vm", "iso": "../etc/passwd.iso",
                       "ram_mb": 2048, "vcpus": 2, "disk_gb": 20},
        })

def test_vm_create_ram_too_low():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "vm_create",
            "params": {"name": "vm", "iso": "d.iso",
                       "ram_mb": 32, "vcpus": 2, "disk_gb": 20},
        })

def test_vm_create_vcpus_too_high():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "vm_create",
            "params": {"name": "vm", "iso": "d.iso",
                       "ram_mb": 1024, "vcpus": 65, "disk_gb": 20},
        })

def test_vm_delete_valid():
    action, params = validate_request({"action": "vm_delete",
                                       "params": {"name": "myvm"}})
    assert action == "vm_delete"
    assert params["name"] == "myvm"

def test_vm_delete_keep_storage_true():
    _, params = validate_request({"action": "vm_delete",
                                  "params": {"name": "vm", "keep_storage": True}})
    assert params["keep_storage"] is True

def test_vm_delete_keep_storage_false():
    _, params = validate_request({"action": "vm_delete",
                                  "params": {"name": "vm", "keep_storage": False}})
    assert params["keep_storage"] is False

def test_vm_list_extra_param_rejected():
    with pytest.raises(ValidationError, match="unexpected param keys"):
        validate_request({"action": "vm_list", "params": {"evil": "x"}})


# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

def test_docker_list_containers_no_params():
    action, params = validate_request({"action": "docker_list_containers"})
    assert action == "docker_list_containers"
    assert params == {}

def test_docker_list_containers_all_true():
    _, params = validate_request({"action": "docker_list_containers",
                                  "params": {"all": True}})
    assert params["all"] is True

def test_docker_list_containers_all_false():
    _, params = validate_request({"action": "docker_list_containers",
                                  "params": {"all": False}})
    assert params["all"] is False

def test_docker_container_action_start():
    action, params = validate_request({
        "action": "docker_container_action",
        "params": {"container_id": "abc1234567890abc", "action": "start"},
    })
    assert action == "docker_container_action"
    assert params["action"] == "start"

def test_docker_container_action_stop():
    _, params = validate_request({
        "action": "docker_container_action",
        "params": {"container_id": "abc1234567890abc", "action": "stop"},
    })
    assert params["action"] == "stop"

def test_docker_container_action_rm():
    _, params = validate_request({
        "action": "docker_container_action",
        "params": {"container_id": "abc1234567890abc", "action": "rm"},
    })
    assert params["action"] == "rm"

def test_docker_container_action_invalid():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "docker_container_action",
            "params": {"container_id": "abc123", "action": "delete"},
        })

def test_docker_container_action_injection():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "docker_container_action",
            "params": {"container_id": "abc; rm -rf /", "action": "stop"},
        })

def test_docker_container_logs_valid():
    action, params = validate_request({
        "action": "docker_container_logs",
        "params": {"container_id": "abc1234567890abc", "lines": 100},
    })
    assert action == "docker_container_logs"
    assert params["lines"] == 100

def test_docker_container_logs_lines_too_high():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "docker_container_logs",
            "params": {"container_id": "abc123", "lines": 99999},
        })

def test_docker_container_logs_lines_zero():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "docker_container_logs",
            "params": {"container_id": "abc123", "lines": 0},
        })

def test_docker_list_images_valid():
    action, _ = validate_request({"action": "docker_list_images"})
    assert action == "docker_list_images"

def test_docker_system_prune_valid():
    action, _ = validate_request({"action": "docker_system_prune"})
    assert action == "docker_system_prune"

def test_docker_pull_image_valid():
    action, params = validate_request({
        "action": "docker_pull_image",
        "params": {"image": "ubuntu:22.04"},
    })
    assert action == "docker_pull_image"
    assert params["image"] == "ubuntu:22.04"

def test_docker_pull_image_with_registry():
    _, params = validate_request({
        "action": "docker_pull_image",
        "params": {"image": "ghcr.io/user/myapp:latest"},
    })
    assert "ghcr.io" in params["image"]

def test_docker_pull_image_invalid():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "docker_pull_image",
            "params": {"image": "bad image name!"},
        })

def test_docker_remove_image_valid():
    action, params = validate_request({
        "action": "docker_remove_image",
        "params": {"image_id": "abc123def456abc1"},
    })
    assert action == "docker_remove_image"

def test_docker_remove_image_force():
    _, params = validate_request({
        "action": "docker_remove_image",
        "params": {"image_id": "abc123", "force": True},
    })
    assert params["force"] is True

def test_docker_remove_image_invalid_id():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "docker_remove_image",
            "params": {"image_id": "bad id!"},
        })


# ---------------------------------------------------------------------------
# Backup jobs
# ---------------------------------------------------------------------------

def test_backup_list_jobs_valid():
    action, _ = validate_request({"action": "backup_list_jobs"})
    assert action == "backup_list_jobs"

def test_backup_create_job_rsync_local():
    action, params = validate_request({
        "action": "backup_create_job",
        "params": {
            "name": "daily", "source_vg": "traid_vg",
            "dest_protocol": "rsync_local", "dest_path": "/mnt/backup",
            "interval_hours": 24,
        },
    })
    assert action == "backup_create_job"
    assert params["dest_protocol"] == "rsync_local"
    assert params["interval_hours"] == 24

def test_backup_create_job_nfs():
    _, params = validate_request({
        "action": "backup_create_job",
        "params": {
            "name": "weekly", "source_vg": "traid_vg",
            "dest_protocol": "nfs", "dest_path": "nas:/backup",
            "interval_hours": 168,
        },
    })
    assert params["dest_protocol"] == "nfs"

def test_backup_create_job_cifs():
    _, params = validate_request({
        "action": "backup_create_job",
        "params": {
            "name": "cifs_job", "source_vg": "vg",
            "dest_protocol": "cifs", "dest_path": "//nas/share",
            "interval_hours": 24,
            "dest_host": "nas.local",
            "dest_cifs_user": "admin", "dest_cifs_pass": "pass",
        },
    })
    assert params["dest_cifs_user"] == "admin"

def test_backup_create_job_btrfs_send():
    _, params = validate_request({
        "action": "backup_create_job",
        "params": {
            "name": "btrfs_job", "source_vg": "vg",
            "dest_protocol": "btrfs_send", "dest_path": "/mnt/streams",
            "interval_hours": 48,
        },
    })
    assert params["dest_protocol"] == "btrfs_send"

def test_backup_create_job_invalid_protocol():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "backup_create_job",
            "params": {
                "name": "j", "source_vg": "vg",
                "dest_protocol": "ftp", "dest_path": "/tmp",
                "interval_hours": 24,
            },
        })

def test_backup_create_job_interval_too_low():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "backup_create_job",
            "params": {
                "name": "j", "source_vg": "vg",
                "dest_protocol": "rsync_local", "dest_path": "/tmp",
                "interval_hours": 0,
            },
        })

def test_backup_create_job_interval_too_high():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "backup_create_job",
            "params": {
                "name": "j", "source_vg": "vg",
                "dest_protocol": "rsync_local", "dest_path": "/tmp",
                "interval_hours": 9999,
            },
        })

def test_backup_create_job_invalid_vg():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "backup_create_job",
            "params": {
                "name": "j", "source_vg": "0invalid",
                "dest_protocol": "rsync_local", "dest_path": "/tmp",
                "interval_hours": 24,
            },
        })

def test_backup_create_job_cifs_cred_injection():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "backup_create_job",
            "params": {
                "name": "j", "source_vg": "vg",
                "dest_protocol": "cifs", "dest_path": "//nas/share",
                "interval_hours": 24,
                "dest_cifs_user": "admin\nrm -rf /",
            },
        })

def test_backup_delete_job_valid_uuid():
    action, params = validate_request({
        "action": "backup_delete_job",
        "params": {"backup_id": "550e8400-e29b-41d4-a716-446655440000"},
    })
    assert action == "backup_delete_job"
    assert params["backup_id"] == "550e8400-e29b-41d4-a716-446655440000"

def test_backup_delete_job_invalid_uuid():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "backup_delete_job",
            "params": {"backup_id": "not-a-uuid"},
        })

def test_backup_run_now_valid():
    action, params = validate_request({
        "action": "backup_run_now",
        "params": {"backup_id": "550e8400-e29b-41d4-a716-446655440000"},
    })
    assert action == "backup_run_now"

def test_backup_job_history_valid():
    action, params = validate_request({
        "action": "backup_job_history",
        "params": {"backup_id": "550e8400-e29b-41d4-a716-446655440000"},
    })
    assert action == "backup_job_history"


# ---------------------------------------------------------------------------
# NFS sharing
# ---------------------------------------------------------------------------

def test_nfs_list_exports_valid():
    action, _ = validate_request({"action": "nfs_list_exports"})
    assert action == "nfs_list_exports"

def test_nfs_add_export_valid():
    action, params = validate_request({
        "action": "nfs_add_export",
        "params": {
            "path": "/srv/traid/data",
            "clients": "192.168.1.0/24",
            "options": "rw,sync,no_subtree_check",
        },
    })
    assert action == "nfs_add_export"
    assert params["path"] == "/srv/traid/data"
    assert params["clients"] == "192.168.1.0/24"

def test_nfs_add_export_mnt_path():
    _, params = validate_request({
        "action": "nfs_add_export",
        "params": {
            "path": "/mnt/traid/data",
            "clients": "*",
            "options": "ro",
        },
    })
    assert params["path"] == "/mnt/traid/data"

def test_nfs_add_export_varlib_path():
    _, params = validate_request({
        "action": "nfs_add_export",
        "params": {
            "path": "/var/lib/traid/data",
            "clients": "10.0.0.0/8",
            "options": "rw",
        },
    })
    assert params["path"] == "/var/lib/traid/data"

def test_nfs_add_export_invalid_path():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "nfs_add_export",
            "params": {
                "path": "/etc/passwd",
                "clients": "*",
                "options": "rw",
            },
        })

def test_nfs_add_export_path_traversal():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "nfs_add_export",
            "params": {
                "path": "/srv/traid/../etc",
                "clients": "*",
                "options": "rw",
            },
        })

def test_nfs_add_export_clients_injection():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "nfs_add_export",
            "params": {
                "path": "/srv/traid/data",
                "clients": "192.168.1.1; cat /etc/shadow",
                "options": "rw",
            },
        })

def test_nfs_add_export_options_injection():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "nfs_add_export",
            "params": {
                "path": "/srv/traid/data",
                "clients": "*",
                "options": "rw;rm -rf /",
            },
        })

def test_nfs_remove_export_valid():
    action, params = validate_request({
        "action": "nfs_remove_export",
        "params": {"path": "/srv/traid/data"},
    })
    assert action == "nfs_remove_export"
    assert params["path"] == "/srv/traid/data"

def test_nfs_remove_export_invalid_path():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "nfs_remove_export",
            "params": {"path": "/home/user/data"},
        })


# ---------------------------------------------------------------------------
# Samba sharing
# ---------------------------------------------------------------------------

def test_samba_list_shares_valid():
    action, _ = validate_request({"action": "samba_list_shares"})
    assert action == "samba_list_shares"

def test_samba_add_share_minimal():
    action, params = validate_request({
        "action": "samba_add_share",
        "params": {
            "name": "myshare",
            "path": "/srv/traid/data",
        },
    })
    assert action == "samba_add_share"
    assert params["name"] == "myshare"
    assert params["path"] == "/srv/traid/data"

def test_samba_add_share_full():
    _, params = validate_request({
        "action": "samba_add_share",
        "params": {
            "name": "pubdata",
            "path": "/mnt/traid/shared",
            "comment": "Public data share",
            "public": True,
            "writable": False,
        },
    })
    assert params["public"] is True
    assert params["writable"] is False
    assert params["comment"] == "Public data share"

def test_samba_add_share_invalid_name():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "samba_add_share",
            "params": {"name": "bad name!", "path": "/srv/traid/data"},
        })

def test_samba_add_share_name_too_long():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "samba_add_share",
            "params": {"name": "a" * 51, "path": "/srv/traid/data"},
        })

def test_samba_add_share_invalid_path():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "samba_add_share",
            "params": {"name": "myshare", "path": "/home/user/data"},
        })

def test_samba_add_share_comment_too_long():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "samba_add_share",
            "params": {
                "name": "myshare",
                "path": "/srv/traid/data",
                "comment": "x" * 201,
            },
        })

def test_samba_remove_share_valid():
    action, params = validate_request({
        "action": "samba_remove_share",
        "params": {"name": "myshare"},
    })
    assert action == "samba_remove_share"
    assert params["name"] == "myshare"

def test_samba_remove_share_invalid_name():
    with pytest.raises(ValidationError):
        validate_request({
            "action": "samba_remove_share",
            "params": {"name": "bad name!"},
        })
