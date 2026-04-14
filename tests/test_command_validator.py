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
