"""
Unit tests for pure utility functions in disk_ops.py.
No subprocess calls, no hardware, no root required.
"""

import pytest
from worker.disk_ops import (
    MiB,
    _part_path,
    _base_disk,
    _is_os_disk,
    _has_content,
)


# ---------------------------------------------------------------------------
# MiB constant
# ---------------------------------------------------------------------------

def test_mib_value():
    assert MiB == 1024 * 1024


# ---------------------------------------------------------------------------
# _part_path — derive partition path from disk + number
# ---------------------------------------------------------------------------

def test_part_path_sata_1():
    assert _part_path("/dev/sdb", 1) == "/dev/sdb1"

def test_part_path_sata_3():
    assert _part_path("/dev/sdc", 3) == "/dev/sdc3"

def test_part_path_virtio_1():
    assert _part_path("/dev/vdb", 1) == "/dev/vdb1"

def test_part_path_virtio_2():
    assert _part_path("/dev/vdc", 2) == "/dev/vdc2"

def test_part_path_nvme_1():
    assert _part_path("/dev/nvme0n1", 1) == "/dev/nvme0n1p1"

def test_part_path_nvme_2():
    assert _part_path("/dev/nvme0n1", 2) == "/dev/nvme0n1p2"

def test_part_path_nvme_second_controller():
    assert _part_path("/dev/nvme1n1", 1) == "/dev/nvme1n1p1"

def test_part_path_nvme_second_namespace():
    assert _part_path("/dev/nvme0n2", 1) == "/dev/nvme0n2p1"

def test_part_path_xvd():
    # Xen virtual disk — ends in a letter, no 'p' separator needed
    assert _part_path("/dev/xvdb", 1) == "/dev/xvdb1"


# ---------------------------------------------------------------------------
# _base_disk — derive whole-disk path from partition path
# ---------------------------------------------------------------------------

def test_base_disk_sata_partition_1():
    assert _base_disk("/dev/sdb1") == "/dev/sdb"

def test_base_disk_sata_partition_3():
    assert _base_disk("/dev/sdc3") == "/dev/sdc"

def test_base_disk_virtio_partition():
    assert _base_disk("/dev/vdb1") == "/dev/vdb"

def test_base_disk_nvme_partition_p1():
    assert _base_disk("/dev/nvme0n1p1") == "/dev/nvme0n1"

def test_base_disk_nvme_partition_p2():
    assert _base_disk("/dev/nvme0n1p2") == "/dev/nvme0n1"

def test_base_disk_nvme_second_controller():
    assert _base_disk("/dev/nvme1n1p1") == "/dev/nvme1n1"

def test_base_disk_whole_disk_unchanged():
    # Passing a whole-disk path should return itself
    assert _base_disk("/dev/sdb") == "/dev/sdb"


# ---------------------------------------------------------------------------
# _is_os_disk — detect OS disk by child mountpoints
# ---------------------------------------------------------------------------

def test_is_os_disk_root_mount():
    blockdev = {"children": [{"mountpoint": "/", "children": None}]}
    assert _is_os_disk(blockdev) is True

def test_is_os_disk_boot_mount():
    blockdev = {"children": [{"mountpoint": "/boot", "children": None}]}
    assert _is_os_disk(blockdev) is True

def test_is_os_disk_boot_efi_mount():
    blockdev = {"children": [{"mountpoint": "/boot/efi", "children": None}]}
    assert _is_os_disk(blockdev) is True

def test_is_os_disk_efi_mount():
    blockdev = {"children": [{"mountpoint": "/efi", "children": None}]}
    assert _is_os_disk(blockdev) is True

def test_is_os_disk_data_mount_not_os():
    blockdev = {"children": [{"mountpoint": "/data", "children": None}]}
    assert _is_os_disk(blockdev) is False

def test_is_os_disk_no_children():
    blockdev = {"children": None}
    assert _is_os_disk(blockdev) is False

def test_is_os_disk_empty_children():
    blockdev = {"children": []}
    assert _is_os_disk(blockdev) is False

def test_is_os_disk_child_with_no_mountpoint():
    blockdev = {"children": [{"mountpoint": None, "children": None}]}
    assert _is_os_disk(blockdev) is False

def test_is_os_disk_nested_root():
    """OS partition nested inside an LVM or md layer."""
    blockdev = {
        "children": [{
            "mountpoint": None,
            "children": [{"mountpoint": "/", "children": None}]
        }]
    }
    assert _is_os_disk(blockdev) is True

def test_is_os_disk_typical_efi_plus_root():
    """Typical EFI system: sda1=/boot/efi, sda2=/."""
    blockdev = {
        "children": [
            {"mountpoint": "/boot/efi", "children": None},
            {"mountpoint": "/", "children": None},
        ]
    }
    assert _is_os_disk(blockdev) is True

def test_is_os_disk_two_data_partitions_not_os():
    blockdev = {
        "children": [
            {"mountpoint": "/data1", "children": None},
            {"mountpoint": "/data2", "children": None},
        ]
    }
    assert _is_os_disk(blockdev) is False


# ---------------------------------------------------------------------------
# _has_content — detect if disk has any recognised filesystem / label
# ---------------------------------------------------------------------------

def test_has_content_linux_raid_member():
    blockdev = {"children": [{"fstype": "linux_raid_member", "mountpoint": None, "children": None}]}
    assert _has_content(blockdev) is True

def test_has_content_lvm2_member():
    blockdev = {"children": [{"fstype": "LVM2_member", "mountpoint": None, "children": None}]}
    assert _has_content(blockdev) is True

def test_has_content_ext4():
    blockdev = {"children": [{"fstype": "ext4", "mountpoint": None, "children": None}]}
    assert _has_content(blockdev) is True

def test_has_content_with_mountpoint():
    blockdev = {"children": [{"fstype": None, "mountpoint": "/data", "children": None}]}
    assert _has_content(blockdev) is True

def test_has_content_empty_partition():
    blockdev = {"children": [{"fstype": None, "mountpoint": None, "children": None}]}
    assert _has_content(blockdev) is False

def test_has_content_no_children():
    blockdev = {"children": None}
    assert _has_content(blockdev) is False

def test_has_content_empty_children():
    blockdev = {"children": []}
    assert _has_content(blockdev) is False

def test_has_content_nested():
    """Content in a nested layer (e.g. md device inside partition) counts."""
    blockdev = {
        "children": [{
            "fstype": None,
            "mountpoint": None,
            "children": [{"fstype": "ext4", "mountpoint": "/", "children": None}]
        }]
    }
    assert _has_content(blockdev) is True

def test_has_content_multiple_partitions_one_with_content():
    blockdev = {
        "children": [
            {"fstype": None, "mountpoint": None, "children": None},
            {"fstype": "ext4", "mountpoint": None, "children": None},
        ]
    }
    assert _has_content(blockdev) is True

def test_has_content_multiple_empty_partitions():
    blockdev = {
        "children": [
            {"fstype": None, "mountpoint": None, "children": None},
            {"fstype": None, "mountpoint": None, "children": None},
        ]
    }
    assert _has_content(blockdev) is False
