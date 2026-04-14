"""Unit tests for Pydantic v2 API request/response models."""

import pytest
from pydantic import ValidationError

from api.models import (
    ArrayCreationRequest,
    BadblocksRequest,
    BackupRequest,
    BtrfsBalanceRequest,
    BtrfsDefragRequest,
    BtrfsQuotaSetRequest,
    BtrfsReceiveRequest,
    BtrfsSendRequest,
    BtrfsSnapshotRequest,
    BtrfsSubvolCreateRequest,
    BtrfsSubvolDeleteRequest,
    CloneRequest,
    CompressionRequest,
    DiskInfo,
    EraseRequest,
    FormatRequest,
    GrowRequest,
    JobAccepted,
    MigrateRequest,
    RaidGroupPreview,
    CapacityPreview,
    ReplaceRequest,
    ShrinkRequest,
    SmartTestRequest,
    VolumeRenameRequest,
)


# ---------------------------------------------------------------------------
# ArrayCreationRequest
# ---------------------------------------------------------------------------

def test_array_creation_traid1():
    r = ArrayCreationRequest(disks=["/dev/sdb", "/dev/sdc"], type="traid1")
    assert r.type == "traid1"

def test_array_creation_traid2():
    r = ArrayCreationRequest(disks=["/dev/sdb", "/dev/sdc"], type="traid2")
    assert r.type == "traid2"

def test_array_creation_default_vg_name():
    r = ArrayCreationRequest(disks=["/dev/sdb", "/dev/sdc"], type="traid1")
    assert r.vg_name == "traid_vg"

def test_array_creation_custom_vg_name():
    r = ArrayCreationRequest(disks=["/dev/sdb", "/dev/sdc"], type="traid1", vg_name="myarray")
    assert r.vg_name == "myarray"

def test_array_creation_empty_disks_rejected():
    with pytest.raises(ValidationError):
        ArrayCreationRequest(disks=[], type="traid1")

def test_array_creation_invalid_type_rejected():
    with pytest.raises(ValidationError):
        ArrayCreationRequest(disks=["/dev/sdb", "/dev/sdc"], type="raid5")

def test_array_creation_invalid_vg_name_rejected():
    with pytest.raises(ValidationError):
        ArrayCreationRequest(disks=["/dev/sdb", "/dev/sdc"], type="traid1", vg_name="0invalid")

def test_array_creation_vg_name_with_slash_rejected():
    with pytest.raises(ValidationError):
        ArrayCreationRequest(disks=["/dev/sdb", "/dev/sdc"], type="traid1", vg_name="bad/name")

def test_array_creation_vg_name_with_dot_allowed():
    r = ArrayCreationRequest(disks=["/dev/sdb", "/dev/sdc"], type="traid1", vg_name="my.array")
    assert r.vg_name == "my.array"


# ---------------------------------------------------------------------------
# VolumeRenameRequest
# ---------------------------------------------------------------------------

def test_rename_valid():
    r = VolumeRenameRequest(new_name="myarray")
    assert r.new_name == "myarray"

def test_rename_with_underscore():
    r = VolumeRenameRequest(new_name="my_array")
    assert r.new_name == "my_array"

def test_rename_starts_with_digit_rejected():
    with pytest.raises(ValidationError):
        VolumeRenameRequest(new_name="0bad")

def test_rename_with_slash_rejected():
    with pytest.raises(ValidationError):
        VolumeRenameRequest(new_name="bad/name")

def test_rename_empty_rejected():
    with pytest.raises(ValidationError):
        VolumeRenameRequest(new_name="")


# ---------------------------------------------------------------------------
# MigrateRequest
# ---------------------------------------------------------------------------

def test_migrate_traid1_to_traid2():
    r = MigrateRequest(direction="traid1_to_traid2")
    assert r.direction == "traid1_to_traid2"
    assert r.new_disk is None

def test_migrate_traid2_to_traid1():
    r = MigrateRequest(direction="traid2_to_traid1")
    assert r.direction == "traid2_to_traid1"

def test_migrate_with_new_disk():
    r = MigrateRequest(direction="traid1_to_traid2", new_disk="/dev/sdd")
    assert r.new_disk == "/dev/sdd"

def test_migrate_with_nvme_disk():
    r = MigrateRequest(direction="traid1_to_traid2", new_disk="/dev/nvme0n1")
    assert r.new_disk == "/dev/nvme0n1"

def test_migrate_invalid_direction_rejected():
    with pytest.raises(ValidationError):
        MigrateRequest(direction="up")

def test_migrate_invalid_disk_rejected():
    with pytest.raises(ValidationError):
        MigrateRequest(direction="traid1_to_traid2", new_disk="sdd")

def test_migrate_disk_path_traversal_rejected():
    with pytest.raises(ValidationError):
        MigrateRequest(direction="traid1_to_traid2", new_disk="/dev/../etc/passwd")


# ---------------------------------------------------------------------------
# ReplaceRequest
# ---------------------------------------------------------------------------

def test_replace_valid():
    r = ReplaceRequest(old_disk="/dev/sdb", new_disk="/dev/sde")
    assert r.old_disk == "/dev/sdb"
    assert r.new_disk == "/dev/sde"

def test_replace_nvme_new_disk():
    r = ReplaceRequest(old_disk="/dev/sdb", new_disk="/dev/nvme0n1")
    assert r.new_disk == "/dev/nvme0n1"

def test_replace_invalid_old_disk():
    with pytest.raises(ValidationError):
        ReplaceRequest(old_disk="sdb", new_disk="/dev/sde")

def test_replace_invalid_new_disk():
    with pytest.raises(ValidationError):
        ReplaceRequest(old_disk="/dev/sdb", new_disk="/dev/sde && rm -rf /")


# ---------------------------------------------------------------------------
# GrowRequest / ShrinkRequest / CloneRequest
# ---------------------------------------------------------------------------

def test_grow_valid():
    r = GrowRequest(new_disk="/dev/sdf")
    assert r.new_disk == "/dev/sdf"

def test_grow_nvme():
    r = GrowRequest(new_disk="/dev/nvme0n1p1")
    assert r.new_disk == "/dev/nvme0n1p1"

def test_grow_invalid_rejected():
    with pytest.raises(ValidationError):
        GrowRequest(new_disk="sdf")

def test_shrink_valid():
    r = ShrinkRequest(disk_to_remove="/dev/sdb")
    assert r.disk_to_remove == "/dev/sdb"

def test_shrink_invalid_rejected():
    with pytest.raises(ValidationError):
        ShrinkRequest(disk_to_remove="/dev/md0")

def test_clone_valid():
    r = CloneRequest(target_disk="/dev/sdf")
    assert r.target_disk == "/dev/sdf"

def test_clone_virtio():
    r = CloneRequest(target_disk="/dev/vdf")
    assert r.target_disk == "/dev/vdf"


# ---------------------------------------------------------------------------
# BackupRequest
# ---------------------------------------------------------------------------

def test_backup_nfs_minimal():
    r = BackupRequest(protocol="nfs", host="192.168.1.10", remote_path="192.168.1.10:/backup")
    assert r.protocol == "nfs"
    assert r.cifs_user == ""
    assert r.cifs_pass == ""

def test_backup_cifs_with_creds():
    r = BackupRequest(
        protocol="cifs", host="nas.local",
        remote_path="//nas.local/backup",
        cifs_user="admin", cifs_pass="s3cret",
    )
    assert r.protocol == "cifs"
    assert r.cifs_user == "admin"
    assert r.cifs_pass == "s3cret"

def test_backup_invalid_protocol_rejected():
    with pytest.raises(ValidationError):
        BackupRequest(protocol="ftp", host="192.168.1.10", remote_path="192.168.1.10:/backup")

def test_backup_host_injection_rejected():
    with pytest.raises(ValidationError):
        BackupRequest(protocol="nfs", host="host; rm -rf /", remote_path="host:/path")

def test_backup_host_pipe_rejected():
    with pytest.raises(ValidationError):
        BackupRequest(protocol="nfs", host="host | cat /etc/passwd", remote_path="host:/path")

def test_backup_cred_newline_rejected():
    with pytest.raises(ValidationError):
        BackupRequest(
            protocol="cifs", host="nas", remote_path="//nas/share",
            cifs_user="admin\nmalicious", cifs_pass="",
        )

def test_backup_remote_path_no_separator_rejected():
    with pytest.raises(ValidationError):
        BackupRequest(protocol="nfs", host="192.168.1.10", remote_path="nodividerhere")

def test_backup_hostname_as_host():
    r = BackupRequest(protocol="nfs", host="nas.local", remote_path="nas.local:/backup")
    assert r.host == "nas.local"


# ---------------------------------------------------------------------------
# SmartTestRequest
# ---------------------------------------------------------------------------

def test_smart_short():
    r = SmartTestRequest(disk="/dev/sdb", test_type="short")
    assert r.test_type == "short"

def test_smart_long():
    r = SmartTestRequest(disk="/dev/nvme0n1", test_type="long")
    assert r.test_type == "long"

def test_smart_invalid_type_rejected():
    with pytest.raises(ValidationError):
        SmartTestRequest(disk="/dev/sdb", test_type="full")

def test_smart_invalid_disk_rejected():
    with pytest.raises(ValidationError):
        SmartTestRequest(disk="sdb", test_type="short")

def test_smart_proc_path_rejected():
    with pytest.raises(ValidationError):
        SmartTestRequest(disk="/proc/sdb", test_type="short")


# ---------------------------------------------------------------------------
# BadblocksRequest
# ---------------------------------------------------------------------------

def test_badblocks_valid():
    r = BadblocksRequest(disk="/dev/sdb")
    assert r.disk == "/dev/sdb"

def test_badblocks_virtio():
    r = BadblocksRequest(disk="/dev/vdb")
    assert r.disk == "/dev/vdb"

def test_badblocks_invalid_rejected():
    with pytest.raises(ValidationError):
        BadblocksRequest(disk="/etc/passwd")


# ---------------------------------------------------------------------------
# EraseRequest
# ---------------------------------------------------------------------------

def test_erase_default_mode():
    r = EraseRequest(disk="/dev/sdb")
    assert r.mode == "dod_short"

def test_erase_quick_mode():
    r = EraseRequest(disk="/dev/sdb", mode="quick")
    assert r.mode == "quick"

def test_erase_dod_short():
    r = EraseRequest(disk="/dev/sdb", mode="dod_short")
    assert r.mode == "dod_short"

def test_erase_dod_7():
    r = EraseRequest(disk="/dev/sdb", mode="dod_7")
    assert r.mode == "dod_7"

def test_erase_invalid_mode_rejected():
    with pytest.raises(ValidationError):
        EraseRequest(disk="/dev/sdb", mode="gutmann")

def test_erase_invalid_disk_rejected():
    with pytest.raises(ValidationError):
        EraseRequest(disk="sdb")

def test_erase_dev_null_rejected():
    with pytest.raises(ValidationError):
        EraseRequest(disk="/dev/null")


# ---------------------------------------------------------------------------
# DiskInfo
# ---------------------------------------------------------------------------

def test_diskinfo_available():
    d = DiskInfo(name="/dev/sdb", size=2000000000, type="disk",
                 mountpoint=None, fstype=None, available=True)
    assert d.available is True

def test_diskinfo_in_use():
    d = DiskInfo(name="/dev/sda", size=500000000000, type="disk",
                 mountpoint=None, fstype="ext4", available=False)
    assert d.available is False

def test_diskinfo_used_by_optional():
    d = DiskInfo(name="/dev/sdb", size=1000000000, type="disk",
                 mountpoint=None, fstype=None, available=False, used_by="traid_vg")
    assert d.used_by == "traid_vg"


# ---------------------------------------------------------------------------
# JobAccepted
# ---------------------------------------------------------------------------

def test_job_accepted():
    j = JobAccepted(accepted=True, job_id="abc-123")
    assert j.accepted is True
    assert j.job_id == "abc-123"

def test_job_rejected():
    j = JobAccepted(accepted=False, reason="ARRAY_BUSY", message="busy")
    assert j.accepted is False
    assert j.reason == "ARRAY_BUSY"

def test_job_accepted_all_optional_none():
    j = JobAccepted(accepted=True)
    assert j.job_id is None
    assert j.reason is None
    assert j.message is None


# ---------------------------------------------------------------------------
# CapacityPreview / RaidGroupPreview
# ---------------------------------------------------------------------------

def test_capacity_preview_valid():
    rg = RaidGroupPreview(tier=0, level="raid5", members=3, stripe_size_bytes=2000000000000, usable_bytes=4000000000000)
    cp = CapacityPreview(usable_bytes=4000000000000, redundancy_disks=1, raid_groups=[rg])
    assert cp.usable_bytes == 4000000000000
    assert len(cp.raid_groups) == 1

def test_capacity_preview_empty_groups():
    cp = CapacityPreview(usable_bytes=0, redundancy_disks=2, raid_groups=[])
    assert cp.raid_groups == []


# ---------------------------------------------------------------------------
# FormatRequest
# ---------------------------------------------------------------------------

def test_format_ext4():
    r = FormatRequest(fstype="ext4")
    assert r.fstype == "ext4"
    assert r.label == ""
    assert r.compression == ""

def test_format_btrfs_with_label():
    r = FormatRequest(fstype="btrfs", label="data", compression="zstd")
    assert r.fstype == "btrfs"
    assert r.label == "data"
    assert r.compression == "zstd"

def test_format_btrfs_lzo():
    r = FormatRequest(fstype="btrfs", compression="lzo")
    assert r.compression == "lzo"

def test_format_btrfs_zlib():
    r = FormatRequest(fstype="btrfs", compression="zlib")
    assert r.compression == "zlib"

def test_format_btrfs_none_compression():
    r = FormatRequest(fstype="btrfs", compression="none")
    assert r.compression == "none"

def test_format_invalid_fstype_rejected():
    with pytest.raises(ValidationError):
        FormatRequest(fstype="xfs")

def test_format_invalid_compression_rejected():
    with pytest.raises(ValidationError):
        FormatRequest(fstype="btrfs", compression="gzip")


# ---------------------------------------------------------------------------
# BtrfsSubvolCreateRequest
# ---------------------------------------------------------------------------

def test_subvol_create_simple():
    r = BtrfsSubvolCreateRequest(name="snapshots")
    assert r.name == "snapshots"

def test_subvol_create_nested():
    r = BtrfsSubvolCreateRequest(name="data/subvol1")
    assert r.name == "data/subvol1"

def test_subvol_create_traversal_rejected():
    with pytest.raises(ValidationError):
        BtrfsSubvolCreateRequest(name="../etc")

def test_subvol_create_empty_rejected():
    with pytest.raises(ValidationError):
        BtrfsSubvolCreateRequest(name="")


# ---------------------------------------------------------------------------
# BtrfsSubvolDeleteRequest
# ---------------------------------------------------------------------------

def test_subvol_delete_simple():
    r = BtrfsSubvolDeleteRequest(path="snapshots/snap1")
    assert r.recursive is False

def test_subvol_delete_recursive():
    r = BtrfsSubvolDeleteRequest(path="snapshots", recursive=True)
    assert r.recursive is True

def test_subvol_delete_traversal_rejected():
    with pytest.raises(ValidationError):
        BtrfsSubvolDeleteRequest(path="../secret")


# ---------------------------------------------------------------------------
# BtrfsSnapshotRequest
# ---------------------------------------------------------------------------

def test_snapshot_create():
    r = BtrfsSnapshotRequest(source_path="data", dest_path="snapshots/snap1")
    assert r.readonly is False

def test_snapshot_create_readonly():
    r = BtrfsSnapshotRequest(source_path="data", dest_path="snaps/ro", readonly=True)
    assert r.readonly is True

def test_snapshot_traversal_rejected():
    with pytest.raises(ValidationError):
        BtrfsSnapshotRequest(source_path="../etc", dest_path="snaps/s1")

def test_snapshot_dest_traversal_rejected():
    with pytest.raises(ValidationError):
        BtrfsSnapshotRequest(source_path="data", dest_path="../escape")


# ---------------------------------------------------------------------------
# BtrfsBalanceRequest
# ---------------------------------------------------------------------------

def test_balance_no_filters():
    r = BtrfsBalanceRequest()
    assert r.usage_filter is None
    assert r.metadata_usage is None

def test_balance_with_usage_filter():
    r = BtrfsBalanceRequest(usage_filter=50)
    assert r.usage_filter == 50

def test_balance_with_both_filters():
    r = BtrfsBalanceRequest(usage_filter=75, metadata_usage=80)
    assert r.usage_filter == 75
    assert r.metadata_usage == 80


# ---------------------------------------------------------------------------
# BtrfsDefragRequest
# ---------------------------------------------------------------------------

def test_defrag_defaults():
    r = BtrfsDefragRequest()
    assert r.path == ""
    assert r.recursive is True
    assert r.compression == ""

def test_defrag_with_path_and_compression():
    r = BtrfsDefragRequest(path="subvol1", recursive=False, compression="zstd")
    assert r.path == "subvol1"
    assert r.compression == "zstd"

def test_defrag_invalid_compression():
    with pytest.raises(ValidationError):
        BtrfsDefragRequest(compression="bzip2")

def test_defrag_traversal_rejected():
    with pytest.raises(ValidationError):
        BtrfsDefragRequest(path="../etc")


# ---------------------------------------------------------------------------
# BtrfsQuotaSetRequest
# ---------------------------------------------------------------------------

def test_quota_set_valid():
    r = BtrfsQuotaSetRequest(qgroup="0/5", limit_bytes=1073741824)
    assert r.qgroup == "0/5"
    assert r.limit_bytes == 1073741824

def test_quota_set_invalid_qgroup():
    with pytest.raises(ValidationError):
        BtrfsQuotaSetRequest(qgroup="bad", limit_bytes=1024)

def test_quota_set_negative_limit():
    with pytest.raises(ValidationError):
        BtrfsQuotaSetRequest(qgroup="0/5", limit_bytes=-1)

def test_quota_set_zero_limit_allowed():
    r = BtrfsQuotaSetRequest(qgroup="1/256", limit_bytes=0)
    assert r.limit_bytes == 0


# ---------------------------------------------------------------------------
# BtrfsSendRequest
# ---------------------------------------------------------------------------

def test_send_valid():
    r = BtrfsSendRequest(snapshot_path="snaps/ro1", dest_file="backup.btrfs")
    assert r.parent_path is None

def test_send_with_parent():
    r = BtrfsSendRequest(snapshot_path="snaps/ro2", dest_file="incr.btrfs",
                         parent_path="snaps/ro1")
    assert r.parent_path == "snaps/ro1"

def test_send_dest_file_must_end_btrfs():
    with pytest.raises(ValidationError):
        BtrfsSendRequest(snapshot_path="snaps/ro1", dest_file="backup.tar")

def test_send_snapshot_traversal_rejected():
    with pytest.raises(ValidationError):
        BtrfsSendRequest(snapshot_path="../secret", dest_file="out.btrfs")

def test_send_parent_traversal_rejected():
    with pytest.raises(ValidationError):
        BtrfsSendRequest(snapshot_path="snaps/ro1", dest_file="out.btrfs",
                         parent_path="../escape")


# ---------------------------------------------------------------------------
# BtrfsReceiveRequest
# ---------------------------------------------------------------------------

def test_receive_valid():
    r = BtrfsReceiveRequest(source_file="backup.btrfs")
    assert r.source_file == "backup.btrfs"

def test_receive_must_end_btrfs():
    with pytest.raises(ValidationError):
        BtrfsReceiveRequest(source_file="backup.zip")

def test_receive_no_path_separators():
    with pytest.raises(ValidationError):
        BtrfsReceiveRequest(source_file="path/to/backup.btrfs")


# ---------------------------------------------------------------------------
# CompressionRequest
# ---------------------------------------------------------------------------

def test_compression_zstd():
    r = CompressionRequest(compression="zstd")
    assert r.compression == "zstd"

def test_compression_none():
    r = CompressionRequest(compression="none")
    assert r.compression == "none"

def test_compression_empty_string():
    r = CompressionRequest(compression="")
    assert r.compression == ""

def test_compression_invalid():
    with pytest.raises(ValidationError):
        CompressionRequest(compression="gzip")
