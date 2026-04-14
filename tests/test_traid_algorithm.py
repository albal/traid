"""Unit tests for the TRAID tier-slicing algorithm. No hardware required."""

import pytest
from worker.traid_algorithm import (
    MiB,
    ALIGNMENT,
    calculate_traid,
    capacity_preview,
    generate_parted_commands,
    generate_mdadm_commands,
    generate_lvm_commands,
    _align_up,
    _align_down,
)

TB = 1024 ** 4
GB = 1024 ** 3


# ---------------------------------------------------------------------------
# Alignment helpers
# ---------------------------------------------------------------------------

def test_align_up_zero():
    assert _align_up(0) == 0

def test_align_up_one_byte():
    assert _align_up(1) == MiB

def test_align_up_exact():
    assert _align_up(MiB) == MiB

def test_align_up_one_over():
    assert _align_up(MiB + 1) == 2 * MiB

def test_align_up_large():
    assert _align_up(10 * TB) == 10 * TB  # TB is divisible by MiB

def test_align_down_zero():
    assert _align_down(0) == 0

def test_align_down_below_mib():
    assert _align_down(MiB - 1) == 0

def test_align_down_exact():
    assert _align_down(MiB) == MiB

def test_align_down_one_over():
    assert _align_down(MiB + 512) == MiB

def test_align_down_large():
    assert _align_down(10 * TB + 512) == 10 * TB


# ---------------------------------------------------------------------------
# Invalid input
# ---------------------------------------------------------------------------

def test_too_few_drives_traid1_one_disk():
    with pytest.raises(ValueError, match="at least 2"):
        calculate_traid([1 * TB], redundancy=1)

def test_too_few_drives_traid1_empty():
    with pytest.raises(ValueError):
        calculate_traid([], redundancy=1)

def test_too_few_drives_traid2_one_disk():
    with pytest.raises(ValueError):
        calculate_traid([1 * TB], redundancy=2)

def test_too_few_drives_traid2_two_disks():
    with pytest.raises(ValueError, match="at least 3"):
        calculate_traid([1 * TB, 2 * TB], redundancy=2)

def test_invalid_redundancy():
    with pytest.raises(ValueError):
        calculate_traid([1 * TB, 2 * TB], redundancy=3)

def test_negative_redundancy():
    with pytest.raises(ValueError):
        calculate_traid([1 * TB, 2 * TB], redundancy=0)


# ---------------------------------------------------------------------------
# Two equal drives — TRAID-1 → single RAID 1
# ---------------------------------------------------------------------------

def test_two_equal_drives_traid1():
    sizes = [2 * TB, 2 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    assert len(plan.raid_groups) == 1
    rg = plan.raid_groups[0]
    assert rg.level == 1
    assert rg.usable_bytes == rg.stripe_size_bytes
    assert plan.total_usable_bytes == rg.usable_bytes

def test_two_equal_drives_traid1_no_waste():
    """Two equal drives produce exactly one tier consuming all space."""
    sizes = [2 * TB, 2 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    assert len(plan.raid_groups) == 1

def test_two_unequal_drives_traid1():
    """Larger drive gets one tier (RAID 1), remaining space on big drive is wasted."""
    sizes = [1 * TB, 2 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    # Only one tier possible: 2 drives × 1 TB → RAID 1 → 1 TB
    assert len(plan.raid_groups) == 1
    assert plan.raid_groups[0].level == 1
    assert plan.total_usable_bytes == pytest.approx(1 * TB, rel=0.001)


# ---------------------------------------------------------------------------
# Three equal drives — TRAID-1 → single RAID 5
# ---------------------------------------------------------------------------

def test_three_equal_drives_traid1():
    sizes = [4 * TB, 4 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    assert len(plan.raid_groups) == 1
    rg = plan.raid_groups[0]
    assert rg.level == 5
    assert rg.usable_bytes == 2 * rg.stripe_size_bytes

def test_three_equal_drives_traid1_usable():
    sizes = [3 * TB, 3 * TB, 3 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    assert plan.total_usable_bytes == pytest.approx(6 * TB, rel=0.001)


# ---------------------------------------------------------------------------
# Reference example: 3×4 TB + 1×2 TB → 10 TB usable (TRAID-1)
# ---------------------------------------------------------------------------

def test_reference_example_traid1():
    sizes = [4 * TB, 4 * TB, 4 * TB, 2 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    assert len(plan.raid_groups) == 2

    rg0 = plan.raid_groups[0]
    assert rg0.level == 5
    assert rg0.usable_bytes == pytest.approx(6 * TB, rel=0.001)

    rg1 = plan.raid_groups[1]
    assert rg1.level == 5
    assert rg1.usable_bytes == pytest.approx(4 * TB, rel=0.001)

    assert plan.total_usable_bytes == pytest.approx(10 * TB, rel=0.001)

def test_reference_example_better_than_naive_raid5():
    """TRAID should always yield >= naive RAID 5 at minimum size."""
    sizes = [4 * TB, 4 * TB, 4 * TB, 2 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    # Naive RAID 5 on 4 drives at min size (2 TB) = 3 × 2 TB = 6 TB
    naive = 3 * 2 * TB
    assert plan.total_usable_bytes > naive


# ---------------------------------------------------------------------------
# Mismatched drives — cascading tiers
# ---------------------------------------------------------------------------

def test_mismatched_drives_cascade():
    sizes = [1 * TB, 2 * TB, 3 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    # Tier 0: 4 × 1 TB → RAID 5 → 3 TB
    # Tier 1: 3 × 1 TB → RAID 5 → 2 TB
    # Tier 2: 2 × 1 TB → RAID 1 → 1 TB
    assert len(plan.raid_groups) == 3
    assert plan.raid_groups[0].level == 5
    assert plan.raid_groups[1].level == 5
    assert plan.raid_groups[2].level == 1
    assert plan.total_usable_bytes == pytest.approx(6 * TB, rel=0.001)

def test_five_drives_all_different():
    sizes = [1 * TB, 2 * TB, 3 * TB, 4 * TB, 5 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    # Tier 0: 5 × 1 TB → RAID 5 → 4 TB
    # Tier 1: 4 × 1 TB → RAID 5 → 3 TB
    # Tier 2: 3 × 1 TB → RAID 5 → 2 TB
    # Tier 3: 2 × 1 TB → RAID 1 → 1 TB
    # Total: 10 TB
    assert len(plan.raid_groups) == 4
    assert plan.total_usable_bytes == pytest.approx(10 * TB, rel=0.001)


# ---------------------------------------------------------------------------
# TRAID-2
# ---------------------------------------------------------------------------

def test_four_equal_drives_traid2():
    sizes = [4 * TB, 4 * TB, 4 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=2)
    assert len(plan.raid_groups) == 1
    rg = plan.raid_groups[0]
    assert rg.level == 6
    assert rg.usable_bytes == 2 * rg.stripe_size_bytes

def test_four_drives_traid2_usable():
    sizes = [4 * TB, 4 * TB, 4 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=2)
    assert plan.total_usable_bytes == pytest.approx(8 * TB, rel=0.001)

def test_three_drives_traid2_no_usable_tiers():
    """TRAID-2 requires 4+ drives per tier (RAID-6 needs 4); 3 drives → no usable tiers."""
    sizes = [4 * TB, 4 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=2)
    assert plan.total_usable_bytes == 0
    assert len(plan.raid_groups) == 0

def test_five_drives_traid2_two_tiers():
    sizes = [2 * TB, 4 * TB, 4 * TB, 4 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=2)
    # Tier 0: 5 × 2 TB → RAID 6 → 6 TB
    # Tier 1: 4 × 2 TB → RAID 6 → 4 TB
    # Total: 10 TB
    assert len(plan.raid_groups) == 2
    assert plan.total_usable_bytes == pytest.approx(10 * TB, rel=0.001)


# ---------------------------------------------------------------------------
# Alignment: all partition boundaries must be MiB-aligned
# ---------------------------------------------------------------------------

def test_partition_alignment_reference():
    sizes = [4 * TB, 4 * TB, 4 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    for disk_idx, parts in plan.partitions.items():
        for p in parts:
            assert p.start_bytes % ALIGNMENT == 0, (
                f"disk {disk_idx} tier {p.tier} start {p.start_bytes} not aligned"
            )
            assert p.end_bytes % ALIGNMENT == 0, (
                f"disk {disk_idx} tier {p.tier} end {p.end_bytes} not aligned"
            )

def test_partition_alignment_mismatched():
    sizes = [1 * TB, 2 * TB, 3 * TB]
    plan = calculate_traid(sizes)
    for disk_idx, parts in plan.partitions.items():
        for p in parts:
            assert p.start_bytes % ALIGNMENT == 0
            assert p.end_bytes % ALIGNMENT == 0


# ---------------------------------------------------------------------------
# Partition continuity — no gaps or overlaps within a single disk
# ---------------------------------------------------------------------------

def test_partition_continuity():
    sizes = [1 * TB, 2 * TB, 3 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    for disk_idx, parts in plan.partitions.items():
        sorted_parts = sorted(parts, key=lambda p: p.start_bytes)
        for i in range(1, len(sorted_parts)):
            assert sorted_parts[i].start_bytes == sorted_parts[i - 1].end_bytes, (
                f"gap or overlap at disk {disk_idx} between tier "
                f"{sorted_parts[i-1].tier} and {sorted_parts[i].tier}"
            )

def test_partition_starts_after_gpt_header():
    """First partition must start at >= 1 MiB to leave room for GPT primary header."""
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    for disk_idx, parts in plan.partitions.items():
        first = min(parts, key=lambda p: p.start_bytes)
        assert first.start_bytes >= ALIGNMENT, (
            f"disk {disk_idx} partition starts at {first.start_bytes}, before 1 MiB"
        )

def test_partition_ends_before_disk_end():
    """Partitions must not overrun the disk (GPT backup header at last MiB)."""
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    for disk_idx, parts in plan.partitions.items():
        disk_size = sizes[disk_idx]
        last = max(parts, key=lambda p: p.end_bytes)
        assert last.end_bytes <= disk_size - ALIGNMENT, (
            f"disk {disk_idx} partition overruns disk end"
        )


# ---------------------------------------------------------------------------
# Stripe sizes consistent across drives in the same tier
# ---------------------------------------------------------------------------

def test_stripe_sizes_consistent():
    sizes = [1 * TB, 2 * TB, 3 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    for rg in plan.raid_groups:
        for p in rg.partitions:
            actual = p.end_bytes - p.start_bytes
            assert actual == rg.stripe_size_bytes, (
                f"tier {rg.tier} partition size {actual} != stripe_size {rg.stripe_size_bytes}"
            )


# ---------------------------------------------------------------------------
# All drives have partitions
# ---------------------------------------------------------------------------

def test_all_drives_have_partitions():
    sizes = [2 * TB, 3 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    assert set(plan.partitions.keys()) == {0, 1, 2}
    for disk_idx in range(3):
        assert len(plan.partitions[disk_idx]) > 0

def test_smallest_drive_has_fewest_partitions():
    sizes = [1 * TB, 2 * TB, 3 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    # Drive 0 (1 TB) has 1 partition; drive 3 (4 TB) has most
    assert len(plan.partitions[0]) == 1
    assert len(plan.partitions[3]) >= len(plan.partitions[0])


# ---------------------------------------------------------------------------
# Parted command generation
# ---------------------------------------------------------------------------

def test_parted_commands_first_is_mklabel():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_parted_commands("/dev/sdb", 0, plan)
    assert cmds[0] == ["parted", "-s", "/dev/sdb", "mklabel", "gpt"]

def test_parted_commands_mkpart_entries():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_parted_commands("/dev/sdb", 0, plan)
    mkpart_cmds = [c for c in cmds[1:] if "mkpart" in c]
    assert len(mkpart_cmds) == len(plan.partitions[0])
    for cmd in mkpart_cmds:
        assert cmd[0] == "parted"
        assert "mkpart" in cmd

def test_parted_mib_boundaries():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_parted_commands("/dev/sdb", 0, plan)
    for cmd in cmds[1:]:
        start_str = cmd[-2]
        end_str = cmd[-1]
        assert start_str.endswith("MiB"), f"start {start_str!r} not MiB"
        assert end_str.endswith("MiB"), f"end {end_str!r} not MiB"
        assert int(start_str[:-3]) >= 0
        assert int(end_str[:-3]) > int(start_str[:-3])

def test_parted_correct_disk_used():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    for i in range(3):
        disk = f"/dev/sd{chr(ord('b') + i)}"
        cmds = generate_parted_commands(disk, i, plan)
        for cmd in cmds:
            assert disk in cmd

def test_parted_tier_names():
    sizes = [2 * TB, 3 * TB, 4 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_parted_commands("/dev/sdb", 0, plan)
    partition_names = [cmd[cmd.index("mkpart") + 1] for cmd in cmds if "mkpart" in cmd]
    for name in partition_names:
        assert "traid-tier" in name


# ---------------------------------------------------------------------------
# mdadm command generation
# ---------------------------------------------------------------------------

def test_mdadm_commands_present():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    disk_paths = ["/dev/sdb", "/dev/sdc", "/dev/sdd"]
    cmds = generate_mdadm_commands(plan, disk_paths)
    executables = {c[0] for c in cmds}
    assert "mdadm" in executables

def test_mdadm_creates_correct_number_of_arrays():
    sizes = [1 * TB, 2 * TB, 3 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    n_tiers = len(plan.raid_groups)
    disk_paths = ["/dev/sdb", "/dev/sdc", "/dev/sdd"]
    cmds = generate_mdadm_commands(plan, disk_paths)
    create_cmds = [c for c in cmds if "--create" in c]
    assert len(create_cmds) == n_tiers

def test_mdadm_raid_level_in_command():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    disk_paths = ["/dev/sdb", "/dev/sdc", "/dev/sdd"]
    cmds = generate_mdadm_commands(plan, disk_paths)
    create_cmd = next(c for c in cmds if "--create" in c)
    # The command uses separate args: ["--level", "5", ...]
    level_idx = create_cmd.index("--level")
    assert create_cmd[level_idx + 1] == "5"

def test_mdadm_raid1_for_two_drives():
    sizes = [2 * TB, 2 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    assert plan.raid_groups[0].level == 1
    disk_paths = ["/dev/sdb", "/dev/sdc"]
    cmds = generate_mdadm_commands(plan, disk_paths)
    create_cmd = next(c for c in cmds if "--create" in c)
    level_idx = create_cmd.index("--level")
    assert create_cmd[level_idx + 1] == "1"


# ---------------------------------------------------------------------------
# LVM command generation
# ---------------------------------------------------------------------------

def test_lvm_commands_order():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_lvm_commands(plan)
    executables = [c[0] for c in cmds]
    # pvcreate before vgcreate before lvcreate
    assert executables.index("pvcreate") < executables.index("vgcreate")
    assert executables.index("vgcreate") < executables.index("lvcreate")

def test_lvm_pvcreate_all_md_devices():
    sizes = [1 * TB, 2 * TB, 3 * TB]
    plan = calculate_traid(sizes, redundancy=1)
    n_tiers = len(plan.raid_groups)
    cmds = generate_lvm_commands(plan)
    pvcreate_cmds = [c for c in cmds if c[0] == "pvcreate"]
    # Each tier's md device gets a pvcreate
    assert len(pvcreate_cmds) == n_tiers

def test_lvm_lvcreate_uses_full_free():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_lvm_commands(plan)
    lvcreate_cmd = next(c for c in cmds if c[0] == "lvcreate")
    assert "100%FREE" in lvcreate_cmd

def test_lvm_custom_vg_name():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes, vg_name="myvg")
    cmds = generate_lvm_commands(plan)
    vgcreate_cmd = next(c for c in cmds if c[0] == "vgcreate")
    assert "myvg" in vgcreate_cmd


# ---------------------------------------------------------------------------
# capacity_preview API helper
# ---------------------------------------------------------------------------

def test_capacity_preview_keys():
    sizes = [4 * TB, 4 * TB, 4 * TB, 2 * TB]
    result = capacity_preview(sizes, redundancy=1)
    assert "usable_bytes" in result
    assert "redundancy_disks" in result
    assert "raid_groups" in result

def test_capacity_preview_usable():
    sizes = [4 * TB, 4 * TB, 4 * TB, 2 * TB]
    result = capacity_preview(sizes, redundancy=1)
    assert result["usable_bytes"] == pytest.approx(10 * TB, rel=0.001)

def test_capacity_preview_redundancy_disks_traid1():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    result = capacity_preview(sizes, redundancy=1)
    assert result["redundancy_disks"] == 1

def test_capacity_preview_redundancy_disks_traid2():
    sizes = [2 * TB, 2 * TB, 2 * TB, 2 * TB]
    result = capacity_preview(sizes, redundancy=2)
    assert result["redundancy_disks"] == 2

def test_capacity_preview_raid_groups_count():
    sizes = [4 * TB, 4 * TB, 4 * TB, 2 * TB]
    result = capacity_preview(sizes, redundancy=1)
    assert len(result["raid_groups"]) == 2

def test_capacity_preview_raid_group_fields():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    result = capacity_preview(sizes, redundancy=1)
    for g in result["raid_groups"]:
        assert "tier" in g
        assert "level" in g
        assert "members" in g
        assert "stripe_size_bytes" in g
        assert "usable_bytes" in g

def test_capacity_preview_level_string():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    result = capacity_preview(sizes, redundancy=1)
    # level should be a string like "raid5"
    for g in result["raid_groups"]:
        assert isinstance(g["level"], str)
        assert g["level"].startswith("raid")

def test_capacity_preview_invalid_type():
    with pytest.raises(ValueError):
        capacity_preview([1 * TB], redundancy=1)
