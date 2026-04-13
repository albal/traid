"""Unit tests for the TRAID tier-slicing algorithm. No hardware required."""

import pytest
from worker.traid_algorithm import (
    MiB,
    ALIGNMENT,
    calculate_traid,
    capacity_preview,
    generate_parted_commands,
    generate_lvm_commands,
    _align_up,
    _align_down,
)

TB = 1024 ** 4
GB = 1024 ** 3


# ---------------------------------------------------------------------------
# Alignment helpers
# ---------------------------------------------------------------------------

def test_align_up():
    assert _align_up(0) == 0
    assert _align_up(1) == MiB
    assert _align_up(MiB) == MiB
    assert _align_up(MiB + 1) == 2 * MiB


def test_align_down():
    assert _align_down(0) == 0
    assert _align_down(MiB - 1) == 0
    assert _align_down(MiB) == MiB
    assert _align_down(MiB + 512) == MiB


# ---------------------------------------------------------------------------
# Invalid input
# ---------------------------------------------------------------------------

def test_too_few_drives_traid1():
    with pytest.raises(ValueError, match="at least 2"):
        calculate_traid([1 * TB], redundancy=1)


def test_too_few_drives_traid2():
    with pytest.raises(ValueError, match="at least 3"):
        calculate_traid([1 * TB, 2 * TB], redundancy=2)


def test_invalid_redundancy():
    with pytest.raises(ValueError):
        calculate_traid([1 * TB, 2 * TB], redundancy=3)


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


# ---------------------------------------------------------------------------
# Reference example: 3×4 TB + 1×2 TB → 10 TB usable (TRAID-1)
# ---------------------------------------------------------------------------

def test_reference_example_traid1():
    sizes = [4 * TB, 4 * TB, 4 * TB, 2 * TB]
    plan = calculate_traid(sizes, redundancy=1)

    # Should produce 2 tiers
    assert len(plan.raid_groups) == 2

    # Tier 0: all 4 drives × 2 TB slice → RAID 5 → 6 TB usable
    rg0 = plan.raid_groups[0]
    assert rg0.level == 5
    assert rg0.usable_bytes == pytest.approx(6 * TB, rel=0.001)

    # Tier 1: remaining 3 drives × 2 TB slice → RAID 5 → 4 TB usable
    rg1 = plan.raid_groups[1]
    assert rg1.level == 5
    assert rg1.usable_bytes == pytest.approx(4 * TB, rel=0.001)

    # Total: 10 TB
    assert plan.total_usable_bytes == pytest.approx(10 * TB, rel=0.001)


# ---------------------------------------------------------------------------
# Mismatched drives — cascading tiers
# ---------------------------------------------------------------------------

def test_mismatched_drives_cascade():
    sizes = [1 * TB, 2 * TB, 3 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=1)

    # Tier 0: 4 drives × 1 TB → RAID 5 → 3 TB
    # Tier 1: 3 drives × 1 TB → RAID 5 → 2 TB
    # Tier 2: 2 drives × 1 TB → RAID 1 → 1 TB
    # Total: 6 TB

    assert len(plan.raid_groups) == 3
    assert plan.raid_groups[0].level == 5
    assert plan.raid_groups[1].level == 5
    assert plan.raid_groups[2].level == 1

    assert plan.total_usable_bytes == pytest.approx(6 * TB, rel=0.001)


# ---------------------------------------------------------------------------
# TRAID-2 — four equal drives → RAID 6
# ---------------------------------------------------------------------------

def test_four_equal_drives_traid2():
    sizes = [4 * TB, 4 * TB, 4 * TB, 4 * TB]
    plan = calculate_traid(sizes, redundancy=2)
    assert len(plan.raid_groups) == 1
    rg = plan.raid_groups[0]
    assert rg.level == 6
    assert rg.usable_bytes == 2 * rg.stripe_size_bytes


# ---------------------------------------------------------------------------
# Alignment: all partition boundaries must be MiB-aligned
# ---------------------------------------------------------------------------

def test_partition_alignment():
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


# ---------------------------------------------------------------------------
# Parted command generation
# ---------------------------------------------------------------------------

def test_parted_commands_structure():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_parted_commands("/dev/sdb", 0, plan)
    # First command must be mklabel gpt
    assert cmds[0] == ["parted", "-s", "/dev/sdb", "mklabel", "gpt"]
    # Remaining commands are mkpart
    for cmd in cmds[1:]:
        assert cmd[0] == "parted"
        assert "mkpart" in cmd


def test_parted_mib_boundaries():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_parted_commands("/dev/sdb", 0, plan)
    for cmd in cmds[1:]:
        # start and end should be "NMiB" strings
        start_str = cmd[-2]
        end_str = cmd[-1]
        assert start_str.endswith("MiB"), f"start {start_str!r} not MiB"
        assert end_str.endswith("MiB"), f"end {end_str!r} not MiB"
        assert int(start_str[:-3]) >= 0
        assert int(end_str[:-3]) > int(start_str[:-3])


# ---------------------------------------------------------------------------
# LVM command generation
# ---------------------------------------------------------------------------

def test_lvm_commands():
    sizes = [2 * TB, 2 * TB, 2 * TB]
    plan = calculate_traid(sizes)
    cmds = generate_lvm_commands(plan)
    executables = [c[0] for c in cmds]
    assert "pvcreate" in executables
    assert "vgcreate" in executables
    assert "lvcreate" in executables
    # lvcreate should use 100%FREE
    lvcreate_cmd = next(c for c in cmds if c[0] == "lvcreate")
    assert "100%FREE" in lvcreate_cmd


# ---------------------------------------------------------------------------
# capacity_preview API helper
# ---------------------------------------------------------------------------

def test_capacity_preview_structure():
    sizes = [4 * TB, 4 * TB, 4 * TB, 2 * TB]
    result = capacity_preview(sizes, redundancy=1)
    assert "usable_bytes" in result
    assert "redundancy_disks" in result
    assert "raid_groups" in result
    assert result["usable_bytes"] == pytest.approx(10 * TB, rel=0.001)
    assert result["redundancy_disks"] == 1
    assert len(result["raid_groups"]) == 2
    for g in result["raid_groups"]:
        assert "tier" in g
        assert "level" in g
        assert "members" in g
        assert "stripe_size_bytes" in g
        assert "usable_bytes" in g
