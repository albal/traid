"""
TRAID (TSEW RAID) tier-slicing algorithm.

Pure Python — no subprocess calls, no I/O. Fully testable without hardware.

Algorithm overview
------------------
Drives are sorted ascending by size. Each iteration ("tier") slices all
remaining drives at the size of the smallest one, forms a RAID group from
those equally-sized slices, removes the smallest drive from the pool, then
repeats with the remaining drives until too few remain for redundancy.

The resulting RAID groups are then stitched together via LVM into a single
logical volume.

Redundancy levels
-----------------
TRAID-1 (redundancy=1): can lose 1 drive.
  - 3+ drives in tier  → RAID 5
  - 2 drives in tier   → RAID 1

TRAID-2 (redundancy=2): can lose 2 drives.
  - 4+ drives in tier  → RAID 6
  - 3 drives in tier   → RAID 6 (minimum)
  - 2 drives in tier   → insufficient, tier skipped
"""

from dataclasses import dataclass, field
from typing import Literal

MiB = 1024 * 1024
ALIGNMENT = MiB  # align all partition boundaries to 1 MiB


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Partition:
    disk_index: int
    tier: int
    start_bytes: int   # inclusive, aligned
    end_bytes: int     # exclusive, aligned


@dataclass
class RaidGroup:
    tier: int
    level: int                    # 1, 5, or 6
    partitions: list[Partition]
    stripe_size_bytes: int        # size of each member partition
    usable_bytes: int             # net usable after parity overhead


@dataclass
class LVMPlan:
    pvcreate: list[str]           # md device paths
    vg_name: str
    lv_name: str


@dataclass
class Plan:
    drives: list[int]             # original sizes, bytes
    partitions: dict[int, list[Partition]]  # disk_index -> partitions
    raid_groups: list[RaidGroup]
    lvm_plan: LVMPlan
    total_usable_bytes: int


# ---------------------------------------------------------------------------
# Alignment helpers
# ---------------------------------------------------------------------------

def _align_down(value: int, alignment: int = ALIGNMENT) -> int:
    return (value // alignment) * alignment


def _align_up(value: int, alignment: int = ALIGNMENT) -> int:
    return ((value + alignment - 1) // alignment) * alignment


# ---------------------------------------------------------------------------
# RAID usable capacity
# ---------------------------------------------------------------------------

def _usable(n_devices: int, level: int, stripe_size: int) -> int:
    if level == 1:
        return stripe_size
    if level == 5:
        return (n_devices - 1) * stripe_size
    if level == 6:
        return (n_devices - 2) * stripe_size
    raise ValueError(f"unsupported RAID level {level}")


def _pick_level(n: int, redundancy: int) -> int | None:
    """Return the RAID level for n drives at the given redundancy, or None if insufficient."""
    if redundancy == 1:
        if n >= 3:
            return 5
        if n == 2:
            return 1
        return None
    if redundancy == 2:
        if n >= 4:
            return 6
        if n == 3:
            return 6
        return None
    raise ValueError(f"unsupported redundancy level {redundancy}")


# ---------------------------------------------------------------------------
# Main algorithm
# ---------------------------------------------------------------------------

def calculate_traid(
    sizes_bytes: list[int],
    redundancy: int = 1,
    vg_name: str = "traid_vg",
    lv_name: str = "traid_lv",
) -> Plan:
    """
    Calculate the TRAID partition/RAID/LVM plan for the given drive sizes.

    Args:
        sizes_bytes: Raw byte sizes of each drive (order does not matter).
        redundancy:  1 for TRAID-1, 2 for TRAID-2.
        vg_name:     LVM volume group name.
        lv_name:     LVM logical volume name.

    Returns:
        A Plan containing partitioning, RAID grouping, and LVM commands.
    """
    if redundancy not in (1, 2):
        raise ValueError("redundancy must be 1 or 2")

    n_drives = len(sizes_bytes)
    if n_drives < redundancy + 1:
        raise ValueError(
            f"need at least {redundancy + 1} drives for TRAID-{redundancy}, got {n_drives}"
        )

    # Map original index → size (sorted ascending, preserving original indices)
    original = list(enumerate(sizes_bytes))
    sorted_indices = sorted(range(n_drives), key=lambda i: sizes_bytes[i])

    # Track how many bytes have been allocated on each drive so far
    allocated: dict[int, int] = {i: 0 for i in range(n_drives)}
    partitions: dict[int, list[Partition]] = {i: [] for i in range(n_drives)}
    raid_groups: list[RaidGroup] = []
    tier = 0

    # active_pool: list of drive indices remaining (sorted ascending by current size)
    active_pool = sorted_indices[:]

    while True:
        n = len(active_pool)
        level = _pick_level(n, redundancy)
        if level is None:
            break  # too few drives to form any group

        # Tier slice size = smallest drive's remaining free space
        smallest_idx = active_pool[0]
        remaining_on_smallest = sizes_bytes[smallest_idx] - allocated[smallest_idx]
        tier_size = _align_down(remaining_on_smallest)

        if tier_size < ALIGNMENT:
            # Remaining space on smallest drive too small to align, skip
            break

        tier_partitions: list[Partition] = []
        for disk_idx in active_pool:
            start = _align_up(allocated[disk_idx])
            end = start + tier_size
            if end > sizes_bytes[disk_idx]:
                # Safety: should not happen given tier_size calculation, but guard anyway
                end = _align_down(sizes_bytes[disk_idx])
                actual_size = end - start
                if actual_size < ALIGNMENT:
                    continue
                tier_size = min(tier_size, actual_size)

            p = Partition(
                disk_index=disk_idx,
                tier=tier,
                start_bytes=start,
                end_bytes=start + tier_size,
            )
            tier_partitions.append(p)
            partitions[disk_idx].append(p)
            allocated[disk_idx] = start + tier_size

        usable = _usable(len(tier_partitions), level, tier_size)
        raid_groups.append(RaidGroup(
            tier=tier,
            level=level,
            partitions=tier_partitions,
            stripe_size_bytes=tier_size,
            usable_bytes=usable,
        ))

        # Consume smallest drive — its full capacity now allocated
        active_pool = active_pool[1:]
        tier += 1

    total_usable = sum(rg.usable_bytes for rg in raid_groups)
    md_devices = [f"/dev/md{i}" for i in range(len(raid_groups))]

    return Plan(
        drives=sizes_bytes,
        partitions=partitions,
        raid_groups=raid_groups,
        lvm_plan=LVMPlan(
            pvcreate=md_devices,
            vg_name=vg_name,
            lv_name=lv_name,
        ),
        total_usable_bytes=total_usable,
    )


# ---------------------------------------------------------------------------
# Command generators
# ---------------------------------------------------------------------------

def generate_parted_commands(
    disk: str,
    disk_index: int,
    plan: Plan,
) -> list[list[str]]:
    """
    Generate parted commands to partition a single disk according to the plan.
    Returns a list of argv lists (no shell, safe for exec).
    """
    cmds: list[list[str]] = []
    cmds.append(["parted", "-s", disk, "mklabel", "gpt"])

    for p in plan.partitions.get(disk_index, []):
        start_mib = p.start_bytes // MiB
        end_mib = p.end_bytes // MiB
        cmds.append([
            "parted", "-s", disk,
            "mkpart", f"traid-tier{p.tier}", "ext4",
            f"{start_mib}MiB", f"{end_mib}MiB",
        ])

    return cmds


def generate_mdadm_commands(
    plan: Plan,
    disk_paths: list[str],
) -> list[list[str]]:
    """
    Generate mdadm --create commands for each RAID group.
    disk_paths[i] is the block device path for drive index i.

    Partition device names are derived by appending the partition number
    (1-based position within the drive's partition list for this tier).
    """
    cmds: list[list[str]] = []
    for rg in plan.raid_groups:
        md_dev = f"/dev/md{rg.tier}"
        member_devs: list[str] = []
        for p in rg.partitions:
            base = disk_paths[p.disk_index]
            # Partition index within this drive = position in partitions list
            part_num = plan.partitions[p.disk_index].index(p) + 1
            # Handle nvme device naming (nvme0n1 → nvme0n1p1)
            if "nvme" in base or base[-1].isdigit():
                member_devs.append(f"{base}p{part_num}")
            else:
                member_devs.append(f"{base}{part_num}")

        cmds.append([
            "mdadm", "--create", md_dev,
            "--level", str(rg.level),
            "--raid-devices", str(len(member_devs)),
            "--metadata", "1.2",
            "--name", f"traid:{rg.tier}",
            *member_devs,
        ])

    return cmds


def generate_lvm_commands(plan: Plan) -> list[list[str]]:
    """
    Generate pvcreate, vgcreate, lvcreate commands to stitch md devices together.
    """
    md_devices = plan.lvm_plan.pvcreate
    vg = plan.lvm_plan.vg_name
    lv = plan.lvm_plan.lv_name

    cmds: list[list[str]] = []

    for dev in md_devices:
        cmds.append(["pvcreate", dev])

    cmds.append(["vgcreate", vg, *md_devices])
    cmds.append(["lvcreate", "-l", "100%FREE", "-n", lv, vg])

    return cmds


# ---------------------------------------------------------------------------
# Capacity preview (API-facing, no hardware required)
# ---------------------------------------------------------------------------

def capacity_preview(
    sizes_bytes: list[int],
    redundancy: int = 1,
) -> dict:
    """
    Return a human-friendly summary of the TRAID plan for the given drives.
    Used by GET /api/preview — pure calculation, no UDS round-trip.
    """
    plan = calculate_traid(sizes_bytes, redundancy)
    groups = []
    for rg in plan.raid_groups:
        groups.append({
            "tier": rg.tier,
            "level": f"raid{rg.level}",
            "members": len(rg.partitions),
            "stripe_size_bytes": rg.stripe_size_bytes,
            "usable_bytes": rg.usable_bytes,
        })
    return {
        "usable_bytes": plan.total_usable_bytes,
        "redundancy_disks": redundancy,
        "raid_groups": groups,
    }
