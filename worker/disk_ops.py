"""
Async subprocess wrappers for read-only disk/array/LVM state queries.

run_privileged() is the single chokepoint that actually spawns processes —
nothing outside this module calls create_subprocess_exec directly.
"""

import asyncio
import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# The OS disk is the one whose children include a mounted / or /boot partition.
# We detect it by inspecting lsblk's child mountpoint data.
_OS_MOUNT_MARKERS = {"/", "/boot", "/boot/efi", "/efi"}


async def run_privileged(executable: str, args: list[str]) -> tuple[int, str, str]:
    """
    Spawn a subprocess using exec (no shell). Returns (returncode, stdout, stderr).
    """
    proc = await asyncio.create_subprocess_exec(
        executable,
        *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_b, stderr_b = await proc.communicate()
    return proc.returncode, stdout_b.decode("utf-8", errors="replace"), stderr_b.decode("utf-8", errors="replace")


def _is_os_disk(blockdev: dict) -> bool:
    """Return True if any descendant partition has a system mountpoint."""
    children = blockdev.get("children") or []
    for child in children:
        mp = child.get("mountpoint") or ""
        if mp in _OS_MOUNT_MARKERS:
            return True
        if _is_os_disk(child):
            return True
    return False


def _has_content(blockdev: dict) -> bool:
    """
    Return True if the disk (or any descendant) has real content — i.e. a
    recognised filesystem, RAID membership, LVM label, or a mounted path.
    An empty partition entry (type=part, fstype=null, no mountpoint) is NOT
    considered content; parted mklabel will overwrite it anyway.
    """
    children = blockdev.get("children") or []
    for child in children:
        if child.get("fstype"):          # linux_raid_member, LVM2_member, ext4, …
            return True
        if child.get("mountpoint"):
            return True
        if _has_content(child):          # recurse into md/lvm layers
            return True
    return False


async def next_free_md_index() -> int:
    """Return the lowest /dev/mdN index not currently in use."""
    try:
        content = await asyncio.to_thread(Path("/proc/mdstat").read_text)
        used = {int(n) for n in re.findall(r"\bmd(\d+)\b", content)}
    except Exception:
        used = set()
    i = 0
    while i in used:
        i += 1
    return i


async def scan_disks() -> list[dict]:
    """
    Return a list of physical disks with availability flag.
    Excludes the OS disk (whichever disk hosts /, /boot, etc.).
    """
    rc, out, err = await run_privileged(
        "lsblk",
        ["--json", "--bytes", "-o", "NAME,SIZE,TYPE,MOUNTPOINT,FSTYPE"],
    )
    if rc != 0:
        logger.error("lsblk failed: %s", err)
        return []

    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        logger.error("lsblk produced invalid JSON")
        return []

    result = []
    for dev in data.get("blockdevices", []):
        if dev.get("type") != "disk":
            continue

        is_os = _is_os_disk(dev)
        is_mounted = bool(dev.get("mountpoint"))
        has_content = _has_content(dev)  # real fs/RAID/LVM on any descendant
        used_by = _find_vg_name(dev) if has_content else None

        result.append({
            "name": f"/dev/{dev['name']}",
            "size": int(dev.get("size", 0)),
            "type": dev.get("type", "disk"),
            "mountpoint": dev.get("mountpoint"),
            "fstype": dev.get("fstype"),
            "available": not is_os and not is_mounted and not has_content,
            "used_by": used_by,
        })

    return result


async def get_array_detail(device: str) -> dict:
    """
    Run `mdadm --detail --export <device>` and parse KEY=VALUE pairs.
    """
    rc, out, err = await run_privileged("mdadm", ["--detail", "--export", device])
    if rc != 0:
        logger.error("mdadm --detail failed for %s: %s", device, err)
        return {"error": err.strip()}

    result: dict = {}
    for line in out.splitlines():
        line = line.strip()
        if "=" in line:
            key, _, value = line.partition("=")
            result[key.strip()] = value.strip()
    return result


def _dm_name_to_vg(dm_name: str) -> str | None:
    """
    Extract the VG name from a device-mapper node name.
    DM doubles hyphens inside VG/LV names, so 'my--vg-my--lv' → 'my-vg'.
    """
    placeholder = "\x00"
    escaped = dm_name.replace("--", placeholder)
    idx = escaped.find("-")
    if idx == -1:
        return None
    vg = escaped[:idx].replace(placeholder, "-")
    return vg or None


def _find_vg_name(blockdev: dict) -> str | None:
    """Return the VG name if any descendant node is an LVM logical volume."""
    for child in (blockdev.get("children") or []):
        if child.get("type") == "lvm":
            return _dm_name_to_vg(child.get("name", ""))
        found = _find_vg_name(child)
        if found:
            return found
    return None


def _base_disk(partition: str) -> str:
    """Derive the whole-disk device from a partition path.
    /dev/vdb1 → /dev/vdb,  /dev/nvme0n1p1 → /dev/nvme0n1
    """
    m = re.match(r"(/dev/nvme\d+n\d+)p\d+$", partition)
    if m:
        return m.group(1)
    m = re.match(r"(/dev/[a-z]+)\d+$", partition)
    if m:
        return m.group(1)
    return partition


async def delete_array(vg_name: str) -> dict:
    """
    Tear down a TRAID volume completely:
      1. Deactivate + remove LVM (VG and all its LVs)
      2. Remove PV labels from md devices
      3. Stop md arrays
      4. Zero md superblocks from member partitions
      5. Wipe GPT/partition-table signatures from base disks
    """
    # Step 1: Discover which md devices are PVs in this VG
    rc, out, _ = await run_privileged("pvs", [
        "--select", f"vg_name={vg_name}",
        "--reportformat", "json",
        "--options", "pv_name",
        "--nosuffix",
    ])
    md_devices: list[str] = []
    if rc == 0:
        try:
            for pv in json.loads(out)["report"][0]["pv"]:
                md_devices.append(pv["pv_name"])
        except (json.JSONDecodeError, KeyError, IndexError):
            pass

    # Step 2: Discover member partitions of each md device (must query before stopping)
    member_partitions: list[str] = []
    for md_dev in md_devices:
        rc, out, _ = await run_privileged("mdadm", ["--detail", md_dev])
        if rc == 0:
            for line in out.splitlines():
                parts = line.split()
                if len(parts) >= 6 and parts[-1].startswith("/dev/") and parts[-1] != md_dev:
                    member_partitions.append(parts[-1])

    base_disks = sorted({_base_disk(p) for p in member_partitions})

    # Step 3: Deactivate and remove the volume group (also removes all LVs)
    await run_privileged("vgchange", ["-an", vg_name])
    await run_privileged("vgremove", ["-f", vg_name])

    # Step 4: Remove PV labels so pvs/vgs no longer see the md devices
    for md_dev in md_devices:
        await run_privileged("pvremove", ["-f", md_dev])

    # Step 5: Stop md arrays
    for md_dev in md_devices:
        await run_privileged("mdadm", ["--stop", md_dev])

    # Step 6: Zero md superblocks so mdadm won't auto-assemble on next boot
    for part in member_partitions:
        await run_privileged("mdadm", ["--zero-superblock", part])

    # Step 7: Wipe GPT and filesystem signatures from the raw disks
    for disk in base_disks:
        await run_privileged("wipefs", ["-a", disk])

    logger.info(
        "array_delete: removed vg=%s md_devices=%s disks=%s",
        vg_name, md_devices, base_disks,
    )
    return {"deleted": True, "vg_name": vg_name, "md_devices": md_devices, "disks": base_disks}


async def get_lvm_report() -> dict:
    """
    Run pvs, vgs, and lsblk concurrently; return merged JSON report.
    Each VG entry gains a 'disks' key listing the physical disks it spans.
    """
    pvs_task = asyncio.create_task(
        run_privileged("pvs", ["--reportformat", "json", "--units", "b", "--nosuffix"])
    )
    vgs_task = asyncio.create_task(
        run_privileged("vgs", ["--reportformat", "json", "--units", "b", "--nosuffix"])
    )
    lsblk_task = asyncio.create_task(
        run_privileged("lsblk", ["--json", "--bytes", "-o", "NAME,SIZE,TYPE,MOUNTPOINT,FSTYPE"])
    )

    (pvs_rc, pvs_out, pvs_err), (vgs_rc, vgs_out, vgs_err), (lb_rc, lb_out, _) = (
        await asyncio.gather(pvs_task, vgs_task, lsblk_task)
    )

    pvs_data: list = []
    if pvs_rc == 0:
        try:
            pvs_data = json.loads(pvs_out).get("report", [{}])[0].get("pv", [])
        except (json.JSONDecodeError, IndexError, KeyError):
            logger.error("pvs produced invalid JSON")
    else:
        logger.error("pvs failed: %s", pvs_err)

    vgs_data: list = []
    if vgs_rc == 0:
        try:
            vgs_data = json.loads(vgs_out).get("report", [{}])[0].get("vg", [])
        except (json.JSONDecodeError, IndexError, KeyError):
            logger.error("vgs produced invalid JSON")
    else:
        logger.error("vgs failed: %s", vgs_err)

    # Build vg_name → [disk paths] map from lsblk tree
    vg_disks: dict[str, list[str]] = {}
    if lb_rc == 0:
        try:
            for dev in json.loads(lb_out).get("blockdevices", []):
                if dev.get("type") == "disk":
                    vg = _find_vg_name(dev)
                    if vg:
                        vg_disks.setdefault(vg, []).append(f"/dev/{dev['name']}")
        except (json.JSONDecodeError, KeyError):
            pass

    for vg in vgs_data:
        vg["disks"] = vg_disks.get(vg.get("vg_name", ""), [])

    return {"pvs": pvs_data, "vgs": vgs_data}


async def rename_vg(old_name: str, new_name: str) -> dict:
    """Rename an LVM volume group."""
    rc, _, err = await run_privileged("vgrename", [old_name, new_name])
    if rc != 0:
        raise RuntimeError(f"vgrename failed: {err.strip()}")
    logger.info("vg renamed: %s → %s", old_name, new_name)
    return {"renamed": True, "old_name": old_name, "new_name": new_name}


# ---------------------------------------------------------------------------
# Constants for new operations
# ---------------------------------------------------------------------------

MiB = 1024 * 1024
BACKUP_DIR = Path("/boot/traid-reshape")   # mdadm reshape backup files (safe: on OS disk)
REPORT_DIR = Path("/var/lib/traid/reports")  # SMART / badblocks / erase reports


# ---------------------------------------------------------------------------
# Helpers: partition paths, md detail, partition layout
# ---------------------------------------------------------------------------

def _part_path(disk: str, num: int) -> str:
    """Return /dev/nvme0n1p1 or /dev/vdb1 style partition path."""
    if "nvme" in disk or disk[-1].isdigit():
        return f"{disk}p{num}"
    return f"{disk}{num}"


async def get_md_detail(md_dev: str) -> dict:
    """
    Parse mdadm --detail output.
    Returns: level(int), raid_devices(int), active_devices(int),
             spare_devices(int), members(list[str]), spares(list[str])
    """
    rc, out, err = await run_privileged("mdadm", ["--detail", md_dev])
    if rc != 0:
        raise RuntimeError(f"mdadm --detail {md_dev}: {err.strip()}")

    result: dict = {
        "device": md_dev, "level": 5,
        "raid_devices": 0, "active_devices": 0, "spare_devices": 0,
        "members": [], "spares": [],
    }
    for line in out.splitlines():
        s = line.strip()
        if s.startswith("Raid Level"):
            m = re.search(r"raid(\d+)", s)
            if m:
                result["level"] = int(m.group(1))
        elif s.startswith("Raid Devices"):
            result["raid_devices"] = int(s.split(":")[-1].strip())
        elif s.startswith("Active Devices"):
            result["active_devices"] = int(s.split(":")[-1].strip())
        elif s.startswith("Spare Devices"):
            result["spare_devices"] = int(s.split(":")[-1].strip())
        else:
            parts = s.split()
            if len(parts) >= 2 and parts[-1].startswith("/dev/") and parts[-1] != md_dev:
                dev = parts[-1]
                if "spare" in s and "active" not in s and "sync" not in s:
                    result["spares"].append(dev)
                else:
                    result["members"].append(dev)
    return result


async def get_md_devices_for_vg(vg_name: str) -> list[str]:
    """Return /dev/mdX paths that are PVs in vg_name, sorted by md number."""
    rc, out, _ = await run_privileged("pvs", [
        "--select", f"vg_name={vg_name}",
        "--reportformat", "json", "--options", "pv_name", "--nosuffix",
    ])
    devs: list[str] = []
    if rc == 0:
        try:
            for pv in json.loads(out)["report"][0]["pv"]:
                devs.append(pv["pv_name"])
        except (json.JSONDecodeError, KeyError, IndexError):
            pass
    devs.sort(key=lambda d: int(re.search(r"\d+$", d).group() if re.search(r"\d+$", d) else "0"))
    return devs


async def get_disk_size(disk: str) -> int:
    """Return disk size in bytes via lsblk."""
    rc, out, _ = await run_privileged(
        "lsblk", ["--bytes", "--nodeps", "--output", "SIZE", "--noheadings", disk]
    )
    if rc != 0:
        raise RuntimeError(f"Cannot get size of {disk}")
    return int(out.strip())


async def get_partition_layout(disk: str) -> list[dict]:
    """
    Read partition table via parted -ms. Returns:
    [{number, start_bytes, end_bytes, name}, ...]
    """
    rc, out, _ = await run_privileged("parted", ["-ms", disk, "unit", "B", "print"])
    partitions = []
    for line in out.splitlines():
        line = line.rstrip(";")
        parts = line.split(":")
        if len(parts) >= 4 and parts[0].isdigit():
            try:
                partitions.append({
                    "number": int(parts[0]),
                    "start_bytes": int(parts[1].rstrip("B")),
                    "end_bytes": int(parts[2].rstrip("B")),
                    "name": parts[5] if len(parts) > 5 else f"traid-tier{int(parts[0]) - 1}",
                })
            except (ValueError, IndexError):
                pass
    return partitions


async def partition_disk_like(new_disk: str, layout: list[dict]) -> None:
    """Create GPT partitions on new_disk matching layout."""
    await run_privileged("parted", ["-s", new_disk, "mklabel", "gpt"])
    for p in layout:
        # Use floor for start (align inward) and ceiling for end (align outward)
        # so the new partition is never smaller than the original member partition.
        # parted reports end_bytes as the inclusive last-byte address, which
        # floor-divides to one MiB short when not exactly aligned.
        start_mib = p["start_bytes"] // MiB
        end_mib = (p["end_bytes"] + MiB - 1) // MiB  # ceiling
        name = p.get("name") or f"traid-tier{p['number'] - 1}"
        rc, _, err = await run_privileged("parted", [
            "-s", new_disk, "mkpart", name, "ext4",
            f"{start_mib}MiB", f"{end_mib}MiB",
        ])
        if rc != 0:
            raise RuntimeError(f"parted mkpart on {new_disk} failed: {err.strip()}")


async def get_vg_lv(vg_name: str) -> str | None:
    """Return the first LV path in vg_name, or None."""
    rc, out, _ = await run_privileged("lvs", [
        "--reportformat", "json", "--options", "lv_name,vg_name",
        "--select", f"vg_name={vg_name}",
    ])
    if rc == 0:
        try:
            lvs = json.loads(out)["report"][0]["lv"]
            if lvs:
                return f"/dev/{vg_name}/{lvs[0]['lv_name'].strip()}"
        except (json.JSONDecodeError, KeyError, IndexError):
            pass
    return None


async def wait_for_md_idle(md_devs: list[str], update_fn, poll_sec: float = 5.0) -> None:
    """
    Poll /proc/mdstat until none of md_devs show active sync/reshape/recovery.
    update_fn(step: str, pct: float) is called each poll with progress.
    """
    md_names = {d.replace("/dev/", "") for d in md_devs}
    from worker.mdstat_reader import parse_mdstat
    while True:
        raw = await asyncio.to_thread(lambda: Path("/proc/mdstat").read_text(errors="replace"))
        arrays = parse_mdstat(raw)
        busy = [a for a in arrays if a["name"] in md_names and a["sync_pct"] is not None]
        if not busy:
            break
        a = busy[0]
        eta = f" ETA {a['finish_eta']}" if a["finish_eta"] else ""
        update_fn(f"Syncing {a['name']}: {a['sync_pct']:.1f}%{eta}", a["sync_pct"] or 0.0)
        await asyncio.sleep(poll_sec)


# ---------------------------------------------------------------------------
# Migration: TRAID-1 → TRAID-2
# ---------------------------------------------------------------------------

async def migrate_to_traid2(vg_name: str, new_disk: str, update_fn) -> None:
    """
    Online reshape of every md array in vg_name from RAID-5/1 → RAID-6.
    new_disk must be >= the largest disk currently in the array.
    Power-failure safe: uses --backup-file on /boot and write-intent bitmaps.
    """
    update_fn("Discovering array structure…", 0.0)
    md_devs = await get_md_devices_for_vg(vg_name)
    if not md_devs:
        raise RuntimeError(f"No md devices found for VG {vg_name!r}")

    # Collect all physical member disks
    all_member_parts: list[str] = []
    for md in md_devs:
        d = await get_md_detail(md)
        all_member_parts.extend(d["members"])
    member_disks = sorted({_base_disk(p) for p in all_member_parts})
    if not member_disks:
        raise RuntimeError("Could not identify member disks from md detail")

    # Validate size constraint
    max_existing = max([await get_disk_size(d) for d in member_disks])
    new_size = await get_disk_size(new_disk)
    if new_size < max_existing:
        raise RuntimeError(
            f"{new_disk} ({new_size // (1024**3)} GiB) is smaller than the "
            f"largest existing disk ({max_existing // (1024**3)} GiB)"
        )
    total_disks = len(member_disks) + 1
    if total_disks < 4:
        raise RuntimeError(
            f"TRAID-2 needs ≥ 4 disks; current {len(member_disks)} + 1 new = {total_disks}"
        )

    # LVM metadata backup
    update_fn("Backing up LVM metadata…", 2.0)
    await run_privileged("vgcfgbackup", [vg_name])

    # Read partition layout from the disk with the most partitions (largest)
    ref_disk = _base_disk(all_member_parts[-1])  # last part tends to be from largest disk
    layout = await get_partition_layout(ref_disk)
    if not layout:
        raise RuntimeError(f"Cannot read partition layout from {ref_disk}")

    # Partition the new disk identically
    update_fn(f"Partitioning {new_disk}…", 5.0)
    await partition_disk_like(new_disk, layout)
    await asyncio.sleep(1)
    await run_privileged("partprobe", [new_disk])
    await asyncio.sleep(2)

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # Reshape each md array
    n_md = len(md_devs)
    for idx, md_dev in enumerate(md_devs):
        base_pct = 10.0 + (idx / n_md) * 80.0
        md_name = md_dev.replace("/dev/", "")
        update_fn(f"Processing {md_dev}…", base_pct)

        detail = await get_md_detail(md_dev)
        level = detail["level"]
        n_active = detail["active_devices"] or detail["raid_devices"]

        # Write-intent bitmap (speeds up recovery after power loss)
        update_fn(f"Adding write-intent bitmap to {md_dev}…", base_pct + 1)
        await run_privileged("mdadm", ["--grow", md_dev, "--bitmap=internal"])

        # Partition number on new_disk for this tier (tier idx → partition idx+1)
        part_num = idx + 1
        new_part = _part_path(new_disk, part_num)
        backup_file = str(BACKUP_DIR / f"{md_name}_reshape.img")

        # Add new partition as spare
        update_fn(f"Adding {new_part} as spare to {md_dev}…", base_pct + 2)
        rc, _, err = await run_privileged("mdadm", [md_dev, "--add", new_part])
        if rc != 0:
            raise RuntimeError(f"mdadm --add {new_part} → {md_dev} failed: {err.strip()}")

        if level == 5:
            # RAID-5 (n) → RAID-6 (n+1); usable stays the same
            update_fn(f"Reshaping {md_dev}: RAID-5 → RAID-6…", base_pct + 3)
            rc, _, err = await run_privileged("mdadm", [
                "--grow", md_dev,
                "--level=6", f"--raid-devices={n_active + 1}",
                f"--backup-file={backup_file}",
            ])
            if rc != 0:
                raise RuntimeError(f"mdadm --grow {md_dev} RAID-5→6 failed: {err.strip()}")

        elif level == 1:
            # RAID-1 (2) → RAID-5 (3) intermediate, then → RAID-6 (4) if enough disks
            bf_a = str(BACKUP_DIR / f"{md_name}_reshape_a.img")
            update_fn(f"Reshaping {md_dev}: RAID-1 → RAID-5 (intermediate)…", base_pct + 3)
            rc, _, err = await run_privileged("mdadm", [
                "--grow", md_dev,
                "--level=5", "--raid-devices=3",
                f"--backup-file={bf_a}",
            ])
            if rc != 0:
                raise RuntimeError(f"mdadm --grow {md_dev} RAID-1→5 failed: {err.strip()}")
            update_fn(f"Waiting for {md_dev} RAID-1→5 reshape…", base_pct + 4)
            await wait_for_md_idle([md_dev], update_fn)

            if total_disks >= 4:
                bf_b = str(BACKUP_DIR / f"{md_name}_reshape_b.img")
                update_fn(f"Reshaping {md_dev}: RAID-5 → RAID-6…", base_pct + 5)
                rc, _, err = await run_privileged("mdadm", [
                    "--grow", md_dev,
                    "--level=6", "--raid-devices=4",
                    f"--backup-file={bf_b}",
                ])
                if rc != 0:
                    raise RuntimeError(f"mdadm --grow {md_dev} RAID-5→6 failed: {err.strip()}")

        # Wait for reshape to finish before processing next md
        update_fn(f"Waiting for {md_dev} reshape to complete…", base_pct + 5)
        await wait_for_md_idle([md_dev], update_fn)

    update_fn("Migration to TRAID-2 complete", 100.0)


# ---------------------------------------------------------------------------
# Migration: TRAID-2 → TRAID-1
# ---------------------------------------------------------------------------

async def migrate_to_traid1(vg_name: str, update_fn) -> None:
    """
    Online reshape of every md array in vg_name from RAID-6 → RAID-5 (or RAID-1).
    One disk is freed per md, then wiped.
    """
    update_fn("Discovering array structure…", 0.0)
    md_devs = await get_md_devices_for_vg(vg_name)
    if not md_devs:
        raise RuntimeError(f"No md devices found for VG {vg_name!r}")

    await run_privileged("vgcfgbackup", [vg_name])
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    freed_disks: set[str] = set()
    n_md = len(md_devs)

    for idx, md_dev in enumerate(md_devs):
        base_pct = 5.0 + (idx / n_md) * 80.0
        md_name = md_dev.replace("/dev/", "")
        detail = await get_md_detail(md_dev)
        level = detail["level"]
        n_active = detail["active_devices"] or detail["raid_devices"]

        if level == 6 and n_active >= 4:
            backup_file = str(BACKUP_DIR / f"{md_name}_downgrade.img")
            update_fn(f"Reshaping {md_dev}: RAID-6 → RAID-5…", base_pct)
            target_level = 5 if (n_active - 1) >= 3 else 1
            target_n = n_active - 1
            rc, _, err = await run_privileged("mdadm", [
                "--grow", md_dev,
                f"--level={target_level}", f"--raid-devices={target_n}",
                f"--backup-file={backup_file}",
            ])
            if rc != 0:
                raise RuntimeError(f"mdadm --grow {md_dev} RAID-6→5 failed: {err.strip()}")
            update_fn(f"Waiting for {md_dev} reshape…", base_pct + 2)
            await wait_for_md_idle([md_dev], update_fn)

        # After reshape, find and remove spare devices
        detail2 = await get_md_detail(md_dev)
        for spare in detail2["spares"]:
            await run_privileged("mdadm", [md_dev, "--fail", spare])
            await run_privileged("mdadm", [md_dev, "--remove", spare])
            freed_disks.add(_base_disk(spare))

    # Wipe freed disks
    for disk in freed_disks:
        update_fn(f"Wiping freed disk {disk}…", 90.0)
        await run_privileged("wipefs", ["-a", disk])

    update_fn("Migration to TRAID-1 complete", 100.0)


# ---------------------------------------------------------------------------
# Disk replacement
# ---------------------------------------------------------------------------

async def disk_replace(vg_name: str, old_disk: str, new_disk: str, update_fn) -> None:
    """Replace old_disk in all md arrays of vg_name with new_disk."""
    update_fn("Discovering array structure…", 0.0)
    md_devs = await get_md_devices_for_vg(vg_name)
    if not md_devs:
        raise RuntimeError(f"No md devices for VG {vg_name!r}")

    # Get partition layout of old_disk to replicate on new_disk
    layout = await get_partition_layout(old_disk)
    if not layout:
        raise RuntimeError(f"Cannot read partition layout from {old_disk}")

    update_fn(f"Partitioning {new_disk} to match {old_disk}…", 5.0)
    await partition_disk_like(new_disk, layout)
    await asyncio.sleep(1)
    await run_privileged("partprobe", [new_disk])
    await asyncio.sleep(2)

    n_md = len(md_devs)
    for idx, md_dev in enumerate(md_devs):
        base_pct = 10.0 + (idx / n_md) * 75.0
        detail = await get_md_detail(md_dev)

        # Find which partition of old_disk is in this array
        old_part = next((m for m in detail["members"] if _base_disk(m) == old_disk), None)
        old_spare = next((s for s in detail["spares"] if _base_disk(s) == old_disk), None)
        old_member = old_part or old_spare
        if not old_member:
            continue  # old_disk not in this md

        # Derive partition number from old member path to pick same number on new disk
        m = re.search(r"p?(\d+)$", old_member)
        part_num = int(m.group(1)) if m else (idx + 1)
        new_part = _part_path(new_disk, part_num)

        update_fn(f"Replacing {old_member} → {new_part} in {md_dev}…", base_pct)
        await run_privileged("mdadm", [md_dev, "--fail", old_member])
        await asyncio.sleep(0.5)
        await run_privileged("mdadm", [md_dev, "--remove", old_member])
        rc, _, err = await run_privileged("mdadm", [md_dev, "--add", new_part])
        if rc != 0:
            raise RuntimeError(f"mdadm --add {new_part} → {md_dev}: {err.strip()}")

    update_fn("Waiting for rebuild to complete…", 85.0)
    await wait_for_md_idle(md_devs, update_fn)

    # Wipe old disk
    update_fn(f"Wiping {old_disk}…", 97.0)
    await run_privileged("wipefs", ["-a", old_disk])
    update_fn("Disk replacement complete", 100.0)


# ---------------------------------------------------------------------------
# Array grow (add one disk to all tiers)
# ---------------------------------------------------------------------------

async def array_grow(vg_name: str, new_disk: str, update_fn) -> None:
    """Add new_disk to every md array in vg_name and grow each array by 1 device."""
    update_fn("Discovering array structure…", 0.0)
    md_devs = await get_md_devices_for_vg(vg_name)
    if not md_devs:
        raise RuntimeError(f"No md devices for VG {vg_name!r}")

    # Use the reference disk with the most partitions (= largest disk in array)
    first_detail = await get_md_detail(md_devs[0])
    ref_disk = _base_disk(first_detail["members"][0]) if first_detail["members"] else None
    if not ref_disk:
        raise RuntimeError("Cannot identify reference disk")

    # Find disk with most partitions
    for md in md_devs:
        d = await get_md_detail(md)
        for mem in d["members"] + d["spares"]:
            bd = _base_disk(mem)
            if len(await get_partition_layout(bd)) > len(await get_partition_layout(ref_disk)):
                ref_disk = bd

    layout = await get_partition_layout(ref_disk)
    if not layout:
        raise RuntimeError(f"Cannot read partition layout from {ref_disk}")

    update_fn(f"Partitioning {new_disk}…", 5.0)
    await partition_disk_like(new_disk, layout)
    await asyncio.sleep(1)
    await run_privileged("partprobe", [new_disk])
    await asyncio.sleep(2)
    await run_privileged("vgcfgbackup", [vg_name])
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    n_md = len(md_devs)
    for idx, md_dev in enumerate(md_devs):
        base_pct = 10.0 + (idx / n_md) * 70.0
        detail = await get_md_detail(md_dev)
        n_active = detail["active_devices"] or detail["raid_devices"]
        part_num = idx + 1
        new_part = _part_path(new_disk, part_num)
        backup_file = str(BACKUP_DIR / f"{md_dev.replace('/dev/', '')}_grow.img")

        update_fn(f"Adding {new_part} to {md_dev}…", base_pct)
        rc, _, err = await run_privileged("mdadm", [md_dev, "--add", new_part])
        if rc != 0:
            raise RuntimeError(f"mdadm --add {new_part}: {err.strip()}")

        update_fn(f"Growing {md_dev} to {n_active + 1} devices…", base_pct + 2)
        rc, _, err = await run_privileged("mdadm", [
            "--grow", md_dev,
            f"--raid-devices={n_active + 1}",
            f"--backup-file={backup_file}",
        ])
        if rc != 0:
            raise RuntimeError(f"mdadm --grow {md_dev}: {err.strip()}")

        update_fn(f"Waiting for {md_dev} reshape…", base_pct + 3)
        await wait_for_md_idle([md_dev], update_fn)

    # Resize PVs and LV to claim new space
    update_fn("Resizing LVM PVs…", 85.0)
    for md_dev in md_devs:
        await run_privileged("pvresize", [md_dev])

    lv = await get_vg_lv(vg_name)
    if lv:
        update_fn("Extending logical volume…", 90.0)
        await run_privileged("lvextend", ["-l", "+100%FREE", lv])
        # Try filesystem resize
        update_fn("Resizing filesystem…", 95.0)
        rc, out, _ = await run_privileged("blkid", ["-o", "value", "-s", "TYPE", lv])
        fs = out.strip()
        if fs in ("ext2", "ext3", "ext4"):
            await run_privileged("resize2fs", [lv])
        elif fs == "xfs":
            rc2, mnt, _ = await run_privileged("findmnt", ["-n", "-o", "TARGET", "--source", lv])
            if rc2 == 0 and mnt.strip():
                await run_privileged("xfs_growfs", [mnt.strip()])

    update_fn("Array grow complete", 100.0)


# ---------------------------------------------------------------------------
# Array shrink (remove one disk)
# ---------------------------------------------------------------------------

async def array_shrink(vg_name: str, disk_to_remove: str, update_fn) -> None:
    """Remove disk_to_remove from all md arrays in vg_name and wipe the disk."""
    update_fn("Discovering array structure…", 0.0)
    md_devs = await get_md_devices_for_vg(vg_name)
    if not md_devs:
        raise RuntimeError(f"No md devices for VG {vg_name!r}")

    await run_privileged("vgcfgbackup", [vg_name])
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    n_md = len(md_devs)
    for idx, md_dev in enumerate(md_devs):
        base_pct = 5.0 + (idx / n_md) * 80.0
        detail = await get_md_detail(md_dev)

        old_member = next(
            (m for m in detail["members"] + detail["spares"] if _base_disk(m) == disk_to_remove),
            None,
        )
        if not old_member:
            continue

        n_active = detail["active_devices"] or detail["raid_devices"]
        level = detail["level"]
        target_n = n_active - 1
        min_n = {5: 3, 6: 4, 1: 2}.get(level, 3)

        update_fn(f"Failing {old_member} in {md_dev}…", base_pct)
        await run_privileged("mdadm", [md_dev, "--fail", old_member])
        await asyncio.sleep(0.5)
        await run_privileged("mdadm", [md_dev, "--remove", old_member])

        if target_n >= min_n:
            backup_file = str(BACKUP_DIR / f"{md_dev.replace('/dev/', '')}_shrink.img")
            update_fn(f"Shrinking {md_dev} to {target_n} devices…", base_pct + 2)
            await run_privileged("mdadm", [
                "--grow", md_dev,
                f"--raid-devices={target_n}",
                f"--backup-file={backup_file}",
            ])
            update_fn(f"Waiting for {md_dev} reshape…", base_pct + 3)
            await wait_for_md_idle([md_dev], update_fn)

    update_fn(f"Wiping {disk_to_remove}…", 90.0)
    await run_privileged("wipefs", ["-a", disk_to_remove])
    update_fn("Array shrink complete", 100.0)


# ---------------------------------------------------------------------------
# Volume clone to single disk
# ---------------------------------------------------------------------------

async def volume_clone(vg_name: str, target_disk: str, update_fn) -> None:
    """Clone the first LV in vg_name to target_disk using dd."""
    lv = await get_vg_lv(vg_name)
    if not lv:
        raise RuntimeError(f"No logical volume found in VG {vg_name!r}")

    lv_size = await get_disk_size(lv)
    disk_size = await get_disk_size(target_disk)
    if disk_size < lv_size:
        raise RuntimeError(
            f"Target disk {target_disk} ({disk_size // (1024**3)} GiB) is smaller "
            f"than the LV ({lv_size // (1024**3)} GiB)"
        )

    update_fn(f"Cloning {lv} → {target_disk}…", 0.0)

    proc = await asyncio.create_subprocess_exec(
        "dd", f"if={lv}", f"of={target_disk}", "bs=64M", "status=progress",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def stream_stderr():
        assert proc.stderr
        async for raw in proc.stderr:
            line = raw.decode("utf-8", errors="replace").strip()
            # dd progress: "1073741824 bytes (1.1 GB) copied, 10.5 s, 102 MB/s"
            m = re.search(r"(\d+) bytes.*copied", line)
            if m and lv_size > 0:
                pct = min(99.0, int(m.group(1)) / lv_size * 100)
                update_fn(f"Cloning: {pct:.1f}%", pct)

    await asyncio.gather(stream_stderr(), proc.wait())
    if proc.returncode != 0:
        raise RuntimeError(f"dd clone failed (exit {proc.returncode})")
    update_fn("Clone complete", 100.0)


# ---------------------------------------------------------------------------
# Volume backup to CIFS / NFS
# ---------------------------------------------------------------------------

async def volume_backup(
    vg_name: str,
    protocol: str,
    host: str,
    remote_path: str,
    cifs_user: str,
    cifs_pass: str,
    update_fn,
) -> None:
    """Backup LV filesystem to a CIFS or NFS target via rsync."""
    import tempfile, shutil as _shutil

    lv = await get_vg_lv(vg_name)
    if not lv:
        raise RuntimeError(f"No LV found in VG {vg_name!r}")

    # Detect filesystem
    rc, out, _ = await run_privileged("blkid", ["-o", "value", "-s", "TYPE", lv])
    fs_type = out.strip()
    if not fs_type:
        raise RuntimeError(f"Cannot detect filesystem on {lv}; format it first")

    src_dir = tempfile.mkdtemp(prefix="traid_src_")
    tgt_dir = tempfile.mkdtemp(prefix="traid_tgt_")

    try:
        # Mount LV
        update_fn("Mounting volume…", 5.0)
        rc, _, err = await run_privileged("mount", ["-t", fs_type, lv, src_dir])
        if rc != 0:
            raise RuntimeError(f"mount {lv}: {err.strip()}")

        # Mount remote target
        update_fn(f"Mounting {protocol.upper()} share…", 10.0)
        if protocol == "cifs":
            share = f"//{host}/{remote_path.lstrip('/')}"
            opts = f"username={cifs_user},password={cifs_pass},vers=auto,iocharset=utf8"
            rc, _, err = await run_privileged("mount", ["-t", "cifs", share, tgt_dir, "-o", opts])
        else:  # nfs
            share = f"{host}:{remote_path}"
            rc, _, err = await run_privileged("mount", ["-t", "nfs", share, tgt_dir])
        if rc != 0:
            raise RuntimeError(f"mount {protocol} share: {err.strip()}")

        # rsync
        update_fn("Running rsync…", 15.0)
        proc = await asyncio.create_subprocess_exec(
            "rsync", "-av", "--progress", f"{src_dir}/", f"{tgt_dir}/",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        assert proc.stdout
        async for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").strip()
            m = re.search(r"(\d+)%", line)
            if m:
                update_fn(f"rsync: {line[:60]}", 15.0 + float(m.group(1)) * 0.8)
        await proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"rsync failed (exit {proc.returncode})")

        update_fn("Backup complete", 100.0)
    finally:
        await run_privileged("umount", ["-l", src_dir])
        await run_privileged("umount", ["-l", tgt_dir])
        import shutil as _sh
        _sh.rmtree(src_dir, ignore_errors=True)
        _sh.rmtree(tgt_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# SMART test
# ---------------------------------------------------------------------------

async def smart_test(disk: str, test_type: str, report_path: Path, update_fn) -> None:
    """Start a SMART self-test, poll until complete, save report."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    update_fn(f"Starting SMART {test_type} test on {disk}…", 0.0)

    rc, out, err = await run_privileged("smartctl", ["-t", test_type, disk])
    if rc > 8:
        raise RuntimeError(f"smartctl -t {test_type} {disk}: {err.strip()}")

    # Estimate duration
    est_min = {"short": 2, "long": 60}.get(test_type, 5)
    poll_sec = 30.0

    elapsed = 0.0
    try:
        while elapsed < est_min * 60 * 3:  # timeout at 3× estimate
            await asyncio.sleep(poll_sec)
            elapsed += poll_sec

            rc2, out2, _ = await run_privileged("smartctl", ["-a", disk])
            report_text = out2
            done = False
            pct = 0.0

            for line in out2.splitlines():
                l = line.strip()
                if "Self-test execution status" in l:
                    if "completed without error" in l.lower() or "00%" in l:
                        done = True
                        pct = 100.0
                        break
                    elif "aborted" in l.lower() or "failed" in l.lower():
                        done = True
                        pct = 100.0
                        break
                    m = re.search(r"(\d+)%", l)
                    if m:
                        remaining = float(m.group(1))
                        pct = 100.0 - remaining

            update_fn(f"SMART {test_type} test: {pct:.0f}% complete…", pct)
            if done:
                break
    except asyncio.CancelledError:
        # Abort the on-drive test before giving up
        await run_privileged("smartctl", ["-X", disk])
        raise

    # Final report
    rc3, final_out, _ = await run_privileged("smartctl", ["-a", disk])
    report_path.write_text(final_out)
    update_fn(f"SMART {test_type} test complete", 100.0)


# ---------------------------------------------------------------------------
# badblocks write test
# ---------------------------------------------------------------------------

async def badblocks_test(disk: str, report_path: Path, update_fn) -> None:
    """Run badblocks in destructive write mode. Disk must be unallocated."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    update_fn(f"Starting badblocks write test on {disk}…", 0.0)

    proc = await asyncio.create_subprocess_exec(
        "badblocks", "-wsv", disk,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stderr_lines: list[str] = []

    async def read_stderr():
        assert proc.stderr
        async for raw in proc.stderr:
            line = raw.decode("utf-8", errors="replace").strip()
            if line:
                stderr_lines.append(line)
                m = re.search(r"([\d.]+)%\s+done", line)
                if m:
                    update_fn(f"badblocks: {m.group(1)}% done", float(m.group(1)))

    stdout_task = asyncio.create_task(proc.stdout.read() if proc.stdout else _empty())
    try:
        await read_stderr()
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        stdout_task.cancel()
        raise
    await proc.wait()
    stdout_b = await stdout_task

    report = "\n".join(stderr_lines)
    bad = stdout_b.decode("utf-8", errors="replace").strip()
    report += f"\n\n{'=== Bad blocks found ===' if bad else '=== No bad blocks detected ==='}\n{bad or '(none)'}"
    report_path.write_text(report)

    if proc.returncode not in (0, 1):
        raise RuntimeError(f"badblocks exited {proc.returncode}")
    update_fn("badblocks test complete", 100.0)


async def _empty() -> bytes:
    return b""


# ---------------------------------------------------------------------------
# Secure disk erase
# ---------------------------------------------------------------------------

_ERASE_MODES = {
    # mode:        (n_random_passes, final_zero, label)
    "quick":       (0, True,  "Quick Erase (1 zero pass)"),
    "dod_short":   (3, False, "DoD Short (3 passes)"),
    "dod_7":       (7, True,  "DoD 5220.22-M (7 passes + zero)"),
}


async def disk_erase(disk: str, report_path: Path, update_fn, mode: str = "dod_short") -> None:
    """Secure erase using shred. mode: 'quick' | 'dod_short' | 'dod_7'."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    n_passes, final_zero, label = _ERASE_MODES.get(mode, _ERASE_MODES["dod_short"])
    update_fn(f"Starting {label} on {disk}…", 0.0)

    cmd = ["shred", "--verbose", "-n", str(n_passes)]
    if final_zero:
        cmd.append("-z")
    cmd.append(disk)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    lines: list[str] = []
    assert proc.stderr
    try:
        async for raw in proc.stderr:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            lines.append(line)
            # "shred: /dev/vdb: pass 1/4 (random)...1.4GiB/21GiB 6%"
            m = re.search(r"pass (\d+)/(\d+).*?(\d+)%", line)
            if m:
                pass_n, pass_total, pct_pass = int(m.group(1)), int(m.group(2)), float(m.group(3))
                overall = ((pass_n - 1) * 100 + pct_pass) / pass_total
                update_fn(f"Erase pass {pass_n}/{pass_total}: {pct_pass:.0f}%", overall)
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        raise

    await proc.wait()
    report_path.write_text(f"Mode: {label}\nDisk: {disk}\n\n" + "\n".join(lines))
    if proc.returncode != 0:
        raise RuntimeError(f"shred failed (exit {proc.returncode})")
    update_fn("Secure erase complete", 100.0)
