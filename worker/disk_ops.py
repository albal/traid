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
