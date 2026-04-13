"""
Async subprocess wrappers for read-only disk/array/LVM state queries.

run_privileged() is the single chokepoint that actually spawns processes —
nothing outside this module calls create_subprocess_exec directly.
"""

import asyncio
import json
import logging

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
        # Recurse for LVM/md children
        if _is_os_disk(child):
            return True
    return False


async def scan_disks() -> list[dict]:
    """
    Return a list of physical disks with availability flag.
    Excludes the OS disk (whichever disk hosts /, /boot, etc.).
    """
    rc, out, err = await run_privileged(
        "lsblk",
        ["--json", "--bytes", "-o", "NAME,SIZE,TYPE,MOUNTPOINT,FSTYPE,CHILDREN"],
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

        result.append({
            "name": f"/dev/{dev['name']}",
            "size": int(dev.get("size", 0)),
            "type": dev.get("type", "disk"),
            "mountpoint": dev.get("mountpoint"),
            "fstype": dev.get("fstype"),
            "available": not is_os and not is_mounted,
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


async def get_lvm_report() -> dict:
    """
    Run pvs and vgs concurrently and return merged JSON report.
    """
    pvs_task = asyncio.create_task(
        run_privileged("pvs", ["--reportformat", "json", "--units", "b", "--nosuffix"])
    )
    vgs_task = asyncio.create_task(
        run_privileged("vgs", ["--reportformat", "json", "--units", "b", "--nosuffix"])
    )

    (pvs_rc, pvs_out, pvs_err), (vgs_rc, vgs_out, vgs_err) = await asyncio.gather(
        pvs_task, vgs_task
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

    return {"pvs": pvs_data, "vgs": vgs_data}
