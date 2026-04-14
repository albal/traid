"""
Filesystem operations for TRAID volumes (ext4 and Btrfs).

All functions run as root (called from daemon.py).
Mount points live under /mnt/traid/{vg_name}.
Persistent state (fstype, options, mounted) is kept in /var/lib/traid/fs_state.json.
"""

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path

try:
    import btrfsutil
    _BTRFSUTIL_AVAILABLE = True
except ImportError:
    _BTRFSUTIL_AVAILABLE = False
    logging.getLogger(__name__).warning(
        "btrfsutil not available; falling back to CLI for subvolume operations"
    )

logger = logging.getLogger(__name__)

MOUNT_BASE = Path("/mnt/traid")
STATE_FILE = Path("/var/lib/traid/fs_state.json")
SEND_DIR   = Path("/var/lib/traid/btrfs-streams")   # staging area for send/receive

_BTRFS_COMPRESS_ALLOWED = {"zstd", "lzo", "zlib", "none", ""}


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

async def _run(*cmd: str, input_data: bytes = b"") -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate(input=input_data or None)
    return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _mount_point(vg_name: str) -> Path:
    return MOUNT_BASE / vg_name


def _is_mounted(mp: Path) -> bool:
    try:
        mp_str = str(mp)
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == mp_str:
                    return True
    except OSError:
        pass
    return False


async def _get_lv(vg_name: str) -> str:
    """Return the first LV device path for the given VG."""
    rc, out, _ = await _run(
        "lvs", "--reportformat", "json", "--options", "lv_name,vg_name",
        "--nosuffix", vg_name
    )
    if rc == 0:
        try:
            lvs = json.loads(out).get("report", [{}])[0].get("lv", [])
            if lvs:
                return f"/dev/{vg_name}/{lvs[0]['lv_name'].strip()}"
        except (json.JSONDecodeError, IndexError, KeyError):
            pass
    raise RuntimeError(f"No logical volume found in VG {vg_name!r}")


# ---------------------------------------------------------------------------
# Format
# ---------------------------------------------------------------------------

async def format_volume(vg_name: str, fstype: str,
                        label: str = "", compression: str = "") -> dict:
    """Format the LV with ext4 or btrfs."""
    if fstype not in ("ext4", "btrfs"):
        raise ValueError(f"Unsupported filesystem type: {fstype!r}")
    if compression and compression not in _BTRFS_COMPRESS_ALLOWED:
        raise ValueError(f"Invalid compression: {compression!r}")

    lv = await _get_lv(vg_name)
    mp = _mount_point(vg_name)

    if _is_mounted(mp):
        raise RuntimeError("Volume is mounted — unmount before formatting")

    if fstype == "ext4":
        cmd = ["mkfs.ext4", "-F"]
        if label:
            cmd += ["-L", label[:16]]   # ext4 label limit
        cmd.append(lv)
    else:  # btrfs
        cmd = ["mkfs.btrfs", "-f"]
        if label:
            cmd += ["-L", label[:255]]
        cmd.append(lv)

    rc, _, err = await _run(*cmd)
    if rc != 0:
        raise RuntimeError(f"mkfs.{fstype} failed: {err.strip()}")

    state = _load_state()
    state[vg_name] = {
        "fstype": fstype, "label": label,
        "compression": compression, "mounted": False,
    }
    _save_state(state)
    logger.info("formatted %s as %s", vg_name, fstype)

    # Auto-mount immediately after formatting
    try:
        await mount_volume(vg_name)
    except Exception as exc:
        logger.warning("auto-mount of %s after format failed: %s", vg_name, exc)

    return {"fstype": fstype, "label": label}


# ---------------------------------------------------------------------------
# Mount / Unmount
# ---------------------------------------------------------------------------

async def _chattr(path: Path, immutable: bool) -> None:
    """Set (+i) or clear (-i) the immutable flag on a directory."""
    flag = "+i" if immutable else "-i"
    rc, _, err = await _run("chattr", flag, str(path))
    if rc != 0:
        logger.warning("chattr %s %s failed: %s", flag, path, err.strip())


async def _ensure_mount_point(mp: Path) -> None:
    """Create mount point directory and make it immutable."""
    if not mp.exists():
        mp.mkdir(parents=True, exist_ok=True)
        await _chattr(mp, immutable=True)


async def mount_volume(vg_name: str) -> dict:
    mp = _mount_point(vg_name)
    if _is_mounted(mp):
        return {"mounted": True, "mount_point": str(mp)}

    lv = await _get_lv(vg_name)
    state = _load_state()
    fsinfo = state.get(vg_name, {})
    fstype = fsinfo.get("fstype", "")
    compression = fsinfo.get("compression", "")

    await _ensure_mount_point(mp)

    cmd = ["mount"]
    if fstype:
        cmd += ["-t", fstype]
    opts = []
    if fstype == "btrfs" and compression and compression != "none":
        opts.append(f"compress={compression}")
    if opts:
        cmd += ["-o", ",".join(opts)]
    cmd += [lv, str(mp)]

    # Lift immutability so the kernel can attach the filesystem.
    # Do NOT restore it afterwards — once mounted the flag would apply to the
    # root of the mounted filesystem, not the underlying directory.
    # It is restored by unmount_volume() after the filesystem is detached.
    await _chattr(mp, immutable=False)
    rc, _, err = await _run(*cmd)
    if rc != 0:
        # Mount failed — re-apply the flag to the bare directory
        await _chattr(mp, immutable=True)
        raise RuntimeError(f"mount failed: {err.strip()}")

    state.setdefault(vg_name, {})["mounted"] = True
    _save_state(state)
    logger.info("mounted %s at %s", vg_name, mp)
    return {"mounted": True, "mount_point": str(mp)}


async def unmount_volume(vg_name: str) -> dict:
    mp = _mount_point(vg_name)
    if not _is_mounted(mp):
        state = _load_state()
        state.setdefault(vg_name, {})["mounted"] = False
        _save_state(state)
        return {"mounted": False}

    rc, _, err = await _run("umount", str(mp))
    if rc != 0:
        raise RuntimeError(f"umount failed: {err.strip()}")

    # Filesystem is now detached — apply immutable flag to the bare directory
    await _chattr(mp, immutable=True)

    state = _load_state()
    state.setdefault(vg_name, {})["mounted"] = False
    _save_state(state)
    logger.info("unmounted %s", vg_name)
    return {"mounted": False}


async def remount_all() -> None:
    """Re-mount any volumes that were mounted before a daemon restart."""
    state = _load_state()
    for vg_name, info in state.items():
        if info.get("mounted"):
            try:
                await mount_volume(vg_name)
                logger.info("remounted %s on startup", vg_name)
            except Exception as exc:
                logger.warning("failed to remount %s: %s", vg_name, exc)


# ---------------------------------------------------------------------------
# Filesystem info
# ---------------------------------------------------------------------------

async def get_fs_info(vg_name: str) -> dict:
    state = _load_state()
    fsinfo = state.get(vg_name, {})
    fstype = fsinfo.get("fstype", "")

    if not fstype:
        return {"formatted": False}

    mp = _mount_point(vg_name)
    mounted = _is_mounted(mp)

    info: dict = {
        "formatted": True,
        "fstype": fstype,
        "label": fsinfo.get("label", ""),
        "compression": fsinfo.get("compression", ""),
        "mounted": mounted,
        "mount_point": str(mp) if mounted else None,
    }

    if mounted:
        await _fill_df_stats(str(mp), info)

        if fstype == "ext4":
            lv = await _get_lv(vg_name)
            rc, out, _ = await _run("tune2fs", "-l", lv)
            if rc == 0:
                info["ext4_details"] = _parse_tune2fs(out)

        elif fstype == "btrfs":
            info["btrfs_usage"] = await _btrfs_usage(str(mp))
            info["btrfs_device_stats"] = await _btrfs_dev_stats(str(mp))

    return info


async def _fill_df_stats(mount_point: str, info: dict) -> None:
    rc, out, _ = await _run(
        "df", "-B1", "--output=size,used,avail,pcent", mount_point
    )
    if rc == 0:
        lines = out.strip().splitlines()
        if len(lines) >= 2:
            p = lines[1].split()
            if len(p) >= 4:
                info["total_bytes"] = int(p[0])
                info["used_bytes"]  = int(p[1])
                info["avail_bytes"] = int(p[2])
                info["use_pct"]     = int(p[3].rstrip("%"))

    # df -i: columns are Filesystem, Inodes, IUsed, IFree, IUse%, Mounted
    # -i and --output are mutually exclusive, so parse positional columns.
    rc, out, _ = await _run("df", "-i", mount_point)
    if rc == 0:
        lines = out.strip().splitlines()
        if len(lines) >= 2:
            p = lines[1].split()
            # p[1]=Inodes  p[2]=IUsed  p[3]=IFree
            if len(p) >= 4:
                def _int_or_none(s):
                    try:
                        return int(s)
                    except ValueError:
                        return None
                inodes_total = _int_or_none(p[1])
                inodes_used  = _int_or_none(p[2])
                inodes_avail = _int_or_none(p[3])
                # btrfs reports 0 for all inode fields — use du --inodes instead
                if inodes_used == 0 and info.get("fstype") == "btrfs":
                    rc2, out2, _ = await _run("du", "--inodes", "-s", mount_point)
                    if rc2 == 0:
                        parts = out2.strip().split()
                        info["inodes_used"] = int(parts[0]) if parts else None
                    else:
                        info["inodes_used"] = None
                    info["inodes_total"] = None  # btrfs has no fixed limit
                    info["inodes_avail"] = None
                else:
                    info["inodes_total"] = inodes_total
                    info["inodes_used"]  = inodes_used
                    info["inodes_avail"] = inodes_avail


def _parse_tune2fs(output: str) -> dict:
    result = {}
    for line in output.splitlines():
        if ":" in line:
            key, _, val = line.partition(":")
            result[key.strip()] = val.strip()
    return result


# ---------------------------------------------------------------------------
# Btrfs helpers
# ---------------------------------------------------------------------------

def _require_btrfs(vg_name: str) -> Path:
    state = _load_state()
    if state.get(vg_name, {}).get("fstype") != "btrfs":
        raise RuntimeError(f"Volume {vg_name!r} is not formatted as btrfs")
    mp = _mount_point(vg_name)
    if not _is_mounted(mp):
        raise RuntimeError(f"Volume {vg_name!r} is not mounted")
    return mp


async def _btrfs_usage(mount_point: str) -> dict:
    rc, out, _ = await _run("btrfs", "filesystem", "usage", "-b", mount_point)
    if rc != 0:
        return {}
    return _parse_btrfs_usage(out)


async def _btrfs_dev_stats(mount_point: str) -> list:
    rc, out, _ = await _run("btrfs", "device", "stats", mount_point)
    if rc != 0:
        return []
    return _parse_btrfs_dev_stats(out)


def _parse_btrfs_usage(output: str) -> dict:
    result: dict = {}
    for line in output.splitlines():
        line = line.strip()
        if ":" in line:
            key, _, val = line.partition(":")
            val = val.strip()
            m = re.match(r"(\d+)", val.replace(",", ""))
            if m:
                result[key.strip().lower().replace(" ", "_").replace("/", "_")] = int(m.group(1))
    return result


def _parse_btrfs_dev_stats(output: str) -> list:
    devices: dict = {}
    for line in output.splitlines():
        m = re.match(r"\[(.+?)\]\.(\w+)\s+(\d+)", line)
        if m:
            dev, stat, val = m.group(1), m.group(2), int(m.group(3))
            devices.setdefault(dev, {})[stat] = val
    return [{"device": dev, **stats} for dev, stats in devices.items()]


# ---------------------------------------------------------------------------
# Btrfs: Subvolumes & Snapshots
# ---------------------------------------------------------------------------

def _list_subvols_sync(mount_point: str) -> list:
    """Run synchronously inside an executor (btrfsutil is a blocking C extension)."""
    _ZERO_UUID = bytes(16)
    default_id = btrfsutil.get_default_subvolume(mount_point)
    subvols = []
    with btrfsutil.SubvolumeIterator(mount_point, info=True) as it:
        for path, info in it:
            subvols.append({
                "id": info.id,
                "parent_id": info.parent_id,
                "path": path,
                "is_snapshot": info.parent_uuid != _ZERO_UUID,
                # default_id == 5 means FS_TREE (no custom default set)
                "is_default": info.id == default_id and default_id != 5,
            })
    return subvols


async def btrfs_list_subvolumes(vg_name: str) -> list:
    mp = _require_btrfs(vg_name)
    if _BTRFSUTIL_AVAILABLE:
        return await asyncio.to_thread(_list_subvols_sync, str(mp))
    # CLI fallback
    rc, out, err = await _run("btrfs", "subvolume", "list", "-p", str(mp))
    if rc != 0:
        raise RuntimeError(f"btrfs subvolume list failed: {err.strip()}")
    # Get default subvolume ID from CLI
    rc_def, out_def, _ = await _run("btrfs", "subvolume", "get-default", str(mp))
    default_id = 5
    if rc_def == 0:
        m_def = re.search(r"ID\s+(\d+)", out_def)
        if m_def:
            default_id = int(m_def.group(1))
    # Get snapshot IDs
    rc2, out2, _ = await _run("btrfs", "subvolume", "list", "-s", str(mp))
    snap_ids: set[int] = set()
    if rc2 == 0:
        for line in out2.strip().splitlines():
            m = re.match(r"ID\s+(\d+)", line.strip())
            if m:
                snap_ids.add(int(m.group(1)))
    subvols = []
    for line in out.strip().splitlines():
        m = re.match(
            r"ID\s+(\d+)\s+gen\s+\d+\s+parent\s+(\d+)\s+top level\s+(\d+)\s+path\s+(.+)",
            line.strip()
        )
        if m:
            sv_id = int(m.group(1))
            subvols.append({
                "id": sv_id,
                "parent_id": int(m.group(2)),
                "path": m.group(4),
                "is_snapshot": sv_id in snap_ids,
                "is_default": sv_id == default_id and default_id != 5,
            })
    return subvols


async def btrfs_create_subvolume(vg_name: str, name: str) -> dict:
    mp = _require_btrfs(vg_name)
    path = str(mp / name)
    if _BTRFSUTIL_AVAILABLE:
        await asyncio.to_thread(btrfsutil.create_subvolume, path)
    else:
        rc, _, err = await _run("btrfs", "subvolume", "create", path)
        if rc != 0:
            raise RuntimeError(f"btrfs subvolume create failed: {err.strip()}")
    return {"created": True, "path": path}


async def btrfs_delete_subvolume(vg_name: str, path: str, recursive: bool = False) -> dict:
    mp = _require_btrfs(vg_name)
    full_path = str(mp / path)
    if _BTRFSUTIL_AVAILABLE:
        await asyncio.to_thread(
            lambda: btrfsutil.delete_subvolume(full_path, recursive=recursive)
        )
    else:
        if recursive:
            # Delete all nested subvolumes first (deepest first)
            rc, out, _ = await _run("btrfs", "subvolume", "list", "-o", str(mp))
            if rc == 0:
                nested = []
                for line in out.strip().splitlines():
                    m = re.search(r"path\s+(.+)", line)
                    if m:
                        nested_path = m.group(1)
                        if nested_path.startswith(path + "/") or nested_path == path:
                            nested.append(str(mp / nested_path))
                for p in sorted(nested, key=len, reverse=True):
                    await _run("btrfs", "subvolume", "delete", p)
        rc, _, err = await _run("btrfs", "subvolume", "delete", full_path)
        if rc != 0:
            raise RuntimeError(f"btrfs subvolume delete failed: {err.strip()}")
    return {"deleted": True, "path": path}


async def btrfs_create_snapshot(
    vg_name: str, source_path: str, dest_path: str, readonly: bool = False
) -> dict:
    mp = _require_btrfs(vg_name)
    src = str(mp / source_path)
    dst = str(mp / dest_path)
    if _BTRFSUTIL_AVAILABLE:
        await asyncio.to_thread(
            lambda: btrfsutil.create_snapshot(src, dst, read_only=readonly)
        )
    else:
        cmd = ["btrfs", "subvolume", "snapshot"]
        if readonly:
            cmd.append("-r")
        cmd += [src, dst]
        rc, _, err = await _run(*cmd)
        if rc != 0:
            raise RuntimeError(f"btrfs snapshot failed: {err.strip()}")
    return {"created": True, "snapshot": dest_path, "readonly": readonly}


async def btrfs_set_default_subvolume(vg_name: str, subvol_id: int) -> dict:
    mp = _require_btrfs(vg_name)
    if _BTRFSUTIL_AVAILABLE:
        await asyncio.to_thread(
            lambda: btrfsutil.set_default_subvolume(str(mp), subvol_id)
        )
    else:
        rc, _, err = await _run(
            "btrfs", "subvolume", "set-default", str(subvol_id), str(mp)
        )
        if rc != 0:
            raise RuntimeError(f"btrfs set-default failed: {err.strip()}")
    return {"default_id": subvol_id}


# ---------------------------------------------------------------------------
# Btrfs: Scrub
# ---------------------------------------------------------------------------

def _parse_scrub_status(output: str) -> dict:
    """Parse the output of 'btrfs scrub status' into a structured dict."""
    result: dict = {}
    for line in output.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip().lower().replace(" ", "_").replace("/", "_")
        val = val.strip()
        if key == "status":
            result["status"] = val
        elif key == "duration":
            result["duration"] = val
            parts = val.split(":")
            try:
                if len(parts) == 3:
                    result["duration_seconds"] = (
                        int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                    )
                elif len(parts) == 2:
                    result["duration_seconds"] = int(parts[0]) * 60 + int(parts[1])
            except (ValueError, IndexError):
                pass
        elif key == "time_left":
            result["time_left"] = val
        elif key == "error_summary":
            result["error_summary"] = val
        elif key in (
            "data_extents_scrubbed", "tree_extents_scrubbed",
            "read_errors", "csum_errors", "verify_errors",
            "corrected_errors", "uncorrectable_errors",
        ):
            try:
                result[key] = int(val)
            except ValueError:
                pass
    if "status" not in result:
        # Could not parse — include raw
        result["status"] = "unknown"
        result["raw"] = output.strip()
    return result


async def btrfs_scrub_start(vg_name: str, update_fn) -> None:
    mp = _require_btrfs(vg_name)
    update_fn("Starting scrub…", 0)
    rc, _, err = await _run("btrfs", "scrub", "start", "-B", str(mp))
    if rc not in (0, 1):   # exit 1 = completed with errors, still want results
        raise RuntimeError(f"btrfs scrub failed: {err.strip()}")
    update_fn("Scrub complete", 100)
    # Persist the final scrub result
    try:
        _, out2, _ = await _run("btrfs", "scrub", "status", str(mp))
        parsed = _parse_scrub_status(out2)
        state = _load_state()
        state.setdefault(vg_name, {})["scrub_last_result"] = {
            "timestamp": time.time(), **parsed,
        }
        _save_state(state)
    except Exception as exc:
        logger.warning("failed to save scrub result for %s: %s", vg_name, exc)


async def btrfs_scrub_status(vg_name: str) -> dict:
    mp = _require_btrfs(vg_name)
    rc, out, _ = await _run("btrfs", "scrub", "status", str(mp))
    parsed = _parse_scrub_status(out)
    return {"status": out.strip(), "parsed": parsed, "rc": rc}


async def btrfs_scrub_pause(vg_name: str) -> dict:
    mp = _require_btrfs(vg_name)
    rc, _, err = await _run("btrfs", "scrub", "pause", str(mp))
    if rc != 0:
        raise RuntimeError(f"btrfs scrub pause failed: {err.strip()}")
    return {"paused": True}


async def btrfs_scrub_resume(vg_name: str) -> dict:
    mp = _require_btrfs(vg_name)
    rc, _, err = await _run("btrfs", "scrub", "resume", str(mp))
    if rc != 0:
        raise RuntimeError(f"btrfs scrub resume failed: {err.strip()}")
    return {"resumed": True}


async def btrfs_scrub_cancel(vg_name: str) -> dict:
    mp = _require_btrfs(vg_name)
    rc, _, err = await _run("btrfs", "scrub", "cancel", str(mp))
    return {"cancelled": rc == 0, "error": err.strip() if rc != 0 else None}


async def btrfs_scrub_last_result(vg_name: str) -> dict:
    """Return the last persisted scrub result from state file."""
    state = _load_state()
    result = state.get(vg_name, {}).get("scrub_last_result")
    if result is None:
        return {"available": False}
    return {"available": True, **result}


# ---------------------------------------------------------------------------
# Btrfs: Balance
# ---------------------------------------------------------------------------

async def btrfs_balance_start(vg_name: str, usage_filter: int | None,
                               metadata_usage: int | None, update_fn) -> None:
    mp = _require_btrfs(vg_name)
    update_fn("Starting balance…", 0)

    cmd = ["btrfs", "balance", "start"]
    filters = []
    if usage_filter is not None:
        filters.append(f"usage={usage_filter}")
    if filters:
        cmd += ["-dconvert=raid5", f"-dfilter={','.join(filters)}"]
        # Simpler: just apply usage filter to data
        cmd = ["btrfs", "balance", "start"]
        if usage_filter is not None:
            cmd += [f"-dusage={usage_filter}"]
        if metadata_usage is not None:
            cmd += [f"-musage={metadata_usage}"]
    cmd.append(str(mp))

    rc, _, err = await _run(*cmd)
    if rc not in (0, 1):
        raise RuntimeError(f"btrfs balance failed: {err.strip()}")
    update_fn("Balance complete", 100)


async def btrfs_balance_status(vg_name: str) -> dict:
    mp = _require_btrfs(vg_name)
    rc, out, _ = await _run("btrfs", "balance", "status", str(mp))
    return {"status": out.strip(), "rc": rc}


async def btrfs_balance_cancel(vg_name: str) -> dict:
    mp = _require_btrfs(vg_name)
    rc, _, err = await _run("btrfs", "balance", "cancel", str(mp))
    return {"cancelled": rc == 0, "error": err.strip() if rc != 0 else None}


# ---------------------------------------------------------------------------
# Btrfs: Defragmentation
# ---------------------------------------------------------------------------

async def btrfs_defrag(vg_name: str, path: str, recursive: bool,
                        compression: str, update_fn) -> None:
    mp = _require_btrfs(vg_name)
    target = str(mp / path) if path else str(mp)
    update_fn("Defragmenting…", 0)
    cmd = ["btrfs", "filesystem", "defragment"]
    if recursive:
        cmd.append("-r")
    if compression and compression != "none":
        cmd += ["-c", compression]
    cmd.append(target)
    rc, _, err = await _run(*cmd)
    if rc != 0:
        raise RuntimeError(f"btrfs defrag failed: {err.strip()}")
    update_fn("Defrag complete", 100)


# ---------------------------------------------------------------------------
# Btrfs: Compression (remount)
# ---------------------------------------------------------------------------

async def btrfs_set_compression(vg_name: str, compression: str) -> dict:
    if compression not in _BTRFS_COMPRESS_ALLOWED:
        raise ValueError(f"Invalid compression: {compression!r}")
    state = _load_state()
    if state.get(vg_name, {}).get("fstype") != "btrfs":
        raise RuntimeError("Not a btrfs volume")
    state[vg_name]["compression"] = compression
    _save_state(state)

    mp = _mount_point(vg_name)
    if _is_mounted(mp):
        # remount with new options
        opts = f"compress={compression}" if compression and compression != "none" else "compress=no"
        rc, _, err = await _run("mount", "-o", f"remount,{opts}", str(mp))
        if rc != 0:
            raise RuntimeError(f"remount failed: {err.strip()}")
    return {"compression": compression}


# ---------------------------------------------------------------------------
# Btrfs: Quotas
# ---------------------------------------------------------------------------

async def btrfs_quota_enable(vg_name: str) -> dict:
    mp = _require_btrfs(vg_name)
    rc, _, err = await _run("btrfs", "quota", "enable", str(mp))
    if rc != 0:
        raise RuntimeError(f"btrfs quota enable failed: {err.strip()}")
    return {"enabled": True}


async def btrfs_quota_list(vg_name: str) -> list:
    mp = _require_btrfs(vg_name)
    rc, out, _ = await _run("btrfs", "qgroup", "show", "--raw", str(mp))
    if rc != 0:
        return []
    return _parse_qgroup_show(out)


async def btrfs_quota_set(vg_name: str, qgroup: str, limit_bytes: int) -> dict:
    mp = _require_btrfs(vg_name)
    rc, _, err = await _run(
        "btrfs", "qgroup", "limit", str(limit_bytes), qgroup, str(mp)
    )
    if rc != 0:
        raise RuntimeError(f"btrfs qgroup limit failed: {err.strip()}")
    return {"qgroup": qgroup, "limit_bytes": limit_bytes}


def _parse_qgroup_show(output: str) -> list:
    result = []
    for line in output.strip().splitlines():
        # qgroupid rfer excl
        parts = line.split()
        if len(parts) >= 3 and re.match(r"\d+/\d+", parts[0]):
            entry = {"qgroupid": parts[0]}
            try:
                entry["referenced_bytes"] = int(parts[1])
                entry["exclusive_bytes"]  = int(parts[2])
                if len(parts) >= 4:
                    entry["limit_bytes"] = None if parts[3] == "none" else int(parts[3])
            except (ValueError, IndexError):
                pass
            result.append(entry)
    return result


# ---------------------------------------------------------------------------
# Btrfs: Send / Receive
# ---------------------------------------------------------------------------

async def btrfs_send(
    vg_name: str, snapshot_path: str, dest_file: str,
    parent_path: str | None, update_fn
) -> dict:
    mp = _require_btrfs(vg_name)
    SEND_DIR.mkdir(parents=True, exist_ok=True)
    src = str(mp / snapshot_path)
    out_path = SEND_DIR / dest_file

    cmd = ["btrfs", "send"]
    if parent_path:
        cmd += ["-p", str(mp / parent_path)]
    cmd.append(src)

    update_fn("Sending btrfs stream…", 0)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    data, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"btrfs send failed: {err.decode(errors='replace').strip()}")

    out_path.write_bytes(data)
    update_fn("Send complete", 100)
    return {"file": str(out_path), "size_bytes": len(data)}


async def btrfs_receive(vg_name: str, source_file: str, update_fn) -> dict:
    mp = _require_btrfs(vg_name)
    src_path = SEND_DIR / source_file
    if not src_path.exists():
        raise RuntimeError(f"Source file not found: {src_path}")

    update_fn("Receiving btrfs stream…", 0)
    data = src_path.read_bytes()
    rc, _, err = await _run("btrfs", "receive", str(mp), input_data=data)
    if rc != 0:
        raise RuntimeError(f"btrfs receive failed: {err.strip()}")
    update_fn("Receive complete", 100)
    return {"received": True, "mount_point": str(mp)}


# ---------------------------------------------------------------------------
# Btrfs: Deduplication (hook for duperemove)
# ---------------------------------------------------------------------------

async def btrfs_dedup(vg_name: str, path: str, update_fn) -> dict:
    mp = _require_btrfs(vg_name)
    target = str(mp / path) if path else str(mp)
    update_fn("Running deduplication…", 0)
    rc, out, err = await _run(
        "duperemove", "-dhr", "--hashfile=/var/lib/traid/duperemove.hash", target
    )
    if rc != 0:
        raise RuntimeError(f"duperemove failed: {err.strip()}")
    update_fn("Dedup complete", 100)
    return {"output": out[-2000:]}   # last 2000 chars of output


# ---------------------------------------------------------------------------
# Btrfs: Usage detail
# ---------------------------------------------------------------------------

async def btrfs_usage_detail(vg_name: str) -> dict:
    mp = _require_btrfs(vg_name)
    rc, out, _ = await _run("btrfs", "filesystem", "usage", "-b", str(mp))
    if rc != 0:
        raise RuntimeError("btrfs filesystem usage failed")
    return {
        "raw": out,
        "parsed": _parse_btrfs_usage(out),
        "device_stats": await _btrfs_dev_stats(str(mp)),
    }
