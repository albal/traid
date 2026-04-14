"""
KVM / Virtual Machine operations via virsh (libvirt).

All functions run as root (called from daemon.py).
ISOs are stored in /var/lib/traid/iso/.
"""

import asyncio
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

ISO_DIR = Path("/var/lib/traid/iso")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _run(*cmd: str) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")


async def _virsh(*args: str) -> tuple[int, str, str]:
    return await _run("virsh", "--connect", "qemu:///system", *args)


# ---------------------------------------------------------------------------
# List / Info
# ---------------------------------------------------------------------------

async def list_vms() -> list:
    rc, out, err = await _virsh("list", "--all", "--name")
    if rc != 0:
        raise RuntimeError(f"virsh list failed: {err.strip()}")
    names = [n.strip() for n in out.strip().splitlines() if n.strip()]
    vms = []
    for name in names:
        try:
            vms.append(await vm_info(name))
        except Exception as exc:
            logger.warning("vm_info(%s) failed: %s", name, exc)
            vms.append({"name": name, "state": "unknown"})
    return vms


async def vm_info(name: str) -> dict:
    rc, out, err = await _virsh("dominfo", name)
    if rc != 0:
        raise RuntimeError(f"virsh dominfo failed: {err.strip()}")

    info: dict = {"name": name}
    for line in out.splitlines():
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip().lower().replace(" ", "_")
        val = val.strip()
        if key == "state":
            info["state"] = val
        elif key == "id":
            info["id"] = val
        elif key == "cpu(s)":
            try:
                info["vcpus"] = int(val)
            except ValueError:
                pass
        elif key == "max_memory":
            info["max_memory_kib"] = val
        elif key == "used_memory":
            info["used_memory_kib"] = val

    # VNC display
    rc2, out2, _ = await _virsh("vncdisplay", name)
    if rc2 == 0:
        m = re.search(r":(\d+)", out2)
        if m:
            info["vnc_port"] = 5900 + int(m.group(1))

    return info


async def list_isos() -> list:
    ISO_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(p.name for p in ISO_DIR.glob("*.iso"))


# ---------------------------------------------------------------------------
# Actions (start / shutdown / pause / resume / delete)
# ---------------------------------------------------------------------------

_VIRSH_ACTION_MAP = {
    "start":    "start",
    "shutdown": "shutdown",
    "destroy":  "destroy",   # force-off
    "suspend":  "suspend",
    "resume":   "resume",
}


async def vm_action(name: str, action: str) -> dict:
    virsh_cmd = _VIRSH_ACTION_MAP.get(action)
    if not virsh_cmd:
        raise ValueError(f"Unknown VM action: {action!r}")
    rc, _, err = await _virsh(virsh_cmd, name)
    if rc != 0:
        raise RuntimeError(f"virsh {virsh_cmd} {name!r} failed: {err.strip()}")
    return {"name": name, "action": action, "ok": True}


async def vm_delete(name: str, keep_storage: bool = False) -> dict:
    cmd = ["undefine", name]
    if not keep_storage:
        cmd.append("--remove-all-storage")
    rc, _, err = await _virsh(*cmd)
    if rc != 0:
        raise RuntimeError(f"virsh undefine {name!r} failed: {err.strip()}")
    return {"name": name, "deleted": True}


# ---------------------------------------------------------------------------
# Create VM
# ---------------------------------------------------------------------------

async def create_vm(
    name: str,
    iso: str,
    ram_mb: int,
    vcpus: int,
    disk_gb: int,
    update_fn,
) -> None:
    ISO_DIR.mkdir(parents=True, exist_ok=True)
    iso_path = ISO_DIR / iso
    if not iso_path.exists():
        raise RuntimeError(f"ISO not found: {iso_path}")

    disk_path = f"/var/lib/libvirt/images/{name}.qcow2"

    update_fn("Creating virtual disk…", 10)
    rc, _, err = await _run(
        "qemu-img", "create", "-f", "qcow2", disk_path, f"{disk_gb}G"
    )
    if rc != 0:
        raise RuntimeError(f"qemu-img create failed: {err.strip()}")

    update_fn("Installing VM…", 30)
    rc, _, err = await _run(
        "virt-install",
        "--connect", "qemu:///system",
        "--name", name,
        "--memory", str(ram_mb),
        "--vcpus", str(vcpus),
        "--disk", f"path={disk_path},format=qcow2",
        "--cdrom", str(iso_path),
        "--os-variant", "detect=on,require=off",
        "--network", "default",
        "--graphics", "vnc",
        "--noautoconsole",
        "--import" if iso_path.suffix == "" else "--wait=-1",
    )
    if rc != 0:
        raise RuntimeError(f"virt-install failed: {err.strip()}")
    update_fn("VM created", 100)
