"""
File sharing management — NFS exports and Samba shares.

Modifies /etc/exports (NFS) and /etc/samba/smb.conf (Samba) directly as root,
then reloads the respective daemon.

All functions run as root (called from daemon.py).
"""

import asyncio
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

EXPORTS_FILE  = Path("/etc/exports")
SMB_CONF_FILE = Path("/etc/samba/smb.conf")

# Header comment written when TRAID first creates /etc/exports from scratch
_EXPORTS_HEADER = "# /etc/exports — managed by TRAID\n"


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


def _atomic_write(path: Path, content: str) -> None:
    tmp = path.with_suffix(".traid-tmp")
    tmp.write_text(content)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# NFS
# ---------------------------------------------------------------------------

def _parse_exports(text: str) -> list:
    exports = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split(None, 1)
        if not parts:
            continue
        path = parts[0]
        clients_opts = parts[1] if len(parts) > 1 else ""
        exports.append({
            "path":    path,
            "clients": clients_opts,
        })
    return exports


async def nfs_list_exports() -> list:
    if not EXPORTS_FILE.exists():
        return []
    return _parse_exports(EXPORTS_FILE.read_text())


async def nfs_add_export(path: str, clients: str, options: str) -> dict:
    if not EXPORTS_FILE.exists():
        EXPORTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        existing = _EXPORTS_HEADER
    else:
        existing = EXPORTS_FILE.read_text()

    # Don't duplicate — remove any existing entry for this path first
    lines = [
        ln for ln in existing.splitlines()
        if not (ln.strip() and not ln.startswith("#") and ln.split()[0] == path)
    ]

    # Format: /path  clients(options)
    if options:
        entry = f"{path}\t{clients}({options})"
    else:
        entry = f"{path}\t{clients}"
    lines.append(entry)
    _atomic_write(EXPORTS_FILE, "\n".join(lines) + "\n")

    rc, _, err = await _run("exportfs", "-ra")
    if rc != 0:
        logger.warning("exportfs -ra: %s", err.strip())

    return {"added": True, "path": path}


async def nfs_remove_export(path: str) -> dict:
    if not EXPORTS_FILE.exists():
        return {"removed": False, "reason": "not found"}
    existing = EXPORTS_FILE.read_text()
    lines = [
        ln for ln in existing.splitlines()
        if not (ln.strip() and not ln.startswith("#") and ln.split()[0] == path)
    ]
    _atomic_write(EXPORTS_FILE, "\n".join(lines) + "\n")

    rc, _, err = await _run("exportfs", "-ra")
    if rc != 0:
        logger.warning("exportfs -ra: %s", err.strip())

    return {"removed": True, "path": path}


# ---------------------------------------------------------------------------
# Samba
# ---------------------------------------------------------------------------

def _parse_smb_conf(text: str) -> dict[str, dict]:
    """
    Parse smb.conf into {section_name: {key: value}} dict.
    Handles multi-value lines separated by '='.
    """
    sections: dict[str, dict] = {}
    current = None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            current = line[1:-1]
            sections[current] = {}
        elif "=" in line and current is not None:
            key, _, val = line.partition("=")
            sections[current][key.strip()] = val.strip()
    return sections


def _render_smb_conf(sections: dict[str, dict]) -> str:
    lines = []
    for section, kvs in sections.items():
        lines.append(f"[{section}]")
        for k, v in kvs.items():
            lines.append(f"    {k} = {v}")
        lines.append("")
    return "\n".join(lines)


async def samba_list_shares() -> list:
    if not SMB_CONF_FILE.exists():
        return []
    sections = _parse_smb_conf(SMB_CONF_FILE.read_text())
    shares = []
    for name, kvs in sections.items():
        if name.lower() in ("global", "homes", "printers"):
            continue
        shares.append({
            "name":     name,
            "path":     kvs.get("path", ""),
            "comment":  kvs.get("comment", ""),
            "public":   kvs.get("guest ok", "no").lower() in ("yes", "true"),
            "writable": kvs.get("writable", kvs.get("read only", "no")).lower() in ("yes", "true"),
        })
    return shares


async def samba_add_share(
    name: str, path: str, comment: str = "",
    public: bool = False, writable: bool = True,
) -> dict:
    if SMB_CONF_FILE.exists():
        sections = _parse_smb_conf(SMB_CONF_FILE.read_text())
    else:
        # Bootstrap a minimal smb.conf
        sections = {"global": {
            "workgroup": "WORKGROUP",
            "server string": "TRAID File Server",
            "security": "user",
            "map to guest": "bad user",
        }}

    sections[name] = {
        "path":      path,
        "comment":   comment,
        "writable":  "yes" if writable else "no",
        "guest ok":  "yes" if public else "no",
        "create mask":    "0664",
        "directory mask": "0775",
    }
    SMB_CONF_FILE.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(SMB_CONF_FILE, _render_smb_conf(sections))

    rc, _, err = await _run("systemctl", "reload", "smbd")
    if rc != 0:
        logger.warning("systemctl reload smbd: %s", err.strip())

    return {"added": True, "name": name}


async def samba_remove_share(name: str) -> dict:
    if not SMB_CONF_FILE.exists():
        return {"removed": False, "reason": "not found"}
    sections = _parse_smb_conf(SMB_CONF_FILE.read_text())
    if name not in sections:
        return {"removed": False, "reason": "share not found"}
    del sections[name]
    _atomic_write(SMB_CONF_FILE, _render_smb_conf(sections))

    rc, _, err = await _run("systemctl", "reload", "smbd")
    if rc != 0:
        logger.warning("systemctl reload smbd: %s", err.strip())

    return {"removed": True, "name": name}
