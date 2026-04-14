"""
Docker container / image management via the Docker CLI.

All functions run as root (called from daemon.py).
Uses 'docker' CLI which connects to /var/run/docker.sock — no Python SDK needed.
"""

import asyncio
import json
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
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


async def _docker(*args: str) -> tuple[int, str, str]:
    return await _run("docker", *args)


def _parse_json_stream(output: str) -> list:
    """Parse newline-delimited JSON (one object per line)."""
    results = []
    for line in output.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            results.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return results


# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------

async def list_containers(all_containers: bool = True) -> list:
    cmd = ["ps", "--format", "json"]
    if all_containers:
        cmd.append("--all")
    rc, out, err = await _docker(*cmd)
    if rc != 0:
        raise RuntimeError(f"docker ps failed: {err.strip()}")
    raw = _parse_json_stream(out)
    result = []
    for c in raw:
        result.append({
            "id":      (c.get("ID") or c.get("id", ""))[:12],
            "name":    c.get("Names") or c.get("name", ""),
            "image":   c.get("Image") or c.get("image", ""),
            "status":  c.get("Status") or c.get("status", ""),
            "state":   c.get("State") or c.get("state", ""),
            "ports":   c.get("Ports") or c.get("ports", ""),
            "created": c.get("CreatedAt") or c.get("created", ""),
        })
    return result


async def container_action(container_id: str, action: str) -> dict:
    rc, _, err = await _docker(action, container_id)
    if rc != 0:
        raise RuntimeError(f"docker {action} {container_id!r} failed: {err.strip()}")
    return {"container_id": container_id, "action": action, "ok": True}


async def container_logs(container_id: str, lines: int = 200) -> dict:
    rc, out, err = await _docker("logs", "--tail", str(lines), container_id)
    # docker logs writes to stderr even on success for some images
    combined = out + err
    if rc != 0 and not combined.strip():
        raise RuntimeError(f"docker logs failed: {err.strip()}")
    return {"container_id": container_id, "logs": combined}


# ---------------------------------------------------------------------------
# Images
# ---------------------------------------------------------------------------

async def list_images() -> list:
    rc, out, err = await _docker("images", "--format", "json")
    if rc != 0:
        raise RuntimeError(f"docker images failed: {err.strip()}")
    raw = _parse_json_stream(out)
    result = []
    for img in raw:
        result.append({
            "id":         (img.get("ID") or img.get("id", ""))[:12],
            "repository": img.get("Repository") or img.get("repository", ""),
            "tag":        img.get("Tag") or img.get("tag", ""),
            "size":       img.get("Size") or img.get("size", ""),
            "created":    img.get("CreatedAt") or img.get("created", ""),
        })
    return result


async def pull_image(image: str, update_fn) -> dict:
    update_fn(f"Pulling {image}…", 0)
    rc, out, err = await _docker("pull", image)
    if rc != 0:
        raise RuntimeError(f"docker pull {image!r} failed: {err.strip()}")
    update_fn("Pull complete", 100)
    return {"image": image, "output": out.strip()[-1000:]}


async def remove_image(image_id: str, force: bool = False) -> dict:
    cmd = ["rmi"]
    if force:
        cmd.append("--force")
    cmd.append(image_id)
    rc, _, err = await _docker(*cmd)
    if rc != 0:
        raise RuntimeError(f"docker rmi {image_id!r} failed: {err.strip()}")
    return {"image_id": image_id, "removed": True}


# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------

async def system_prune(update_fn) -> dict:
    update_fn("Pruning unused Docker resources…", 0)
    rc, out, err = await _docker("system", "prune", "-f")
    if rc != 0:
        raise RuntimeError(f"docker system prune failed: {err.strip()}")
    update_fn("Prune complete", 100)
    return {"output": out.strip()[-2000:]}
