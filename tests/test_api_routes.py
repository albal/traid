"""
API route tests using a mocked worker (no running daemon required).

monkeypatches api.uds_client.send_request so the full FastAPI stack
is exercised without a real Unix socket.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.main import app

FIXTURES = Path(__file__).parent / "fixtures"


def _load_lsblk() -> dict:
    raw = json.loads((FIXTURES / "lsblk_output.json").read_text())
    # Convert to the format disk_ops.scan_disks() returns
    disks = []
    for dev in raw["blockdevices"]:
        if dev["type"] != "disk":
            continue
        # OS disk detection: check children for mounted partitions
        children = dev.get("children") or []
        is_os = any(
            c.get("mountpoint") in {"/", "/boot", "/boot/efi", "/efi"}
            for c in children
        )
        disks.append({
            "name": f"/dev/{dev['name']}",
            "size": int(dev["size"]),
            "type": dev["type"],
            "mountpoint": dev.get("mountpoint"),
            "fstype": dev.get("fstype"),
            "available": not is_os and not dev.get("mountpoint"),
        })
    return {"disks": disks}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_worker():
    lsblk_data = _load_lsblk()

    async def fake_send(action, params=None, timeout=30.0):
        if action == "disk_scan":
            return lsblk_data
        if action == "lvm_report":
            return {"pvs": [], "vgs": []}
        if action == "array_create":
            return {"accepted": True, "job_id": "test-job-1234"}
        return {}

    with patch("api.uds_client.send_request", side_effect=fake_send):
        yield


@pytest.fixture
async def client(mock_worker):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# GET /api/disks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_disks_returns_list(client):
    resp = await client.get("/api/disks")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) > 0


@pytest.mark.asyncio
async def test_get_disks_has_required_fields(client):
    resp = await client.get("/api/disks")
    disk = resp.json()[0]
    assert "name" in disk
    assert "size" in disk
    assert "available" in disk


@pytest.mark.asyncio
async def test_os_disk_marked_unavailable(client):
    resp = await client.get("/api/disks")
    disks = {d["name"]: d for d in resp.json()}
    # sda has / and /boot/efi mounted — should not be available
    assert disks["/dev/sda"]["available"] is False


@pytest.mark.asyncio
async def test_bare_disks_marked_available(client):
    resp = await client.get("/api/disks")
    disks = {d["name"]: d for d in resp.json()}
    assert disks["/dev/sdb"]["available"] is True
    assert disks["/dev/sdc"]["available"] is True


# ---------------------------------------------------------------------------
# GET /api/volumes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_volumes(client):
    resp = await client.get("/api/volumes")
    assert resp.status_code == 200
    data = resp.json()
    assert "pvs" in data
    assert "vgs" in data


# ---------------------------------------------------------------------------
# GET /api/preview
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_preview_valid(client):
    resp = await client.get("/api/preview", params={
        "disks": ["/dev/sdb", "/dev/sdc", "/dev/sdd"],
        "type": "traid1",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "usable_bytes" in data
    assert "raid_groups" in data
    assert data["usable_bytes"] > 0


@pytest.mark.asyncio
async def test_preview_invalid_type(client):
    resp = await client.get("/api/preview", params={
        "disks": ["/dev/sdb", "/dev/sdc"],
        "type": "shr1",
    })
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_preview_unknown_disk(client):
    resp = await client.get("/api/preview", params={
        "disks": ["/dev/sdz"],
        "type": "traid1",
    })
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/create
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_returns_202(client):
    resp = await client.post("/api/create", json={
        "disks": ["/dev/sdb", "/dev/sdc", "/dev/sdd"],
        "type": "traid1",
    })
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] is True
    assert "job_id" in data


@pytest.mark.asyncio
async def test_create_invalid_type_rejected(client):
    resp = await client.post("/api/create", json={
        "disks": ["/dev/sdb", "/dev/sdc"],
        "type": "shr1",
    })
    assert resp.status_code == 422  # Pydantic validation


@pytest.mark.asyncio
async def test_create_empty_disks_rejected(client):
    resp = await client.post("/api/create", json={
        "disks": [],
        "type": "traid1",
    })
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Worker unavailable
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_unavailable_returns_503():
    from api.uds_client import WorkerUnavailableError

    async def unavailable(*args, **kwargs):
        raise WorkerUnavailableError("socket not found")

    with patch("api.uds_client.send_request", side_effect=unavailable):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/disks")
    assert resp.status_code == 503
