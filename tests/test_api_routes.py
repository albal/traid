"""
API route tests using a mocked worker (no running daemon required).

monkeypatches api.uds_client.send_request so the full FastAPI stack
is exercised without a real Unix socket.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.main import app, REPORT_DIR
from api.uds_client import WorkerError, WorkerUnavailableError

FIXTURES = Path(__file__).parent / "fixtures"

_FAKE_JOB_ID = "aabbccdd-1234-5678-abcd-000000000001"
_FAKE_JOB_ID_2 = "aabbccdd-1234-5678-abcd-000000000002"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_lsblk() -> dict:
    raw = json.loads((FIXTURES / "lsblk_output.json").read_text())
    disks = []
    for dev in raw["blockdevices"]:
        if dev["type"] != "disk":
            continue
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


def _accepted(job_id: str = _FAKE_JOB_ID) -> dict:
    return {"accepted": True, "job_id": job_id}


def _busy() -> dict:
    return {"accepted": False, "reason": "ARRAY_BUSY", "message": "busy"}


# ---------------------------------------------------------------------------
# Shared mock fixture
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
            return _accepted()
        if action == "smart_test":
            return _accepted()
        if action == "badblocks_test":
            return _accepted()
        if action == "disk_erase":
            return _accepted()
        if action == "array_migrate":
            return _accepted()
        if action == "disk_replace":
            return _accepted()
        if action == "array_grow":
            return _accepted()
        if action == "array_shrink":
            return _accepted()
        if action == "volume_clone":
            return _accepted()
        if action == "volume_backup":
            return _accepted()
        if action == "vg_rename":
            return {"renamed": True}
        if action == "array_delete":
            return {"accepted": True, "job_id": _FAKE_JOB_ID}
        if action == "jobs_list":
            return {"jobs": []}
        if action == "job_delete":
            return {"cancelled": True, "job_id": params.get("job_id")}
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
    assert isinstance(resp.json(), list)
    assert len(resp.json()) > 0


@pytest.mark.asyncio
async def test_get_disks_has_required_fields(client):
    resp = await client.get("/api/disks")
    disk = resp.json()[0]
    for field in ("name", "size", "available"):
        assert field in disk


@pytest.mark.asyncio
async def test_os_disk_marked_unavailable(client):
    resp = await client.get("/api/disks")
    disks = {d["name"]: d for d in resp.json()}
    assert disks["/dev/sda"]["available"] is False


@pytest.mark.asyncio
async def test_bare_disks_marked_available(client):
    resp = await client.get("/api/disks")
    disks = {d["name"]: d for d in resp.json()}
    assert disks["/dev/sdb"]["available"] is True
    assert disks["/dev/sdc"]["available"] is True


@pytest.mark.asyncio
async def test_get_disks_worker_unavailable():
    async def unavailable(*a, **kw):
        raise WorkerUnavailableError("no socket")
    with patch("api.uds_client.send_request", side_effect=unavailable):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/disks")
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_get_disks_worker_error_500():
    async def fail(*a, **kw):
        raise WorkerError("SUBPROCESS_FAILED", "lsblk failed")
    with patch("api.uds_client.send_request", side_effect=fail):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/disks")
    assert resp.status_code == 500


# ---------------------------------------------------------------------------
# POST /api/disks/smart
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_smart_short_accepted(client):
    resp = await client.post("/api/disks/smart", json={"disk": "/dev/sdb", "test_type": "short"})
    assert resp.status_code == 202
    assert resp.json()["accepted"] is True
    assert "job_id" in resp.json()


@pytest.mark.asyncio
async def test_smart_long_accepted(client):
    resp = await client.post("/api/disks/smart", json={"disk": "/dev/nvme0n1", "test_type": "long"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_smart_invalid_type_rejected(client):
    resp = await client.post("/api/disks/smart", json={"disk": "/dev/sdb", "test_type": "full"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_smart_invalid_disk_rejected(client):
    resp = await client.post("/api/disks/smart", json={"disk": "sdb", "test_type": "short"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_smart_worker_unavailable():
    async def unavailable(*a, **kw):
        raise WorkerUnavailableError("no socket")
    with patch("api.uds_client.send_request", side_effect=unavailable):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/disks/smart", json={"disk": "/dev/sdb", "test_type": "short"})
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# POST /api/disks/badblocks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_badblocks_accepted(client):
    resp = await client.post("/api/disks/badblocks", json={"disk": "/dev/sdb"})
    assert resp.status_code == 202
    assert resp.json()["accepted"] is True


@pytest.mark.asyncio
async def test_badblocks_invalid_disk_rejected(client):
    resp = await client.post("/api/disks/badblocks", json={"disk": "/etc/passwd"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_badblocks_busy_returns_409(client):
    async def busy(*a, **kw):
        return _busy()
    with patch("api.uds_client.send_request", side_effect=busy):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/disks/badblocks", json={"disk": "/dev/sdb"})
    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# POST /api/disks/erase
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_erase_default_mode_accepted(client):
    resp = await client.post("/api/disks/erase", json={"disk": "/dev/sdb"})
    assert resp.status_code == 202
    assert resp.json()["accepted"] is True


@pytest.mark.asyncio
async def test_erase_quick_mode(client):
    resp = await client.post("/api/disks/erase", json={"disk": "/dev/sdb", "mode": "quick"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_erase_dod_short_mode(client):
    resp = await client.post("/api/disks/erase", json={"disk": "/dev/sdb", "mode": "dod_short"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_erase_dod_7_mode(client):
    resp = await client.post("/api/disks/erase", json={"disk": "/dev/sdb", "mode": "dod_7"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_erase_invalid_mode_rejected(client):
    resp = await client.post("/api/disks/erase", json={"disk": "/dev/sdb", "mode": "gutmann"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_erase_mode_forwarded_to_worker():
    """Verify the 'mode' param is passed through to the worker."""
    captured = {}

    async def capture(action, params=None, timeout=30.0):
        captured.update(params or {})
        return _accepted()

    with patch("api.uds_client.send_request", side_effect=capture):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            await c.post("/api/disks/erase", json={"disk": "/dev/sdb", "mode": "quick"})

    assert captured.get("mode") == "quick"
    assert captured.get("disk") == "/dev/sdb"


@pytest.mark.asyncio
async def test_erase_dev_null_rejected(client):
    resp = await client.post("/api/disks/erase", json={"disk": "/dev/null"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_erase_busy_returns_409(client):
    async def busy(*a, **kw):
        return _busy()
    with patch("api.uds_client.send_request", side_effect=busy):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/disks/erase", json={"disk": "/dev/sdb"})
    assert resp.status_code == 409


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


@pytest.mark.asyncio
async def test_get_volumes_worker_unavailable():
    async def unavailable(*a, **kw):
        raise WorkerUnavailableError("no socket")
    with patch("api.uds_client.send_request", side_effect=unavailable):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/volumes")
    assert resp.status_code == 503


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
async def test_preview_traid2(client):
    resp = await client.get("/api/preview", params={
        "disks": ["/dev/sdb", "/dev/sdc", "/dev/sdd", "/dev/sde"],
        "type": "traid2",
    })
    assert resp.status_code == 200


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


@pytest.mark.asyncio
async def test_preview_has_raid_group_fields(client):
    resp = await client.get("/api/preview", params={
        "disks": ["/dev/sdb", "/dev/sdc", "/dev/sdd"],
        "type": "traid1",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["raid_groups"]) >= 1
    rg = data["raid_groups"][0]
    assert "level" in rg
    assert "usable_bytes" in rg


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
    assert resp.json()["accepted"] is True
    assert "job_id" in resp.json()


@pytest.mark.asyncio
async def test_create_traid2(client):
    resp = await client.post("/api/create", json={
        "disks": ["/dev/sdb", "/dev/sdc", "/dev/sdd", "/dev/sde"],
        "type": "traid2",
    })
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_create_custom_vg_name(client):
    resp = await client.post("/api/create", json={
        "disks": ["/dev/sdb", "/dev/sdc"],
        "type": "traid1",
        "vg_name": "mystore",
    })
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_create_invalid_type_rejected(client):
    resp = await client.post("/api/create", json={
        "disks": ["/dev/sdb", "/dev/sdc"],
        "type": "shr1",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_empty_disks_rejected(client):
    resp = await client.post("/api/create", json={
        "disks": [],
        "type": "traid1",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_busy_returns_409():
    async def busy(*a, **kw):
        return {"accepted": False}
    with patch("api.uds_client.send_request", side_effect=busy):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/create", json={
                "disks": ["/dev/sdb", "/dev/sdc"],
                "type": "traid1",
            })
    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# POST /api/volumes/{vg_name}/migrate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_migrate_traid1_to_traid2(client):
    resp = await client.post("/api/volumes/traid_vg/migrate",
                             json={"direction": "traid1_to_traid2"})
    assert resp.status_code == 202
    assert resp.json()["accepted"] is True


@pytest.mark.asyncio
async def test_migrate_traid2_to_traid1(client):
    resp = await client.post("/api/volumes/traid_vg/migrate",
                             json={"direction": "traid2_to_traid1"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_migrate_with_new_disk(client):
    resp = await client.post("/api/volumes/traid_vg/migrate",
                             json={"direction": "traid1_to_traid2", "new_disk": "/dev/sde"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_migrate_invalid_direction_rejected(client):
    resp = await client.post("/api/volumes/traid_vg/migrate",
                             json={"direction": "up"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_migrate_invalid_vg_name(client):
    resp = await client.post("/api/volumes/0badname/migrate",
                             json={"direction": "traid1_to_traid2"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_migrate_direction_forwarded():
    captured = {}

    async def capture(action, params=None, timeout=30.0):
        captured.update(params or {})
        return _accepted()

    with patch("api.uds_client.send_request", side_effect=capture):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            await c.post("/api/volumes/traid_vg/migrate",
                         json={"direction": "traid2_to_traid1"})

    assert captured.get("direction") == "traid2_to_traid1"
    assert captured.get("vg_name") == "traid_vg"


@pytest.mark.asyncio
async def test_migrate_busy_returns_409(client):
    async def busy(*a, **kw):
        return _busy()
    with patch("api.uds_client.send_request", side_effect=busy):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/volumes/traid_vg/migrate",
                                json={"direction": "traid1_to_traid2"})
    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# POST /api/volumes/{vg_name}/replace
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replace_valid(client):
    resp = await client.post("/api/volumes/traid_vg/replace",
                             json={"old_disk": "/dev/sdb", "new_disk": "/dev/sde"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_replace_invalid_disk_rejected(client):
    resp = await client.post("/api/volumes/traid_vg/replace",
                             json={"old_disk": "sdb", "new_disk": "/dev/sde"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_replace_params_forwarded():
    captured = {}

    async def capture(action, params=None, timeout=30.0):
        captured.update(params or {})
        return _accepted()

    with patch("api.uds_client.send_request", side_effect=capture):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            await c.post("/api/volumes/traid_vg/replace",
                         json={"old_disk": "/dev/sdb", "new_disk": "/dev/sde"})

    assert captured.get("vg_name") == "traid_vg"
    assert captured.get("old_disk") == "/dev/sdb"
    assert captured.get("new_disk") == "/dev/sde"


# ---------------------------------------------------------------------------
# POST /api/volumes/{vg_name}/grow
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_grow_valid(client):
    resp = await client.post("/api/volumes/traid_vg/grow",
                             json={"new_disk": "/dev/sdf"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_grow_nvme(client):
    resp = await client.post("/api/volumes/traid_vg/grow",
                             json={"new_disk": "/dev/nvme0n1"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_grow_invalid_disk_rejected(client):
    resp = await client.post("/api/volumes/traid_vg/grow",
                             json={"new_disk": "sdf"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_grow_invalid_vg_name(client):
    resp = await client.post("/api/volumes/bad/name/grow",
                             json={"new_disk": "/dev/sdf"})
    assert resp.status_code in (400, 404, 405)  # routing may reject before vg check


# ---------------------------------------------------------------------------
# POST /api/volumes/{vg_name}/shrink
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shrink_valid(client):
    resp = await client.post("/api/volumes/traid_vg/shrink",
                             json={"disk_to_remove": "/dev/sdb"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_shrink_md_device_rejected(client):
    resp = await client.post("/api/volumes/traid_vg/shrink",
                             json={"disk_to_remove": "/dev/md0"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_shrink_params_forwarded():
    captured = {}

    async def capture(action, params=None, timeout=30.0):
        captured.update(params or {})
        return _accepted()

    with patch("api.uds_client.send_request", side_effect=capture):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            await c.post("/api/volumes/traid_vg/shrink",
                         json={"disk_to_remove": "/dev/sdb"})

    assert captured.get("vg_name") == "traid_vg"
    assert captured.get("disk_to_remove") == "/dev/sdb"


# ---------------------------------------------------------------------------
# POST /api/volumes/{vg_name}/clone
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_clone_valid(client):
    resp = await client.post("/api/volumes/traid_vg/clone",
                             json={"target_disk": "/dev/sdf"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_clone_virtio_disk(client):
    resp = await client.post("/api/volumes/traid_vg/clone",
                             json={"target_disk": "/dev/vdf"})
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_clone_invalid_disk_rejected(client):
    resp = await client.post("/api/volumes/traid_vg/clone",
                             json={"target_disk": "sdf"})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/volumes/{vg_name}/backup
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backup_nfs(client):
    resp = await client.post("/api/volumes/traid_vg/backup", json={
        "protocol": "nfs",
        "host": "192.168.1.10",
        "remote_path": "192.168.1.10:/backup",
    })
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_backup_cifs_with_creds(client):
    resp = await client.post("/api/volumes/traid_vg/backup", json={
        "protocol": "cifs",
        "host": "nas.local",
        "remote_path": "//nas.local/backup",
        "cifs_user": "admin",
        "cifs_pass": "s3cret",
    })
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_backup_invalid_protocol_rejected(client):
    resp = await client.post("/api/volumes/traid_vg/backup", json={
        "protocol": "ftp",
        "host": "192.168.1.10",
        "remote_path": "192.168.1.10:/backup",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_backup_host_injection_rejected(client):
    resp = await client.post("/api/volumes/traid_vg/backup", json={
        "protocol": "nfs",
        "host": "host; rm -rf /",
        "remote_path": "host:/path",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_backup_params_forwarded():
    captured = {}

    async def capture(action, params=None, timeout=30.0):
        captured.update(params or {})
        return _accepted()

    with patch("api.uds_client.send_request", side_effect=capture):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            await c.post("/api/volumes/traid_vg/backup", json={
                "protocol": "nfs",
                "host": "nas.local",
                "remote_path": "nas.local:/backup",
            })

    assert captured.get("vg_name") == "traid_vg"
    assert captured.get("protocol") == "nfs"
    assert captured.get("host") == "nas.local"


# ---------------------------------------------------------------------------
# PATCH /api/volumes/{vg_name} — rename
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rename_valid(client):
    resp = await client.patch("/api/volumes/traid_vg", json={"new_name": "mystore"})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_rename_invalid_new_name_rejected(client):
    resp = await client.patch("/api/volumes/traid_vg", json={"new_name": "0bad"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_rename_slash_rejected(client):
    resp = await client.patch("/api/volumes/traid_vg", json={"new_name": "bad/name"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_rename_params_forwarded():
    captured = {}

    async def capture(action, params=None, timeout=30.0):
        captured.update(params or {})
        return {"renamed": True}

    with patch("api.uds_client.send_request", side_effect=capture):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            await c.patch("/api/volumes/traid_vg", json={"new_name": "mystore"})

    assert captured.get("vg_name") == "traid_vg"
    assert captured.get("new_name") == "mystore"


@pytest.mark.asyncio
async def test_rename_invalid_vg_name(client):
    resp = await client.patch("/api/volumes/0bad", json={"new_name": "good"})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# DELETE /api/volumes/{vg_name}
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_volume_returns_202(client):
    resp = await client.delete("/api/volumes/traid_vg")
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_delete_volume_returns_job(client):
    resp = await client.delete("/api/volumes/traid_vg")
    data = resp.json()
    assert "job_id" in data


@pytest.mark.asyncio
async def test_delete_invalid_vg_name(client):
    resp = await client.delete("/api/volumes/0invalid")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_delete_volume_worker_unavailable():
    async def unavailable(*a, **kw):
        raise WorkerUnavailableError("no socket")
    with patch("api.uds_client.send_request", side_effect=unavailable):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.delete("/api/volumes/traid_vg")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# GET /api/jobs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_jobs_empty(client):
    resp = await client.get("/api/jobs")
    assert resp.status_code == 200
    assert "jobs" in resp.json()


@pytest.mark.asyncio
async def test_list_jobs_with_entries():
    async def with_jobs(*a, **kw):
        return {"jobs": [
            {"job_id": _FAKE_JOB_ID, "action": "disk_erase", "status": "running",
             "step": "Erasing…", "progress_pct": 42, "report_url": None},
        ]}
    with patch("api.uds_client.send_request", side_effect=with_jobs):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/jobs")
    assert resp.status_code == 200
    jobs = resp.json()["jobs"]
    assert len(jobs) == 1
    assert jobs[0]["job_id"] == _FAKE_JOB_ID


@pytest.mark.asyncio
async def test_list_jobs_worker_unavailable():
    async def unavailable(*a, **kw):
        raise WorkerUnavailableError("no socket")
    with patch("api.uds_client.send_request", side_effect=unavailable):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/jobs")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# DELETE /api/jobs/{job_id}
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_job_valid(client):
    resp = await client.delete(f"/api/jobs/{_FAKE_JOB_ID}")
    assert resp.status_code == 200
    assert resp.json()["cancelled"] is True


@pytest.mark.asyncio
async def test_cancel_job_invalid_id_rejected(client):
    resp = await client.delete("/api/jobs/not-a-uuid")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_cancel_job_invalid_id_with_slash(client):
    resp = await client.delete("/api/jobs/../../etc/passwd")
    assert resp.status_code in (400, 404, 405)


@pytest.mark.asyncio
async def test_cancel_job_id_forwarded():
    captured = {}

    async def capture(action, params=None, timeout=30.0):
        captured["action"] = action
        captured.update(params or {})
        return {"cancelled": True, "job_id": params.get("job_id")}

    with patch("api.uds_client.send_request", side_effect=capture):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            await c.delete(f"/api/jobs/{_FAKE_JOB_ID}")

    assert captured.get("action") == "job_delete"
    assert captured.get("job_id") == _FAKE_JOB_ID


# ---------------------------------------------------------------------------
# GET /api/reports/{filename}
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_report_dir(tmp_path, monkeypatch):
    """Redirect REPORT_DIR to a writable temp directory for tests."""
    import api.main as main_mod
    monkeypatch.setattr(main_mod, "REPORT_DIR", tmp_path)
    return tmp_path


def _make_report(report_dir: Path, name: str, content: str = "REPORT DATA\n") -> Path:
    """Write a report file into the given directory."""
    path = report_dir / name
    path.write_text(content)
    return path


@pytest.mark.asyncio
async def test_report_inline_content_disposition(mock_worker, tmp_report_dir):
    fname = f"{_FAKE_JOB_ID}_smart_short.txt"
    _make_report(tmp_report_dir, fname, "SMART PASS\n")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(f"/api/reports/{fname}")
    assert resp.status_code == 200
    assert "inline" in resp.headers.get("content-disposition", "")
    assert "attachment" not in resp.headers.get("content-disposition", "")


@pytest.mark.asyncio
async def test_report_download_content_disposition(mock_worker, tmp_report_dir):
    fname = f"{_FAKE_JOB_ID}_erase.txt"
    _make_report(tmp_report_dir, fname, "ERASE LOG\n")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(f"/api/reports/{fname}", params={"download": "true"})
    assert resp.status_code == 200
    cd = resp.headers.get("content-disposition", "")
    assert "attachment" in cd
    assert fname in cd


@pytest.mark.asyncio
async def test_report_content_returned(mock_worker, tmp_report_dir):
    fname = f"{_FAKE_JOB_ID}_badblocks.txt"
    _make_report(tmp_report_dir, fname, "0 bad blocks found\n")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(f"/api/reports/{fname}")
    assert resp.status_code == 200
    assert "0 bad blocks found" in resp.text


@pytest.mark.asyncio
async def test_report_media_type_text_plain(mock_worker, tmp_report_dir):
    fname = f"{_FAKE_JOB_ID}_smart_long.txt"
    _make_report(tmp_report_dir, fname)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(f"/api/reports/{fname}")
    assert "text/plain" in resp.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_report_not_found_returns_404(mock_worker, tmp_report_dir):
    # valid filename pattern but file doesn't exist
    fname = f"{_FAKE_JOB_ID_2}_smart_short.txt"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(f"/api/reports/{fname}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_report_invalid_filename_rejected(client):
    resp = await client.get("/api/reports/../../etc/passwd")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_report_traversal_with_valid_suffix_rejected(client):
    # Ensure path traversal is rejected even with a valid suffix
    resp = await client.get("/api/reports/../../tmp/aabbccdd-1234-5678-abcd-000000000001_smart_short.txt")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_report_unknown_type_rejected(client):
    # valid UUID but unknown operation type
    resp = await client.get(f"/api/reports/{_FAKE_JOB_ID}_backup.txt")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Worker error → HTTP status mapping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_validation_error_returns_400():
    async def fail(*a, **kw):
        raise WorkerError("VALIDATION_ERROR", "bad params")
    with patch("api.uds_client.send_request", side_effect=fail):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/volumes")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_worker_array_busy_returns_409():
    async def fail(*a, **kw):
        raise WorkerError("ARRAY_BUSY", "busy")
    with patch("api.uds_client.send_request", side_effect=fail):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/volumes/traid_vg/grow",
                                json={"new_disk": "/dev/sde"})
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "ARRAY_BUSY"


@pytest.mark.asyncio
async def test_worker_subprocess_failed_returns_500():
    async def fail(*a, **kw):
        raise WorkerError("SUBPROCESS_FAILED", "command failed")
    with patch("api.uds_client.send_request", side_effect=fail):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/volumes/traid_vg/shrink",
                                json={"disk_to_remove": "/dev/sdb"})
    assert resp.status_code == 500


@pytest.mark.asyncio
async def test_worker_unavailable_returns_503():
    async def unavailable(*a, **kw):
        raise WorkerUnavailableError("socket not found")
    with patch("api.uds_client.send_request", side_effect=unavailable):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/disks")
    assert resp.status_code == 503
