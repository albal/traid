"""
Unit tests for worker/backup_scheduler.py — CRUD operations and job persistence.

All tests use a temporary directory for BACKUP_JOBS_FILE so no real filesystem
state is modified.  The actual backup execution (run_backup) is not tested here
because it spawns real subprocesses; those code paths are covered by the
integration tests.
"""

import json
import time
import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock

import worker.backup_scheduler as sched
from worker.backup_scheduler import (
    create_job,
    delete_job,
    list_jobs,
    get_history,
    _load_jobs,
    _save_jobs,
    _update_job_record,
)

_PROTO_RSYNC   = "rsync_local"
_PROTO_NFS     = "nfs"
_PROTO_CIFS    = "cifs"
_PROTO_BTRFS   = "btrfs_send"

_VALID_UUID_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


@pytest.fixture(autouse=True)
def tmp_jobs_file(tmp_path):
    """Redirect BACKUP_JOBS_FILE to a temp path for every test."""
    jobs_file = tmp_path / "backup_jobs.json"
    with patch.object(sched, "BACKUP_JOBS_FILE", jobs_file):
        yield jobs_file


# ---------------------------------------------------------------------------
# _load_jobs / _save_jobs
# ---------------------------------------------------------------------------

def test_load_jobs_empty_file(tmp_jobs_file):
    assert _load_jobs() == {}


def test_load_jobs_missing_file(tmp_jobs_file):
    tmp_jobs_file.unlink(missing_ok=True)
    assert _load_jobs() == {}


def test_load_jobs_corrupt_file(tmp_jobs_file):
    tmp_jobs_file.write_text("this is not json{{{")
    assert _load_jobs() == {}


def test_save_and_load_roundtrip(tmp_jobs_file):
    data = {"abc": {"name": "test", "backup_id": "abc"}}
    _save_jobs(data)
    assert _load_jobs() == data


def test_save_jobs_atomic(tmp_jobs_file):
    """Temp file must not remain after a successful save."""
    _save_jobs({"x": {}})
    tmp_file = tmp_jobs_file.with_suffix(".tmp")
    assert not tmp_file.exists()


# ---------------------------------------------------------------------------
# create_job
# ---------------------------------------------------------------------------

def test_create_job_returns_record():
    record = create_job("daily", "traid_vg", _PROTO_RSYNC, "/mnt/backup", 24)
    assert record["name"] == "daily"
    assert record["source_vg"] == "traid_vg"
    assert record["dest_protocol"] == _PROTO_RSYNC
    assert record["interval_hours"] == 24
    assert record["last_run"] is None
    assert record["last_status"] == "never"


def test_create_job_uuid_generated():
    import re
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 12)
    assert re.match(_VALID_UUID_RE, record["backup_id"])


def test_create_job_persisted():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    jobs = _load_jobs()
    assert record["backup_id"] in jobs


def test_create_job_with_creds():
    record = create_job(
        "cifsjob", "vg", _PROTO_CIFS, "//nas/share", 24,
        dest_host="nas.local", dest_cifs_user="admin", dest_cifs_pass="s3cret",
    )
    assert record["dest_cifs_user"] == "admin"
    assert record["dest_cifs_pass"] == "s3cret"
    assert record["dest_host"] == "nas.local"


def test_create_multiple_jobs():
    r1 = create_job("j1", "vg1", _PROTO_RSYNC, "/tmp/a", 24)
    r2 = create_job("j2", "vg2", _PROTO_NFS, "nas:/backup", 12)
    assert r1["backup_id"] != r2["backup_id"]
    assert len(_load_jobs()) == 2


def test_create_job_history_empty():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    assert record["history"] == []


# ---------------------------------------------------------------------------
# list_jobs
# ---------------------------------------------------------------------------

def test_list_jobs_empty():
    assert list_jobs() == []


def test_list_jobs_returns_all():
    create_job("a", "vg", _PROTO_RSYNC, "/tmp/a", 24)
    create_job("b", "vg", _PROTO_NFS, "nas:/b", 12)
    result = list_jobs()
    assert len(result) == 2
    names = {j["name"] for j in result}
    assert names == {"a", "b"}


# ---------------------------------------------------------------------------
# delete_job
# ---------------------------------------------------------------------------

def test_delete_job_not_found():
    result = delete_job("00000000-0000-0000-0000-000000000000")
    assert result["deleted"] is False


def test_delete_job_existing():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    backup_id = record["backup_id"]
    result = delete_job(backup_id)
    assert result["deleted"] is True
    assert backup_id not in _load_jobs()


def test_delete_job_leaves_others():
    r1 = create_job("j1", "vg", _PROTO_RSYNC, "/tmp/a", 24)
    r2 = create_job("j2", "vg", _PROTO_RSYNC, "/tmp/b", 24)
    delete_job(r1["backup_id"])
    jobs = _load_jobs()
    assert r1["backup_id"] not in jobs
    assert r2["backup_id"] in jobs


# ---------------------------------------------------------------------------
# get_history
# ---------------------------------------------------------------------------

def test_get_history_empty():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    assert get_history(record["backup_id"]) == []


def test_get_history_not_found():
    with pytest.raises(RuntimeError, match="not found"):
        get_history("00000000-0000-0000-0000-000000000000")


def test_get_history_after_update():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    bid = record["backup_id"]
    _update_job_record(bid, status="ok", error=None, duration_s=5.0)
    history = get_history(bid)
    assert len(history) == 1
    assert history[0]["status"] == "ok"
    assert history[0]["duration_s"] == 5.0


# ---------------------------------------------------------------------------
# _update_job_record
# ---------------------------------------------------------------------------

def test_update_job_record_sets_last_run():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    bid = record["backup_id"]
    before = time.time()
    _update_job_record(bid, status="ok", error=None, duration_s=1.0)
    jobs = _load_jobs()
    assert jobs[bid]["last_run"] >= before
    assert jobs[bid]["last_status"] == "ok"
    assert jobs[bid]["last_error"] is None


def test_update_job_record_failed_status():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    bid = record["backup_id"]
    _update_job_record(bid, status="failed", error="rsync failed", duration_s=2.5)
    jobs = _load_jobs()
    assert jobs[bid]["last_status"] == "failed"
    assert jobs[bid]["last_error"] == "rsync failed"


def test_update_job_record_history_prepended():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    bid = record["backup_id"]
    _update_job_record(bid, status="ok", error=None, duration_s=1.0)
    _update_job_record(bid, status="failed", error="err", duration_s=2.0)
    history = get_history(bid)
    # Most recent first
    assert history[0]["status"] == "failed"
    assert history[1]["status"] == "ok"


def test_update_job_record_history_capped_at_max():
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    bid = record["backup_id"]
    for i in range(sched.MAX_HISTORY + 5):
        _update_job_record(bid, status="ok", error=None, duration_s=float(i))
    history = get_history(bid)
    assert len(history) == sched.MAX_HISTORY


def test_update_job_record_missing_id_no_crash():
    # Should silently ignore missing IDs
    _update_job_record("00000000-0000-0000-0000-000000000000",
                       status="ok", error=None, duration_s=1.0)


# ---------------------------------------------------------------------------
# run_backup — protocol dispatch (mocked subprocesses)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_backup_rsync_local_success(tmp_path):
    record = create_job("j", "traid_vg", _PROTO_RSYNC, str(tmp_path / "dest"), 24)
    bid = record["backup_id"]
    updates = []

    with patch("worker.backup_scheduler._run", AsyncMock(return_value=(0, "", ""))):
        await sched.run_backup(bid, lambda msg, pct: updates.append((msg, pct)))

    jobs = _load_jobs()
    assert jobs[bid]["last_status"] == "ok"
    assert any("complete" in u[0].lower() for u in updates)


@pytest.mark.asyncio
async def test_run_backup_rsync_failure_updates_record(tmp_path):
    record = create_job("j", "traid_vg", _PROTO_RSYNC, str(tmp_path / "dest"), 24)
    bid = record["backup_id"]

    with patch("worker.backup_scheduler._run", AsyncMock(return_value=(1, "", "connection refused"))):
        with pytest.raises(RuntimeError, match="rsync failed"):
            await sched.run_backup(bid, lambda *a: None)

    jobs = _load_jobs()
    assert jobs[bid]["last_status"] == "failed"
    assert "rsync failed" in (jobs[bid]["last_error"] or "")


@pytest.mark.asyncio
async def test_run_backup_unknown_id():
    with pytest.raises(RuntimeError, match="not found"):
        await sched.run_backup("00000000-0000-0000-0000-000000000000", lambda *a: None)


@pytest.mark.asyncio
async def test_run_backup_unknown_protocol(tmp_path):
    """Directly patch a job with an invalid protocol."""
    record = create_job("j", "vg", _PROTO_RSYNC, "/tmp/x", 24)
    bid = record["backup_id"]
    jobs = _load_jobs()
    jobs[bid]["dest_protocol"] = "ftp"
    _save_jobs(jobs)

    with pytest.raises(RuntimeError, match="Unknown backup protocol"):
        await sched.run_backup(bid, lambda *a: None)
