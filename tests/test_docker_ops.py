"""
Unit tests for worker/docker_ops.py

All subprocess calls are mocked with AsyncMock — no real Docker daemon needed.
"""

import json
import pytest
from unittest.mock import AsyncMock, patch

import worker.docker_ops as docker_ops
from worker.docker_ops import (
    _parse_json_stream,
    list_containers,
    container_action,
    container_logs,
    list_images,
    pull_image,
    remove_image,
    system_prune,
)


def _make_run(rc=0, stdout="", stderr=""):
    return AsyncMock(return_value=(rc, stdout, stderr))


# ---------------------------------------------------------------------------
# _parse_json_stream
# ---------------------------------------------------------------------------

def test_parse_json_stream_empty():
    assert _parse_json_stream("") == []


def test_parse_json_stream_single():
    obj = {"ID": "abc123", "Names": "mycontainer"}
    result = _parse_json_stream(json.dumps(obj))
    assert len(result) == 1
    assert result[0]["ID"] == "abc123"


def test_parse_json_stream_multiple_lines():
    lines = "\n".join([
        json.dumps({"ID": "a", "Names": "c1"}),
        json.dumps({"ID": "b", "Names": "c2"}),
    ])
    result = _parse_json_stream(lines)
    assert len(result) == 2


def test_parse_json_stream_skips_bad_lines():
    lines = json.dumps({"ID": "good"}) + "\nnot-json\n" + json.dumps({"ID": "also-good"})
    result = _parse_json_stream(lines)
    assert len(result) == 2


def test_parse_json_stream_blank_lines_ignored():
    lines = "\n\n" + json.dumps({"ID": "a"}) + "\n\n"
    result = _parse_json_stream(lines)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# list_containers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_containers_empty():
    with patch("worker.docker_ops._docker", _make_run(stdout="")):
        result = await list_containers()
    assert result == []


@pytest.mark.asyncio
async def test_list_containers_one():
    row = {"ID": "abc1234567890", "Names": "web", "Image": "nginx:latest",
           "Status": "Up 2 hours", "State": "running", "Ports": "80/tcp", "CreatedAt": "2024-01-01"}
    with patch("worker.docker_ops._docker", _make_run(stdout=json.dumps(row))):
        result = await list_containers()
    assert len(result) == 1
    c = result[0]
    assert c["id"] == "abc1234567890"[:12]
    assert c["name"] == "web"
    assert c["image"] == "nginx:latest"
    assert c["state"] == "running"


@pytest.mark.asyncio
async def test_list_containers_multiple():
    rows = "\n".join([
        json.dumps({"ID": "aaa", "Names": "c1", "Image": "img1",
                    "Status": "Up", "State": "running", "Ports": "", "CreatedAt": ""}),
        json.dumps({"ID": "bbb", "Names": "c2", "Image": "img2",
                    "Status": "Exited", "State": "exited", "Ports": "", "CreatedAt": ""}),
    ])
    with patch("worker.docker_ops._docker", _make_run(stdout=rows)):
        result = await list_containers()
    assert len(result) == 2


@pytest.mark.asyncio
async def test_list_containers_all_false():
    """When all=False, --all flag should not be passed."""
    calls = []

    async def capture(*args):
        calls.append(args)
        return (0, "", "")

    with patch("worker.docker_ops._docker", side_effect=capture):
        await list_containers(all_containers=False)

    assert "--all" not in calls[0]


@pytest.mark.asyncio
async def test_list_containers_failure():
    with patch("worker.docker_ops._docker", _make_run(rc=1, stderr="permission denied")):
        with pytest.raises(RuntimeError, match="docker ps failed"):
            await list_containers()


# ---------------------------------------------------------------------------
# container_action
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_container_start():
    with patch("worker.docker_ops._docker", _make_run()):
        result = await container_action("abc123", "start")
    assert result["ok"] is True
    assert result["action"] == "start"


@pytest.mark.asyncio
async def test_container_stop():
    with patch("worker.docker_ops._docker", _make_run()):
        result = await container_action("abc123", "stop")
    assert result["ok"] is True


@pytest.mark.asyncio
async def test_container_rm():
    with patch("worker.docker_ops._docker", _make_run()):
        result = await container_action("abc123", "rm")
    assert result["ok"] is True


@pytest.mark.asyncio
async def test_container_action_failure():
    with patch("worker.docker_ops._docker", _make_run(rc=1, stderr="no such container")):
        with pytest.raises(RuntimeError, match="failed"):
            await container_action("abc123", "start")


# ---------------------------------------------------------------------------
# container_logs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_container_logs_returns_output():
    with patch("worker.docker_ops._docker", _make_run(stdout="line1\nline2\n", stderr="")):
        result = await container_logs("abc123", lines=50)
    assert "line1" in result["logs"]
    assert result["container_id"] == "abc123"


@pytest.mark.asyncio
async def test_container_logs_combines_stderr():
    """Docker logs often writes to stderr for some images."""
    with patch("worker.docker_ops._docker", _make_run(stdout="", stderr="stderr-line\n")):
        result = await container_logs("abc123")
    assert "stderr-line" in result["logs"]


@pytest.mark.asyncio
async def test_container_logs_failure_empty_output():
    with patch("worker.docker_ops._docker", _make_run(rc=1, stdout="", stderr="")):
        with pytest.raises(RuntimeError, match="docker logs failed"):
            await container_logs("abc123")


# ---------------------------------------------------------------------------
# list_images
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_images_empty():
    with patch("worker.docker_ops._docker", _make_run(stdout="")):
        result = await list_images()
    assert result == []


@pytest.mark.asyncio
async def test_list_images_one():
    row = {"ID": "sha256:abc123", "Repository": "ubuntu", "Tag": "22.04",
           "Size": "77.8MB", "CreatedAt": "2024-01-01"}
    with patch("worker.docker_ops._docker", _make_run(stdout=json.dumps(row))):
        result = await list_images()
    assert len(result) == 1
    img = result[0]
    assert img["repository"] == "ubuntu"
    assert img["tag"] == "22.04"


@pytest.mark.asyncio
async def test_list_images_failure():
    with patch("worker.docker_ops._docker", _make_run(rc=1, stderr="error")):
        with pytest.raises(RuntimeError, match="docker images failed"):
            await list_images()


# ---------------------------------------------------------------------------
# pull_image
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pull_image_success():
    updates = []
    with patch("worker.docker_ops._docker", _make_run(stdout="Pull complete")):
        result = await pull_image("nginx:latest", lambda msg, pct: updates.append((msg, pct)))
    assert result["image"] == "nginx:latest"
    assert any(pct == 100 for _, pct in updates)


@pytest.mark.asyncio
async def test_pull_image_failure():
    with patch("worker.docker_ops._docker", _make_run(rc=1, stderr="not found")):
        with pytest.raises(RuntimeError, match="docker pull"):
            await pull_image("badimage:tag", lambda *a: None)


@pytest.mark.asyncio
async def test_pull_image_truncates_long_output():
    long_output = "x" * 5000
    with patch("worker.docker_ops._docker", _make_run(stdout=long_output)):
        result = await pull_image("ubuntu", lambda *a: None)
    assert len(result["output"]) <= 1000


# ---------------------------------------------------------------------------
# remove_image
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_image_success():
    with patch("worker.docker_ops._docker", _make_run()):
        result = await remove_image("abc123")
    assert result["removed"] is True


@pytest.mark.asyncio
async def test_remove_image_force():
    calls = []

    async def capture(*args):
        calls.append(args)
        return (0, "", "")

    with patch("worker.docker_ops._docker", side_effect=capture):
        await remove_image("abc123", force=True)

    assert "--force" in calls[0]


@pytest.mark.asyncio
async def test_remove_image_no_force():
    calls = []

    async def capture(*args):
        calls.append(args)
        return (0, "", "")

    with patch("worker.docker_ops._docker", side_effect=capture):
        await remove_image("abc123", force=False)

    assert "--force" not in calls[0]


@pytest.mark.asyncio
async def test_remove_image_failure():
    with patch("worker.docker_ops._docker", _make_run(rc=1, stderr="image in use")):
        with pytest.raises(RuntimeError, match="docker rmi"):
            await remove_image("abc123")


# ---------------------------------------------------------------------------
# system_prune
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_system_prune_success():
    updates = []
    with patch("worker.docker_ops._docker", _make_run(stdout="Deleted: sha256:aaa")):
        result = await system_prune(lambda msg, pct: updates.append((msg, pct)))
    assert "output" in result
    assert any(pct == 100 for _, pct in updates)


@pytest.mark.asyncio
async def test_system_prune_failure():
    with patch("worker.docker_ops._docker", _make_run(rc=1, stderr="cannot prune")):
        with pytest.raises(RuntimeError, match="docker system prune failed"):
            await system_prune(lambda *a: None)
