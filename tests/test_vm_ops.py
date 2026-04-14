"""
Unit tests for worker/vm_ops.py

All virsh/qemu-img/virt-install subprocesses are mocked — no real KVM needed.
"""

import pytest
from unittest.mock import AsyncMock, patch, call

import worker.vm_ops as vm_ops
from worker.vm_ops import (
    list_vms,
    vm_info,
    list_isos,
    vm_action,
    vm_delete,
)


def _make_virsh(rc=0, stdout="", stderr=""):
    return AsyncMock(return_value=(rc, stdout, stderr))


# ---------------------------------------------------------------------------
# list_isos
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_isos_empty(tmp_path):
    with patch.object(vm_ops, "ISO_DIR", tmp_path):
        result = await list_isos()
    assert result == []


@pytest.mark.asyncio
async def test_list_isos_returns_iso_files(tmp_path):
    (tmp_path / "debian12.iso").write_bytes(b"")
    (tmp_path / "ubuntu2204.iso").write_bytes(b"")
    (tmp_path / "readme.txt").write_bytes(b"")  # should be excluded
    with patch.object(vm_ops, "ISO_DIR", tmp_path):
        result = await list_isos()
    assert "debian12.iso" in result
    assert "ubuntu2204.iso" in result
    assert "readme.txt" not in result


@pytest.mark.asyncio
async def test_list_isos_sorted(tmp_path):
    for name in ["c.iso", "a.iso", "b.iso"]:
        (tmp_path / name).write_bytes(b"")
    with patch.object(vm_ops, "ISO_DIR", tmp_path):
        result = await list_isos()
    assert result == sorted(result)


@pytest.mark.asyncio
async def test_list_isos_creates_dir(tmp_path):
    iso_dir = tmp_path / "iso"
    assert not iso_dir.exists()
    with patch.object(vm_ops, "ISO_DIR", iso_dir):
        await list_isos()
    assert iso_dir.exists()


# ---------------------------------------------------------------------------
# vm_info
# ---------------------------------------------------------------------------

_DOMINFO_RUNNING = """\
Id:             5
Name:           debian12
UUID:           aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
OS Type:        hvm
State:          running
CPU(s):         2
CPU time:       100.0s
Max memory:     2097152 KiB
Used memory:    2097152 KiB
Persistent:     yes
Autostart:      disable
Managed save:   no
Security model: apparmor
Security DOI:   0
"""

_DOMINFO_SHUT_OFF = """\
Id:             -
Name:           winvm
State:          shut off
CPU(s):         4
Max memory:     8388608 KiB
Used memory:    0 KiB
"""


@pytest.mark.asyncio
async def test_vm_info_running():
    with patch("worker.vm_ops._virsh") as mock_virsh:
        # dominfo call returns running state
        # vncdisplay call returns :0
        mock_virsh.side_effect = [
            (0, _DOMINFO_RUNNING, ""),
            (0, ":0\n", ""),
        ]
        info = await vm_info("debian12")
    assert info["name"] == "debian12"
    assert info["state"] == "running"
    assert info["vcpus"] == 2
    assert info["max_memory_kib"] == "2097152 KiB"
    assert info["vnc_port"] == 5900


@pytest.mark.asyncio
async def test_vm_info_shut_off():
    with patch("worker.vm_ops._virsh") as mock_virsh:
        mock_virsh.side_effect = [
            (0, _DOMINFO_SHUT_OFF, ""),
            (1, "", "error: domain is not running"),
        ]
        info = await vm_info("winvm")
    assert info["state"] == "shut off"
    assert info["vcpus"] == 4
    assert "vnc_port" not in info


@pytest.mark.asyncio
async def test_vm_info_dominfo_failure():
    with patch("worker.vm_ops._virsh", _make_virsh(rc=1, stderr="Domain not found")):
        with pytest.raises(RuntimeError, match="dominfo failed"):
            await vm_info("nonexistent")


@pytest.mark.asyncio
async def test_vm_info_no_vnc():
    with patch("worker.vm_ops._virsh") as mock_virsh:
        mock_virsh.side_effect = [
            (0, _DOMINFO_RUNNING, ""),
            (1, "", "error: no VNC"),
        ]
        info = await vm_info("debian12")
    assert "vnc_port" not in info


# ---------------------------------------------------------------------------
# list_vms
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_vms_empty():
    with patch("worker.vm_ops._virsh", _make_virsh(stdout="")):
        result = await list_vms()
    assert result == []


@pytest.mark.asyncio
async def test_list_vms_one():
    with patch("worker.vm_ops._virsh") as mock_virsh:
        # First call: list --all --name
        # Subsequent pairs: dominfo + vncdisplay
        mock_virsh.side_effect = [
            (0, "debian12\n", ""),
            (0, _DOMINFO_RUNNING, ""),
            (0, ":0\n", ""),
        ]
        result = await list_vms()
    assert len(result) == 1
    assert result[0]["name"] == "debian12"


@pytest.mark.asyncio
async def test_list_vms_multiple():
    with patch("worker.vm_ops._virsh") as mock_virsh:
        mock_virsh.side_effect = [
            (0, "vm1\nvm2\n", ""),
            (0, _DOMINFO_RUNNING, ""),
            (0, "", ""),
            (0, _DOMINFO_SHUT_OFF, ""),
            (1, "", ""),
        ]
        result = await list_vms()
    assert len(result) == 2


@pytest.mark.asyncio
async def test_list_vms_info_failure_returns_partial():
    """If vm_info fails for one VM, still return a partial record."""
    with patch("worker.vm_ops._virsh") as mock_virsh:
        mock_virsh.side_effect = [
            (0, "good\nbad\n", ""),
            # good: dominfo + vnc
            (0, _DOMINFO_RUNNING, ""),
            (0, ":1\n", ""),
            # bad: dominfo fails
            (1, "", "not found"),
        ]
        result = await list_vms()
    assert len(result) == 2
    bad = next(v for v in result if v["name"] == "bad")
    assert bad["state"] == "unknown"


@pytest.mark.asyncio
async def test_list_vms_failure():
    with patch("worker.vm_ops._virsh", _make_virsh(rc=1, stderr="connection failed")):
        with pytest.raises(RuntimeError, match="virsh list failed"):
            await list_vms()


# ---------------------------------------------------------------------------
# vm_action
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("action,expected_cmd", [
    ("start",    "start"),
    ("shutdown", "shutdown"),
    ("destroy",  "destroy"),
    ("suspend",  "suspend"),
    ("resume",   "resume"),
])
async def test_vm_action_valid(action, expected_cmd):
    calls = []

    async def capture(*args):
        calls.append(args)
        return (0, "", "")

    with patch("worker.vm_ops._virsh", side_effect=capture):
        result = await vm_action("myvm", action)

    assert result["ok"] is True
    assert expected_cmd in calls[0]
    assert "myvm" in calls[0]


@pytest.mark.asyncio
async def test_vm_action_unknown():
    with pytest.raises(ValueError, match="Unknown VM action"):
        await vm_action("myvm", "explode")


@pytest.mark.asyncio
async def test_vm_action_virsh_failure():
    with patch("worker.vm_ops._virsh", _make_virsh(rc=1, stderr="domain already running")):
        with pytest.raises(RuntimeError, match="failed"):
            await vm_action("myvm", "start")


# ---------------------------------------------------------------------------
# vm_delete
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_vm_delete_removes_storage():
    calls = []

    async def capture(*args):
        calls.append(args)
        return (0, "", "")

    with patch("worker.vm_ops._virsh", side_effect=capture):
        result = await vm_delete("myvm", keep_storage=False)

    assert result["deleted"] is True
    assert "--remove-all-storage" in calls[0]


@pytest.mark.asyncio
async def test_vm_delete_keeps_storage():
    calls = []

    async def capture(*args):
        calls.append(args)
        return (0, "", "")

    with patch("worker.vm_ops._virsh", side_effect=capture):
        await vm_delete("myvm", keep_storage=True)

    assert "--remove-all-storage" not in calls[0]


@pytest.mark.asyncio
async def test_vm_delete_failure():
    with patch("worker.vm_ops._virsh", _make_virsh(rc=1, stderr="domain not found")):
        with pytest.raises(RuntimeError, match="undefine"):
            await vm_delete("myvm")


# ---------------------------------------------------------------------------
# create_vm
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_vm_success(tmp_path):
    iso_dir = tmp_path / "iso"
    iso_dir.mkdir()
    (iso_dir / "debian12.iso").write_bytes(b"fake")

    updates = []
    calls = []

    async def capture(*args):
        calls.append(args)
        return (0, "", "")

    with patch.object(vm_ops, "ISO_DIR", iso_dir), \
         patch("worker.vm_ops._run", side_effect=capture):
        await vm_ops.create_vm(
            "testvm", "debian12.iso", 2048, 2, 20,
            lambda msg, pct: updates.append((msg, pct)),
        )

    # Should call qemu-img create and virt-install
    cmds = [c[0] for c in calls]
    assert any("qemu-img" in c for c in cmds)
    assert any("virt-install" in c for c in cmds)
    assert any(pct == 100 for _, pct in updates)


@pytest.mark.asyncio
async def test_create_vm_missing_iso(tmp_path):
    iso_dir = tmp_path / "iso"
    iso_dir.mkdir()
    # ISO file does not exist

    with patch.object(vm_ops, "ISO_DIR", iso_dir):
        with pytest.raises(RuntimeError, match="ISO not found"):
            await vm_ops.create_vm(
                "testvm", "missing.iso", 2048, 2, 20, lambda *a: None,
            )


@pytest.mark.asyncio
async def test_create_vm_qemu_img_failure(tmp_path):
    iso_dir = tmp_path / "iso"
    iso_dir.mkdir()
    (iso_dir / "debian12.iso").write_bytes(b"fake")

    async def fail_first(*args):
        return (1, "", "no space left on device")

    with patch.object(vm_ops, "ISO_DIR", iso_dir), \
         patch("worker.vm_ops._run", side_effect=fail_first):
        with pytest.raises(RuntimeError, match="qemu-img create failed"):
            await vm_ops.create_vm(
                "testvm", "debian12.iso", 2048, 2, 20, lambda *a: None,
            )
