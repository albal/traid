"""
Unit tests for worker/sharing_ops.py — NFS exports and Samba share management.

All tests run without any real system files by patching the file paths.
subprocess calls (_run) are also mocked so no real exportfs/systemctl is needed.
"""

import pytest
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

import worker.sharing_ops as sharing_ops
from worker.sharing_ops import (
    _parse_exports,
    _parse_smb_conf,
    _render_smb_conf,
    _atomic_write,
    nfs_list_exports,
    nfs_add_export,
    nfs_remove_export,
    samba_list_shares,
    samba_add_share,
    samba_remove_share,
)


# ---------------------------------------------------------------------------
# _parse_exports
# ---------------------------------------------------------------------------

def test_parse_exports_empty():
    assert _parse_exports("") == []


def test_parse_exports_comment_only():
    assert _parse_exports("# managed by TRAID\n") == []


def test_parse_exports_single_entry():
    text = "/srv/traid/data\t192.168.1.0/24(rw,sync,no_subtree_check)\n"
    result = _parse_exports(text)
    assert len(result) == 1
    assert result[0]["path"] == "/srv/traid/data"
    assert "192.168.1.0/24" in result[0]["clients"]


def test_parse_exports_multiple_entries():
    text = (
        "# header\n"
        "/srv/traid/a\t10.0.0.0/8(ro)\n"
        "/srv/traid/b\t*(rw)\n"
    )
    result = _parse_exports(text)
    assert len(result) == 2
    assert result[0]["path"] == "/srv/traid/a"
    assert result[1]["path"] == "/srv/traid/b"


def test_parse_exports_blank_lines_ignored():
    text = "\n/srv/traid/data\t192.168.1.1(rw)\n\n"
    result = _parse_exports(text)
    assert len(result) == 1


def test_parse_exports_path_only():
    text = "/srv/traid/data\n"
    result = _parse_exports(text)
    assert len(result) == 1
    assert result[0]["path"] == "/srv/traid/data"
    assert result[0]["clients"] == ""


# ---------------------------------------------------------------------------
# _parse_smb_conf / _render_smb_conf
# ---------------------------------------------------------------------------

def test_parse_smb_conf_empty():
    assert _parse_smb_conf("") == {}


def test_parse_smb_conf_comments_ignored():
    text = "; comment\n# another comment\n"
    assert _parse_smb_conf(text) == {}


def test_parse_smb_conf_global_section():
    text = "[global]\n    workgroup = WORKGROUP\n    security = user\n"
    sections = _parse_smb_conf(text)
    assert "global" in sections
    assert sections["global"]["workgroup"] == "WORKGROUP"
    assert sections["global"]["security"] == "user"


def test_parse_smb_conf_multiple_sections():
    text = (
        "[global]\n    workgroup = WORKGROUP\n\n"
        "[myshare]\n    path = /srv/traid/data\n    writable = yes\n"
    )
    sections = _parse_smb_conf(text)
    assert "global" in sections
    assert "myshare" in sections
    assert sections["myshare"]["path"] == "/srv/traid/data"


def test_parse_smb_conf_value_with_equals():
    text = "[share]\n    path = /srv/traid/a=b\n"
    sections = _parse_smb_conf(text)
    assert sections["share"]["path"] == "/srv/traid/a=b"


def test_render_smb_conf_roundtrip():
    original = {
        "global": {"workgroup": "WORKGROUP"},
        "myshare": {"path": "/srv/traid/data", "writable": "yes"},
    }
    rendered = _render_smb_conf(original)
    parsed_back = _parse_smb_conf(rendered)
    assert parsed_back["global"]["workgroup"] == "WORKGROUP"
    assert parsed_back["myshare"]["path"] == "/srv/traid/data"


def test_render_smb_conf_section_headers():
    sections = {"testshare": {"path": "/srv/traid/x"}}
    rendered = _render_smb_conf(sections)
    assert "[testshare]" in rendered
    assert "path = /srv/traid/x" in rendered


# ---------------------------------------------------------------------------
# nfs_list_exports
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_nfs_list_exports_no_file(tmp_path):
    fake_exports = tmp_path / "exports"
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports):
        result = await nfs_list_exports()
    assert result == []


@pytest.mark.asyncio
async def test_nfs_list_exports_with_entries(tmp_path):
    fake_exports = tmp_path / "exports"
    fake_exports.write_text("/srv/traid/data\t192.168.1.0/24(rw,sync)\n")
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports):
        result = await nfs_list_exports()
    assert len(result) == 1
    assert result[0]["path"] == "/srv/traid/data"


@pytest.mark.asyncio
async def test_nfs_list_exports_multiple(tmp_path):
    fake_exports = tmp_path / "exports"
    fake_exports.write_text(
        "/srv/traid/a\t10.0.0.0/8(ro)\n"
        "/srv/traid/b\t*(rw)\n"
    )
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports):
        result = await nfs_list_exports()
    assert len(result) == 2


# ---------------------------------------------------------------------------
# nfs_add_export
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_nfs_add_export_creates_file(tmp_path):
    fake_exports = tmp_path / "exports"
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        result = await nfs_add_export("/srv/traid/data", "192.168.1.0/24", "rw,sync")
    assert result["added"] is True
    assert fake_exports.exists()
    content = fake_exports.read_text()
    assert "/srv/traid/data" in content
    assert "192.168.1.0/24(rw,sync)" in content


@pytest.mark.asyncio
async def test_nfs_add_export_appends_to_existing(tmp_path):
    fake_exports = tmp_path / "exports"
    fake_exports.write_text("/srv/traid/a\t*(ro)\n")
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        await nfs_add_export("/srv/traid/b", "*", "rw")
    content = fake_exports.read_text()
    assert "/srv/traid/a" in content
    assert "/srv/traid/b" in content


@pytest.mark.asyncio
async def test_nfs_add_export_deduplicates(tmp_path):
    """Adding an export for an existing path replaces it."""
    fake_exports = tmp_path / "exports"
    fake_exports.write_text("/srv/traid/data\t10.0.0.0/8(ro)\n")
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        await nfs_add_export("/srv/traid/data", "*", "rw")
    content = fake_exports.read_text()
    # Only one entry for this path
    assert content.count("/srv/traid/data") == 1
    assert "*(rw)" in content


@pytest.mark.asyncio
async def test_nfs_add_export_no_options(tmp_path):
    fake_exports = tmp_path / "exports"
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        await nfs_add_export("/srv/traid/data", "192.168.1.1", "")
    content = fake_exports.read_text()
    # No parentheses when options is empty
    assert "()" not in content
    assert "192.168.1.1" in content


@pytest.mark.asyncio
async def test_nfs_add_export_calls_exportfs(tmp_path):
    fake_exports = tmp_path / "exports"
    calls = []

    async def capture(*cmd):
        calls.append(cmd)
        return (0, "", "")

    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports), \
         patch("worker.sharing_ops._run", side_effect=capture):
        await nfs_add_export("/srv/traid/data", "*", "rw")

    assert any("exportfs" in str(c) for c in calls)


# ---------------------------------------------------------------------------
# nfs_remove_export
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_nfs_remove_export_no_file(tmp_path):
    fake_exports = tmp_path / "missing"
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports):
        result = await nfs_remove_export("/srv/traid/data")
    assert result["removed"] is False


@pytest.mark.asyncio
async def test_nfs_remove_export_existing(tmp_path):
    fake_exports = tmp_path / "exports"
    fake_exports.write_text(
        "/srv/traid/data\t*(rw)\n"
        "/srv/traid/other\t*(ro)\n"
    )
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        result = await nfs_remove_export("/srv/traid/data")
    assert result["removed"] is True
    content = fake_exports.read_text()
    assert "/srv/traid/data" not in content
    assert "/srv/traid/other" in content


@pytest.mark.asyncio
async def test_nfs_remove_export_not_present(tmp_path):
    fake_exports = tmp_path / "exports"
    fake_exports.write_text("/srv/traid/other\t*(ro)\n")
    with patch.object(sharing_ops, "EXPORTS_FILE", fake_exports), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        result = await nfs_remove_export("/srv/traid/data")
    # Removal of a non-present path still succeeds (idempotent)
    assert result["removed"] is True


# ---------------------------------------------------------------------------
# samba_list_shares
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_samba_list_shares_no_file(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb):
        result = await samba_list_shares()
    assert result == []


@pytest.mark.asyncio
async def test_samba_list_shares_skips_system_sections(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    fake_smb.write_text(
        "[global]\n    workgroup = WORKGROUP\n\n"
        "[homes]\n    browseable = no\n\n"
        "[printers]\n    printable = yes\n"
    )
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb):
        result = await samba_list_shares()
    assert result == []


@pytest.mark.asyncio
async def test_samba_list_shares_returns_user_shares(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    fake_smb.write_text(
        "[global]\n    workgroup = WORKGROUP\n\n"
        "[mydata]\n    path = /srv/traid/data\n    writable = yes\n    guest ok = no\n"
    )
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb):
        result = await samba_list_shares()
    assert len(result) == 1
    assert result[0]["name"] == "mydata"
    assert result[0]["path"] == "/srv/traid/data"


@pytest.mark.asyncio
async def test_samba_list_shares_multiple(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    fake_smb.write_text(
        "[share1]\n    path = /srv/traid/a\n\n"
        "[share2]\n    path = /srv/traid/b\n    guest ok = yes\n"
    )
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb):
        result = await samba_list_shares()
    assert len(result) == 2
    names = {s["name"] for s in result}
    assert "share1" in names
    assert "share2" in names


@pytest.mark.asyncio
async def test_samba_list_shares_public_flag(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    fake_smb.write_text("[pub]\n    path = /srv/traid/x\n    guest ok = yes\n")
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb):
        result = await samba_list_shares()
    assert result[0]["public"] is True


# ---------------------------------------------------------------------------
# samba_add_share
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_samba_add_share_creates_conf(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        result = await samba_add_share("myshare", "/srv/traid/data")
    assert result["added"] is True
    assert fake_smb.exists()
    content = fake_smb.read_text()
    assert "[myshare]" in content
    assert "/srv/traid/data" in content


@pytest.mark.asyncio
async def test_samba_add_share_public(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        await samba_add_share("pubshare", "/srv/traid/pub", public=True)
    content = fake_smb.read_text()
    assert "guest ok = yes" in content


@pytest.mark.asyncio
async def test_samba_add_share_not_writable(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        await samba_add_share("roshare", "/srv/traid/ro", writable=False)
    content = fake_smb.read_text()
    assert "writable = no" in content


@pytest.mark.asyncio
async def test_samba_add_share_comment(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        await samba_add_share("share", "/srv/traid/data", comment="My data share")
    content = fake_smb.read_text()
    assert "My data share" in content


@pytest.mark.asyncio
async def test_samba_add_share_appends_to_existing(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    fake_smb.write_text("[existing]\n    path = /srv/traid/old\n")
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        await samba_add_share("newshare", "/srv/traid/new")
    content = fake_smb.read_text()
    assert "[existing]" in content
    assert "[newshare]" in content


@pytest.mark.asyncio
async def test_samba_add_share_replaces_existing(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    fake_smb.write_text("[myshare]\n    path = /srv/traid/old\n    writable = no\n")
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        await samba_add_share("myshare", "/srv/traid/new", writable=True)
    sections = _parse_smb_conf(fake_smb.read_text())
    assert sections["myshare"]["path"] == "/srv/traid/new"
    assert sections["myshare"]["writable"] == "yes"


@pytest.mark.asyncio
async def test_samba_add_share_calls_reload(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    calls = []

    async def capture(*cmd):
        calls.append(cmd)
        return (0, "", "")

    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", side_effect=capture):
        await samba_add_share("s", "/srv/traid/x")

    assert any("smbd" in str(c) for c in calls)


# ---------------------------------------------------------------------------
# samba_remove_share
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_samba_remove_share_no_file(tmp_path):
    fake_smb = tmp_path / "missing.conf"
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb):
        result = await samba_remove_share("myshare")
    assert result["removed"] is False


@pytest.mark.asyncio
async def test_samba_remove_share_not_found(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    fake_smb.write_text("[global]\n    workgroup = WORKGROUP\n")
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        result = await samba_remove_share("missing")
    assert result["removed"] is False


@pytest.mark.asyncio
async def test_samba_remove_share_existing(tmp_path):
    fake_smb = tmp_path / "smb.conf"
    fake_smb.write_text(
        "[global]\n    workgroup = WORKGROUP\n\n"
        "[myshare]\n    path = /srv/traid/data\n\n"
        "[other]\n    path = /srv/traid/other\n"
    )
    with patch.object(sharing_ops, "SMB_CONF_FILE", fake_smb), \
         patch("worker.sharing_ops._run", AsyncMock(return_value=(0, "", ""))):
        result = await samba_remove_share("myshare")
    assert result["removed"] is True
    sections = _parse_smb_conf(fake_smb.read_text())
    assert "myshare" not in sections
    assert "global" in sections
    assert "other" in sections
