"""Unit tests for /proc/mdstat parser. No hardware required."""

import pytest
from pathlib import Path
from worker.mdstat_reader import parse_mdstat

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Fixture-based tests
# ---------------------------------------------------------------------------

def test_parse_syncing():
    raw = (FIXTURES / "mdstat_syncing.txt").read_text()
    arrays = parse_mdstat(raw)
    assert len(arrays) == 1
    arr = arrays[0]
    assert arr["name"] == "md0"
    assert arr["state"] == "active"
    assert arr["level"] == "raid5"
    assert arr["sync_pct"] == pytest.approx(37.4)
    assert arr["finish_eta"] == "43.5min"

def test_parse_clean():
    raw = (FIXTURES / "mdstat_clean.txt").read_text()
    arrays = parse_mdstat(raw)
    assert len(arrays) == 1
    arr = arrays[0]
    assert arr["name"] == "md0"
    assert arr["state"] == "active"
    assert arr["sync_pct"] is None
    assert arr["finish_eta"] is None


# ---------------------------------------------------------------------------
# Empty / minimal input
# ---------------------------------------------------------------------------

def test_parse_empty_personalities():
    arrays = parse_mdstat("Personalities : []\nunused devices: <none>\n")
    assert arrays == []

def test_parse_empty_string():
    arrays = parse_mdstat("")
    assert arrays == []

def test_parse_only_header():
    arrays = parse_mdstat("Personalities : [raid5] [raid6]\nunused devices: <none>\n")
    assert arrays == []


# ---------------------------------------------------------------------------
# RAID levels
# ---------------------------------------------------------------------------

def test_parse_raid1():
    raw = (
        "Personalities : [raid1]\n"
        "md1 : active raid1 sde1[0] sdf1[1]\n"
        "      976762368 blocks super 1.2 [2/2] [UU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert len(arrays) == 1
    assert arrays[0]["level"] == "raid1"
    assert arrays[0]["name"] == "md1"

def test_parse_raid5():
    raw = (
        "Personalities : [raid5]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "      7813836800 blocks super 1.2 level 5, 512k chunk [3/3] [UUU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["level"] == "raid5"

def test_parse_raid6():
    raw = (
        "Personalities : [raid6]\n"
        "md0 : active raid6 sdb1[0] sdc1[1] sdd1[2] sde1[3]\n"
        "      11718787072 blocks super 1.2 level 6, 512k chunk [4/4] [UUUU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["level"] == "raid6"


# ---------------------------------------------------------------------------
# Sync / recovery / check operations
# ---------------------------------------------------------------------------

def test_parse_resync_progress():
    raw = (
        "Personalities : [raid5]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "      7813836800 blocks super 1.2 level 5 [3/3] [UUU]\n"
        "      [=======>.............]  resync = 37.4% (1464549376/3906262784) finish=43.5min speed=92893K/sec\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["sync_pct"] == pytest.approx(37.4)
    assert "min" in arrays[0]["finish_eta"]

def test_parse_recovery_progress():
    raw = (
        "Personalities : [raid5]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2] sde1[3]\n"
        "      11718787072 blocks super 1.2 level 5 [4/3] [UUU_]\n"
        "      [=======>.............]  recovery = 37.4% (1464549376/3906262784) finish=43.5min speed=92893K/sec\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["sync_pct"] == pytest.approx(37.4)

def test_parse_check_progress():
    raw = (
        "Personalities : [raid5]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "      7813836800 blocks super 1.2 level 5 [3/3] [UUU]\n"
        "      [==>..................]  check = 15.2% (593526272/3906918400) finish=340.3min speed=191256K/sec\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["sync_pct"] == pytest.approx(15.2)

def test_parse_reshape_progress():
    raw = (
        "Personalities : [raid6]\n"
        "md0 : active raid6 sdb1[0] sdc1[1] sdd1[2] sde1[3]\n"
        "      11718787072 blocks super 1.2 level 6 [4/4] [UUUU]\n"
        "      [=============>.......]  reshape = 65.0% (2541453312/3906262784) finish=12.1min speed=190123K/sec\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["sync_pct"] == pytest.approx(65.0)

def test_parse_idle_has_none_sync():
    raw = (
        "Personalities : [raid5]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "      7813836800 blocks super 1.2 level 5, 512k chunk [3/3] [UUU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["sync_pct"] is None
    assert arrays[0]["finish_eta"] is None


# ---------------------------------------------------------------------------
# Degraded / partial arrays
# ---------------------------------------------------------------------------

def test_parse_degraded_state():
    raw = (
        "Personalities : [raid5]\n"
        "md0 : active raid5 sdb1[0] sdd1[2]\n"
        "      5859026432 blocks super 1.2 level 5, 512k chunk [3/2] [U_U]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert len(arrays) == 1
    assert arrays[0]["name"] == "md0"
    assert arrays[0]["sync_pct"] is None

def test_parse_auto_read_only():
    raw = (
        "Personalities : [raid1]\n"
        "md0 : active (auto-read-only) raid1 sdb1[0] sdc1[1]\n"
        "      976762368 blocks super 1.2 [2/2] [UU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert len(arrays) == 1
    assert arrays[0]["name"] == "md0"


# ---------------------------------------------------------------------------
# Multiple arrays
# ---------------------------------------------------------------------------

def test_parse_multiple_arrays():
    raw = (
        "Personalities : [raid5] [raid1]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "      7813836800 blocks super 1.2 level 5, 512k chunk [3/3] [UUU]\n\n"
        "md1 : active raid1 sde1[0] sdf1[1]\n"
        "      976762368 blocks super 1.2 [2/2] [UU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert len(arrays) == 2
    names = {a["name"] for a in arrays}
    assert names == {"md0", "md1"}

def test_parse_multiple_arrays_different_levels():
    raw = (
        "Personalities : [raid5] [raid6] [raid1]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "      7813836800 blocks super 1.2 level 5 [3/3] [UUU]\n\n"
        "md1 : active raid6 sde1[0] sdf1[1] sdg1[2] sdh1[3]\n"
        "      11718787072 blocks super 1.2 level 6 [4/4] [UUUU]\n\n"
        "md2 : active raid1 sdi1[0] sdj1[1]\n"
        "      976762368 blocks super 1.2 [2/2] [UU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert len(arrays) == 3
    levels = {a["name"]: a["level"] for a in arrays}
    assert levels["md0"] == "raid5"
    assert levels["md1"] == "raid6"
    assert levels["md2"] == "raid1"

def test_parse_one_syncing_one_idle():
    raw = (
        "Personalities : [raid5]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "      7813836800 blocks super 1.2 level 5 [3/3] [UUU]\n"
        "      [=======>.............]  resync = 37.4% (1464549376/3906262784) finish=43.5min speed=92893K/sec\n\n"
        "md1 : active raid1 sde1[0] sdf1[1]\n"
        "      976762368 blocks super 1.2 [2/2] [UU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    by_name = {a["name"]: a for a in arrays}
    assert by_name["md0"]["sync_pct"] == pytest.approx(37.4)
    assert by_name["md1"]["sync_pct"] is None


# ---------------------------------------------------------------------------
# High-numbered md devices
# ---------------------------------------------------------------------------

def test_parse_md10():
    raw = (
        "Personalities : [raid5]\n"
        "md10 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "       7813836800 blocks super 1.2 level 5, 512k chunk [3/3] [UUU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["name"] == "md10"

def test_parse_md127():
    raw = (
        "Personalities : [raid1]\n"
        "md127 : active raid1 sde1[0] sdf1[1]\n"
        "        976762368 blocks super 1.2 [2/2] [UU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    assert arrays[0]["name"] == "md127"


# ---------------------------------------------------------------------------
# Return structure
# ---------------------------------------------------------------------------

def test_result_has_required_keys():
    raw = (
        "Personalities : [raid5]\n"
        "md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]\n"
        "      7813836800 blocks super 1.2 level 5, 512k chunk [3/3] [UUU]\n\n"
        "unused devices: <none>\n"
    )
    arrays = parse_mdstat(raw)
    arr = arrays[0]
    assert "name" in arr
    assert "state" in arr
    assert "level" in arr
    assert "sync_pct" in arr
    assert "finish_eta" in arr

def test_sync_pct_is_float_or_none():
    raw = (FIXTURES / "mdstat_syncing.txt").read_text()
    arrays = parse_mdstat(raw)
    pct = arrays[0]["sync_pct"]
    assert pct is None or isinstance(pct, float)

def test_finish_eta_is_string_or_none():
    raw = (FIXTURES / "mdstat_syncing.txt").read_text()
    arrays = parse_mdstat(raw)
    eta = arrays[0]["finish_eta"]
    assert eta is None or isinstance(eta, str)
