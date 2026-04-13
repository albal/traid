"""Unit tests for /proc/mdstat parser. No hardware required."""

import pytest
from pathlib import Path
from worker.mdstat_reader import parse_mdstat

FIXTURES = Path(__file__).parent / "fixtures"


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


def test_parse_empty():
    arrays = parse_mdstat("Personalities : []\nunused devices: <none>\n")
    assert arrays == []


def test_parse_multiple_arrays():
    raw = """\
Personalities : [raid5] [raid1]
md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]
      7813836800 blocks super 1.2 level 5, 512k chunk [3/3] [UUU]

md1 : active raid1 sde1[0] sdf1[1]
      976762368 blocks super 1.2 [2/2] [UU]

unused devices: <none>
"""
    arrays = parse_mdstat(raw)
    assert len(arrays) == 2
    names = {a["name"] for a in arrays}
    assert names == {"md0", "md1"}


