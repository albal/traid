"""
/proc/mdstat poller with asyncio.Queue fan-out.

inotify does not work on virtual proc files, so we poll every 2 seconds.
asyncio.to_thread() is used for the blocking file read to keep the event
loop free. Multiple subscribers each get their own asyncio.Queue.
"""

import asyncio
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

POLL_INTERVAL = 2.0  # seconds


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

# Matches lines like:
#   md0 : active raid5 sdb1[0] sdc1[1] sdd1[2]
_ARRAY_RE = re.compile(
    r"^(?P<name>md\d+)\s*:\s*(?P<state>\S+)\s+(?P<level>raid\d+|linear|faulty)?\s*(?P<members>.*)$"
)

# Matches the sync progress line:
#   [=======>.............]  resync = 37.4% (1462499328/3906918400) finish=43.5min speed=93428K/sec
_SYNC_RE = re.compile(
    r"(?P<op>resync|recovery|reshape|check|repair)\s*=\s*(?P<pct>[\d.]+)%"
    r".*?finish=(?P<eta>[\d.]+\w+)"
)


def parse_mdstat(raw: str) -> list[dict]:
    """
    Parse the contents of /proc/mdstat into a list of array dicts.

    Each dict contains:
      name, state, level, sync_pct (float|None), finish_eta (str|None)
    """
    arrays: list[dict] = []
    lines = raw.splitlines()
    i = 0
    while i < len(lines):
        m = _ARRAY_RE.match(lines[i].strip())
        if m:
            entry: dict = {
                "name": m.group("name"),
                "state": m.group("state"),
                "level": m.group("level") or "unknown",
                "sync_pct": None,
                "finish_eta": None,
            }
            # Look ahead for the sync line (appears within the next 3 lines)
            for lookahead in range(1, 4):
                if i + lookahead >= len(lines):
                    break
                sync_m = _SYNC_RE.search(lines[i + lookahead])
                if sync_m:
                    try:
                        entry["sync_pct"] = float(sync_m.group("pct"))
                    except ValueError:
                        pass
                    entry["finish_eta"] = sync_m.group("eta")
                    break
            arrays.append(entry)
        i += 1
    return arrays


def _read_mdstat_sync() -> str:
    try:
        with open("/proc/mdstat", "r", encoding="utf-8") as f:
            return f.read()
    except OSError as exc:
        logger.warning("cannot read /proc/mdstat: %s", exc)
        return ""


# ---------------------------------------------------------------------------
# Reader class
# ---------------------------------------------------------------------------

class MdstatReader:
    """
    Background asyncio task that polls /proc/mdstat and fans parsed data
    out to all registered subscriber queues.
    """

    def __init__(self) -> None:
        self._subscribers: list[asyncio.Queue] = []
        self._task: Optional[asyncio.Task] = None

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop(), name="mdstat-reader")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _poll_loop(self) -> None:
        while True:
            try:
                raw = await asyncio.to_thread(_read_mdstat_sync)
                arrays = parse_mdstat(raw)
                event = {"event": "mdstat_update", "arrays": arrays}
                for q in list(self._subscribers):
                    try:
                        q.put_nowait(event)
                    except asyncio.QueueFull:
                        # Slow consumer — drop oldest item to make room
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        try:
                            q.put_nowait(event)
                        except asyncio.QueueFull:
                            pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("mdstat poll error")

            await asyncio.sleep(POLL_INTERVAL)
