"""
WebSocket connection pool and mdstat stream relay.

Maintains a set of active browser WebSocket connections and a single
persistent UDS connection to the worker's mdstat_subscribe stream.
Reconnects to the worker with exponential backoff (max 30s) on failure.
"""

import asyncio
import logging

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from api import uds_client
from shared.protocol import recv_message, ProtocolError

logger = logging.getLogger(__name__)

_MIN_BACKOFF = 1.0
_MAX_BACKOFF = 30.0


class WebSocketManager:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._stream_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self, ws: WebSocket) -> None:
        self._connections.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)

    async def broadcast(self, data: dict) -> None:
        dead: set[WebSocket] = set()
        for ws in list(self._connections):
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_json(data)
            except Exception:
                dead.add(ws)
        self._connections -= dead

    # ------------------------------------------------------------------
    # mdstat stream relay
    # ------------------------------------------------------------------

    async def start_mdstat_stream(self) -> None:
        self._stream_task = asyncio.create_task(
            self._stream_loop(), name="mdstat-relay"
        )

    async def shutdown(self) -> None:
        if self._stream_task:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass

    async def _stream_loop(self) -> None:
        backoff = _MIN_BACKOFF
        while True:
            writer = None
            try:
                reader, writer = await uds_client.open_stream("mdstat_subscribe")
                backoff = _MIN_BACKOFF  # reset on successful connection
                logger.info("mdstat stream connected")

                while True:
                    event = await recv_message(reader)
                    if self._connections:
                        await self.broadcast(event)

            except asyncio.CancelledError:
                raise
            except (uds_client.WorkerUnavailableError, uds_client.WorkerError, ProtocolError, OSError) as exc:
                logger.warning("mdstat stream error: %s — retrying in %.0fs", exc, backoff)
            except Exception:
                logger.exception("mdstat stream unexpected error — retrying in %.0fs", backoff)
            finally:
                if writer is not None:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except OSError:
                        pass

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)
