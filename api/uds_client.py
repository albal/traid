"""
Async Unix Domain Socket client for the TRAID worker daemon.

Uses a new connection per request (matching the daemon's connection-per-request
model). For streaming (mdstat_subscribe), callers manage the connection lifetime
themselves via open_stream().
"""

import asyncio
import uuid
import logging

from shared.protocol import send_message, recv_message, ProtocolError

logger = logging.getLogger(__name__)

SOCKET_PATH = "/run/hybrid-storage.sock"
DEFAULT_TIMEOUT = 30.0  # seconds


class WorkerUnavailableError(Exception):
    """Worker socket does not exist or connection was refused."""


class WorkerError(Exception):
    """Worker returned status: error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


async def send_request(action: str, params: dict | None = None, timeout: float = DEFAULT_TIMEOUT) -> dict | list:
    """
    Send a single request to the worker daemon and return the response data.

    Raises:
        WorkerUnavailableError: socket missing or connection refused.
        WorkerError: worker returned an error response.
        ProtocolError: framing violation.
        asyncio.TimeoutError: request took longer than `timeout` seconds.
    """
    params = params or {}
    request = {
        "id": str(uuid.uuid4()),
        "action": action,
        "params": params,
    }

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(SOCKET_PATH),
            timeout=5.0,
        )
    except (FileNotFoundError, ConnectionRefusedError, OSError) as exc:
        raise WorkerUnavailableError(f"cannot connect to worker socket: {exc}") from exc

    try:
        await asyncio.wait_for(send_message(writer, request), timeout=timeout)
        response = await asyncio.wait_for(recv_message(reader), timeout=timeout)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass

    if response.get("status") == "error":
        err = response.get("error") or {}
        raise WorkerError(
            code=err.get("code", "UNKNOWN"),
            message=err.get("message", "worker returned error"),
        )

    return response.get("data") or {}


async def open_stream(action: str, params: dict | None = None) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """
    Open a persistent connection for streaming actions (e.g. mdstat_subscribe).
    The caller is responsible for closing the writer.

    Returns (reader, writer) after consuming the initial ok response.
    Raises WorkerUnavailableError / WorkerError on failure.
    """
    params = params or {}
    request = {
        "id": str(uuid.uuid4()),
        "action": action,
        "params": params,
    }

    try:
        reader, writer = await asyncio.open_unix_connection(SOCKET_PATH)
    except (FileNotFoundError, ConnectionRefusedError, OSError) as exc:
        raise WorkerUnavailableError(f"cannot connect to worker socket: {exc}") from exc

    await send_message(writer, request)
    initial = await recv_message(reader)

    if initial.get("status") == "error":
        writer.close()
        err = initial.get("error") or {}
        raise WorkerError(
            code=err.get("code", "UNKNOWN"),
            message=err.get("message", "worker returned error"),
        )

    return reader, writer
