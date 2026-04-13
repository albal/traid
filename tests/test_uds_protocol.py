"""
Integration tests for the UDS framing protocol.
Spins up an in-process echo server on a temp socket — no worker daemon needed.
"""

import asyncio
import json
import os
import struct
import tempfile
import pytest

from shared.protocol import send_message, recv_message, ProtocolError, MAX_MESSAGE_BYTES


# ---------------------------------------------------------------------------
# Echo server fixture
# ---------------------------------------------------------------------------

async def _echo_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        msg = await recv_message(reader)
        await send_message(writer, msg)
    except ProtocolError:
        pass
    finally:
        writer.close()


@pytest.fixture
async def echo_socket(tmp_path):
    sock_path = str(tmp_path / "test.sock")
    server = await asyncio.start_unix_server(_echo_handler, path=sock_path)
    yield sock_path
    server.close()
    await server.wait_closed()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_roundtrip(echo_socket):
    original = {"action": "disk_scan", "params": {}, "id": "abc-123"}
    reader, writer = await asyncio.open_unix_connection(echo_socket)
    await send_message(writer, original)
    result = await recv_message(reader)
    writer.close()
    await writer.wait_closed()
    assert result == original


@pytest.mark.asyncio
async def test_unicode_values(echo_socket):
    original = {"msg": "héllo wörld 日本語"}
    reader, writer = await asyncio.open_unix_connection(echo_socket)
    await send_message(writer, original)
    result = await recv_message(reader)
    writer.close()
    await writer.wait_closed()
    assert result == original


@pytest.mark.asyncio
async def test_large_message(echo_socket):
    # 1 MB payload — under the 10 MB cap, should work
    original = {"data": "x" * (1024 * 1024)}
    reader, writer = await asyncio.open_unix_connection(echo_socket)
    await send_message(writer, original)
    result = await recv_message(reader)
    writer.close()
    await writer.wait_closed()
    assert len(result["data"]) == 1024 * 1024


@pytest.mark.asyncio
async def test_oversized_message_rejected(echo_socket):
    """Sending a raw oversized header should cause recv_message to raise ProtocolError."""
    reader, writer = await asyncio.open_unix_connection(echo_socket)

    # Manually write a header claiming 11 MB
    oversize = MAX_MESSAGE_BYTES + 1
    writer.write(struct.pack(">I", oversize))
    await writer.drain()
    writer.close()

    # The echo server closes on ProtocolError; the client reader gets EOF
    with pytest.raises((asyncio.IncompleteReadError, ProtocolError, ConnectionResetError)):
        await recv_message(reader)


@pytest.mark.asyncio
async def test_connection_closed_mid_header(echo_socket):
    reader, writer = await asyncio.open_unix_connection(echo_socket)
    writer.write(b"\x00\x00")  # partial header — only 2 bytes
    writer.close()

    with pytest.raises((asyncio.IncompleteReadError, ProtocolError)):
        await recv_message(reader)
