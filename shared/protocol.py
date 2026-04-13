"""
UDS message framing shared by worker daemon and API client.

Format: [4-byte big-endian uint32 = N][N bytes UTF-8 JSON]

Never uses readline() — length-prefix framing is unambiguous even when
JSON values contain embedded newlines.
"""

import json
import struct
import asyncio

MAX_MESSAGE_BYTES = 10 * 1024 * 1024  # 10 MB sanity cap


class ProtocolError(Exception):
    pass


async def send_message(writer: asyncio.StreamWriter, obj: dict) -> None:
    data = json.dumps(obj).encode("utf-8")
    writer.write(struct.pack(">I", len(data)))
    writer.write(data)
    await writer.drain()


async def recv_message(reader: asyncio.StreamReader) -> dict:
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        raise ProtocolError("connection closed before header") from exc

    (n,) = struct.unpack(">I", header)

    if n == 0:
        raise ProtocolError("zero-length message")
    if n > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"message too large: {n} bytes")

    try:
        body = await reader.readexactly(n)
    except asyncio.IncompleteReadError as exc:
        raise ProtocolError("connection closed mid-message") from exc

    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"invalid JSON: {exc}") from exc
