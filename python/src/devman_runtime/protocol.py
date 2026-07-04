from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
import json
import socket
import struct
from typing import Any


class ProtocolError(RuntimeError):
    pass


def _json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "__dict__"):
        return vars(value)
    return str(value)


def send_message(sock: socket.socket, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, default=_json_default, separators=(",", ":")).encode("utf-8")
    sock.sendall(struct.pack("!I", len(body)))
    sock.sendall(body)


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ProtocolError("unexpected EOF while reading message")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_message(sock: socket.socket) -> dict[str, Any]:
    header = _recv_exact(sock, 4)
    (size,) = struct.unpack("!I", header)
    raw = _recv_exact(sock, size)
    try:
        data = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"invalid message JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ProtocolError("message must be a JSON object")
    return data
