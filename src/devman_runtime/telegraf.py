"""Minimal InfluxDB line-protocol sender for Telegraf.

Supports Telegraf's socket_listener (udp://host:port) and
influxdb_listener / http_listener_v2 (http://host:port[/path]) inputs.
Stdlib only; senders are cheap to construct and hold no open connection
except the UDP socket.
"""

from __future__ import annotations

import socket
import urllib.request
from typing import Any
from urllib.parse import urlparse


def _escape_measurement(value: str) -> str:
    return value.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ")


def _escape_tag(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace(" ", "\\ ")
    )


def _format_field_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    if isinstance(value, float):
        return repr(float(value))
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def build_line(
    measurement: str,
    tags: dict[str, Any],
    fields: dict[str, Any],
    timestamp_ns: int | None = None,
) -> str:
    """Build one line-protocol record; fields must be non-empty."""
    if not fields:
        raise ValueError("line protocol requires at least one field")
    parts = [_escape_measurement(str(measurement))]
    for key in sorted(tags):
        value = tags[key]
        if value is None or str(value) == "":
            continue
        parts.append(f"{_escape_tag(str(key))}={_escape_tag(str(value))}")
    head = ",".join(parts)
    body = ",".join(
        f"{_escape_tag(str(key))}={_format_field_value(value)}"
        for key, value in sorted(fields.items())
        if value is not None
    )
    line = f"{head} {body}"
    if timestamp_ns is not None:
        line += f" {int(timestamp_ns)}"
    return line


class TelegrafSender:
    """Send line-protocol batches to Telegraf over UDP or HTTP."""

    def __init__(self, url: str, timeout: float = 2.0) -> None:
        parsed = urlparse(str(url))
        scheme = (parsed.scheme or "").lower()
        if scheme not in ("udp", "http", "https"):
            raise ValueError(f"unsupported telegraf url scheme: {url!r} (use udp:// or http(s)://)")
        if not parsed.hostname or not parsed.port:
            raise ValueError(f"telegraf url must include host and port: {url!r}")
        self.url = str(url)
        self.scheme = scheme
        self.host = parsed.hostname
        self.port = int(parsed.port)
        self.timeout = float(timeout)
        self._http_url: str | None = None
        self._socket: socket.socket | None = None
        if scheme == "udp":
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            path = parsed.path if parsed.path and parsed.path != "/" else "/write"
            self._http_url = f"{scheme}://{parsed.hostname}:{parsed.port}{path}"

    def send_lines(self, lines: list[str]) -> None:
        payload = "\n".join(line for line in lines if line)
        if not payload:
            return
        data = (payload + "\n").encode("utf-8")
        if self._socket is not None:
            # UDP datagrams should stay well under typical MTU-ish limits;
            # send line-by-line batches of moderate size.
            batch: list[str] = []
            size = 0
            for line in payload.split("\n"):
                encoded = len(line.encode("utf-8")) + 1
                if batch and size + encoded > 60000:
                    self._socket.sendto(("\n".join(batch) + "\n").encode("utf-8"), (self.host, self.port))
                    batch, size = [], 0
                batch.append(line)
                size += encoded
            if batch:
                self._socket.sendto(("\n".join(batch) + "\n").encode("utf-8"), (self.host, self.port))
            return
        request = urllib.request.Request(
            self._http_url or self.url,
            data=data,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout):
            pass

    def close(self) -> None:
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None
