from __future__ import annotations

import socket
from typing import Any

from .protocol import recv_message, send_message


class ManagerError(RuntimeError):
    pass


class ManagerClient:
    def __init__(
        self,
        host: str,
        port: int,
        client_name: str,
        timeout: float = 5.0,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.client_name = str(client_name).strip()
        if not self.client_name:
            raise ValueError("client_name is required")
        self.timeout = timeout
        self._session: str | None = None
        self._fresh: bool = False
        self.last_meta: dict[str, Any] = {}

    def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
            send_message(sock, payload)
            response = recv_message(sock)

        if response.get("status") != "ok":
            error = response.get("error", "unknown manager error")
            raise ManagerError(str(error))
        return response

    def connect(self, force: bool = False) -> None:
        if self._session:
            return
        response = self._request({"op": "connect", "client": self.client_name, "force": bool(force)})
        session = response.get("session")
        if not session:
            raise ManagerError("manager did not return a session token")
        self._session = str(session)

    def disconnect(self) -> None:
        if not self._session:
            return
        session = self._session
        self._session = None
        self._request(
            {
                "op": "disconnect",
                "client": self.client_name,
                "session": session,
            }
        )

    def close(self) -> None:
        self.disconnect()

    def __enter__(self) -> "ManagerClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    def _with_session(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.connect()
        assert self._session is not None
        data = dict(payload)
        data["client"] = self.client_name
        data["session"] = self._session
        return data

    def acquire(self, resource: str) -> bool:
        response = self._request(self._with_session({"op": "acquire", "resource": resource}))
        return bool(response.get("acquired", False))

    def release(self, resource: str) -> bool:
        response = self._request(self._with_session({"op": "release", "resource": resource}))
        return bool(response.get("released", False))

    def owner_of(self, resource: str) -> str | None:
        response = self._request(self._with_session({"op": "owner_of", "resource": resource}))
        owner = response.get("owner")
        if owner is None:
            return None
        return str(owner)

    def owners_of(self, resources: list[str]) -> dict[str, str | None]:
        response = self._request(self._with_session({"op": "owners_of", "resources": list(resources)}))
        owners = response.get("owners")
        if not isinstance(owners, dict):
            return {}
        result: dict[str, str | None] = {}
        for key, value in owners.items():
            if value is None:
                result[str(key)] = None
            else:
                result[str(key)] = str(value)
        return result

    def set_link_groups(self, groups: list[list[str]]) -> int:
        response = self._request(
            self._with_session({"op": "set_link_groups", "groups": [list(g) for g in groups]})
        )
        return int(response.get("groups", 0))

    def list_link_groups(self) -> dict[str, list[list[str]]]:
        response = self._request(self._with_session({"op": "list_link_groups"}))
        registered = response.get("link_groups")
        return registered if isinstance(registered, dict) else {}

    def set_fresh(self, enabled: bool = True) -> None:
        """Force subsequent reads to bypass the server cache (sticky)."""
        self._fresh = bool(enabled)

    def invoke(
        self,
        function: str,
        args: list[Any],
        kwargs: dict[str, Any],
        resources: list[str],
        handle: str | None = None,
        fresh: bool | None = None,
    ) -> Any:
        payload = self._with_session(
            {
                "op": "call",
                "function": function,
                "args": args,
                "kwargs": kwargs,
                "resources": resources,
            }
        )
        if handle is not None:
            payload["handle"] = handle
        if fresh if fresh is not None else getattr(self, "_fresh", False):
            payload["fresh"] = True
        response = self._request(payload)
        # Freshness metadata of the last read, for callers that poll and want
        # to tell a new device sample from a repeat (compare last_meta['ts']).
        self.last_meta = {"cached": response.get("cached"), "ts": response.get("ts")}
        return response.get("result")
