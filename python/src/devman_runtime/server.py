from __future__ import annotations

import importlib
import importlib.util
import inspect
import itertools
from datetime import datetime
from pathlib import Path
import re
import socketserver
import sys
import time
import traceback
import uuid
from dataclasses import dataclass
from threading import Event, Lock, Thread
from typing import Any

from .db import OwnershipDB
from .protocol import recv_message, send_message

_EXPAND_FIELD_RE = re.compile(r"\{([A-Za-z_]\w*)\[\]\}")

def _expand_resource_template(template: str, context: dict[str, Any]) -> list[str]:
    expand_fields = _EXPAND_FIELD_RE.findall(template)
    if not expand_fields:
        return [template.format(**context)]

    ordered_fields = list(dict.fromkeys(expand_fields))
    normalized = template
    values_by_field: list[list[Any]] = []
    for field in ordered_fields:
        normalized = normalized.replace(f"{{{field}[]}}", f"{{{field}}}")
        raw = context.get(field)
        if raw is None:
            return []
        if isinstance(raw, (str, bytes, bytearray)):
            values = [raw]
        else:
            try:
                values = list(raw)
            except TypeError:
                values = [raw]
        if not values:
            return []
        values_by_field.append(values)

    resources: list[str] = []
    for combo in itertools.product(*values_by_field):
        local_context = dict(context)
        for field, value in zip(ordered_fields, combo):
            local_context[field] = value
        resources.append(normalized.format(**local_context))
    return resources


def _resolve_backend_callable(backend: Any, dotted: str):
    target: Any = backend
    for token in dotted.split("."):
        target = getattr(target, token)
    if not callable(target):
        raise AttributeError(f"{dotted} is not callable")
    return target


def _resolve_file_callable(file_path: str, function_name: str):
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"hook file not found: {file_path}")
    module_name = f"_devman_hook_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import hook file: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fn = getattr(module, function_name, None)
    if not callable(fn):
        raise AttributeError(f"{function_name} is not callable in {file_path}")
    return fn


def _invoke_hook(fn, context: dict[str, Any]) -> Any:
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    if not params:
        return fn()

    accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
    if accepts_var_kw:
        return fn(**context)

    kwargs: dict[str, Any] = {}
    for p in params:
        if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
            if p.name in context:
                kwargs[p.name] = context[p.name]
    if kwargs:
        return fn(**kwargs)

    first = params[0]
    if first.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
        return fn(context)

    return fn()


@dataclass(slots=True)
class RuntimeFunctionSpec:
    name: str
    param_order: list[str]
    param_kinds: dict[str, str]
    resource_template: str | None
    dispatch: str = "default"
    dispatch_target: str | None = None


class ManagerCore:
    def __init__(
        self,
        backend_module: str,
        db_path: str,
        functions: dict[str, RuntimeFunctionSpec],
        singleton_object: Any | None = None,
        verbose: bool = False,
        client_lease_sec: float = 90.0,
    ):
        self.backend = importlib.import_module(backend_module)
        self.db = OwnershipDB(db_path)
        self.functions = functions
        self._singleton_object = singleton_object
        self.verbose = bool(verbose)
        self._handles: dict[str, Any] = {}
        self._handle_owners: dict[str, str] = {}
        self._handles_lock = Lock()
        self._sessions_by_name: dict[str, str] = {}
        self._sessions_by_id: dict[str, str] = {}
        self._sessions_lock = Lock()
        self._singleton_lock = Lock()
        self.client_lease_sec = float(client_lease_sec)
        self._last_seen: dict[str, float] = {}
        self._last_seen_lock = Lock()
        self._started_at = time.monotonic()

    def _call_singleton(self, method_name: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
        target = self._singleton_object
        if target is None:
            raise RuntimeError("singleton dispatch requested but no singleton object is configured")
        with self._singleton_lock:
            method = _resolve_backend_callable(target, method_name)
            return method(*args, **kwargs)

    def _note_client_seen(self, client: str) -> None:
        with self._last_seen_lock:
            self._last_seen[str(client)] = time.monotonic()

    def is_client_live(self, client: str) -> bool:
        """A client is live while its lease keeps renewing.

        Any authenticated request renews the lease. Unknown clients fall back
        to the server start time, giving reconnecting clients a grace period
        of one lease window after a server restart.
        """
        if self.client_lease_sec <= 0:
            return True
        with self._last_seen_lock:
            seen = self._last_seen.get(str(client))
        if seen is None:
            seen = self._started_at
        return (time.monotonic() - seen) <= self.client_lease_sec

    def _resolve_resources(
        self, fn_spec: RuntimeFunctionSpec, args: list[Any], kwargs: dict[str, Any]
    ) -> list[str]:
        if fn_spec.resource_template is None:
            return []

        context = dict(kwargs)
        positional_index = 0
        for name in fn_spec.param_order:
            kind = fn_spec.param_kinds.get(name, "POSITIONAL_OR_KEYWORD")
            if kind in ("POSITIONAL_ONLY", "POSITIONAL_OR_KEYWORD") and positional_index < len(args):
                context.setdefault(name, args[positional_index])
                positional_index += 1

        try:
            return _expand_resource_template(fn_spec.resource_template, context)
        except Exception as exc:
            raise RuntimeError(f"failed to resolve resource template for {fn_spec.name}: {exc}") from exc

    def _resolve_dotted_callable(self, function: str) -> Any:
        target: Any = self.backend
        for token in function.split("."):
            target = getattr(target, token)
        if not callable(target):
            raise AttributeError(f"{function} is not callable")
        return target

    def _get_handle(self, handle: str, client: str) -> Any:
        with self._handles_lock:
            obj = self._handles.get(handle)
            owner = self._handle_owners.get(handle)
        if obj is None:
            raise RuntimeError(f"unknown handle: {handle}")
        if owner is not None and owner != client:
            raise RuntimeError(f"handle '{handle}' is owned by '{owner}'")
        return obj

    def _register_handle(self, obj: Any, owner: str) -> str:
        handle = uuid.uuid4().hex
        with self._handles_lock:
            self._handles[handle] = obj
            self._handle_owners[handle] = owner
        return handle

    def _release_handle(self, handle: str) -> None:
        with self._handles_lock:
            self._handles.pop(handle, None)
            self._handle_owners.pop(handle, None)

    def _release_client_handles(self, client: str) -> None:
        to_close: list[Any] = []
        with self._handles_lock:
            for handle, owner in list(self._handle_owners.items()):
                if owner != client:
                    continue
                self._handle_owners.pop(handle, None)
                obj = self._handles.pop(handle, None)
                if obj is not None:
                    to_close.append(obj)
        for obj in to_close:
            close_fn = getattr(obj, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception:
                    pass

    def _connect_client(self, client: str, force: bool = False) -> str:
        with self._sessions_lock:
            if client in self._sessions_by_name:
                # Reject only if the existing session is still live; a
                # lease-expired session (crashed / ungracefully closed client)
                # is evicted so the name can reconnect without --force.
                if not force and self.is_client_live(client):
                    raise RuntimeError(f"client '{client}' is already connected")
                old_session = self._sessions_by_name[client]
                self._sessions_by_name.pop(client, None)
                self._sessions_by_id.pop(old_session, None)
                if not force:
                    self._log(f"evicted stale session for '{client}' (lease expired) on reconnect")
            session = uuid.uuid4().hex
            self._sessions_by_name[client] = session
            self._sessions_by_id[session] = client
        return session

    def _disconnect_client(self, client: str, session: str) -> None:
        with self._sessions_lock:
            active_session = self._sessions_by_name.get(client)
            if active_session != session:
                raise RuntimeError("invalid session")
            self._sessions_by_name.pop(client, None)
            self._sessions_by_id.pop(session, None)

    def _ensure_connected(self, client: str, session: str | None) -> None:
        if not session:
            raise RuntimeError("missing session")
        with self._sessions_lock:
            active_session = self._sessions_by_name.get(client)
            if active_session != session:
                raise RuntimeError(f"client '{client}' is not connected")

    def _log(self, message: str) -> None:
        if self.verbose:
            ts = datetime.now().isoformat(timespec="seconds")
            print(f"[devman {ts}] {message}", file=sys.stderr, flush=True)

    def _log_request(self, client: str, request: dict[str, Any]) -> None:
        if not self.verbose:
            return
        op = request.get("op")
        if op == "call":
            function = request.get("function")
            args = request.get("args", [])
            kwargs = request.get("kwargs", {})
            resources = request.get("resources")
            handle = request.get("handle")
            self._log(
                f"client={client} op=call function={function} handle={handle} "
                f"args={args!r} kwargs={kwargs!r} resources={resources!r}"
            )
        elif op in ("acquire", "release", "owner_of"):
            self._log(f"client={client} op={op} resource={request.get('resource')!r}")
        elif op == "owners_of":
            resources = request.get("resources")
            count = len(resources) if isinstance(resources, list) else "?"
            self._log(f"client={client} op=owners_of count={count}")
        elif op in ("connect", "disconnect"):
            self._log(f"client={client} op={op}")
        else:
            self._log(f"client={client} op={op} request={request!r}")

    def _dispatch(
        self,
        fn_spec: RuntimeFunctionSpec,
        function: str,
        args: list[Any],
        kwargs: dict[str, Any],
        handle: str | None,
        client: str,
    ) -> Any:
        if fn_spec.dispatch == "singleton":
            method_name = fn_spec.dispatch_target or function
            return self._call_singleton(method_name, args, kwargs)

        if function == "Device_open":
            device_cls = getattr(self.backend, "Device")
            obj = device_cls.open(*args, **kwargs)
            return {"__devman_handle__": self._register_handle(obj, owner=client)}

        if function.startswith("Device_"):
            method_name = function[len("Device_") :]
            if handle is not None:
                target = self._get_handle(handle, client=client)
            else:
                target = getattr(self.backend, "Device")
            method = getattr(target, method_name)
            result = method(*args, **kwargs)
            if method_name == "close" and handle is not None:
                self._release_handle(handle)
            return result

        backend_fn = self._resolve_dotted_callable(function)
        return backend_fn(*args, **kwargs)

    def handle(self, request: dict[str, Any]) -> dict[str, Any]:
        op = request.get("op")
        client = request.get("client")
        if not client:
            return {"status": "error", "error": "missing client name"}
        client_name = str(client)
        self._log_request(client_name, request)
        session = request.get("session")

        if op == "connect":
            try:
                connected_session = self._connect_client(client_name, force=bool(request.get("force", False)))
            except Exception as exc:
                return {"status": "error", "error": str(exc)}
            self._note_client_seen(client_name)
            return {"status": "ok", "session": connected_session}

        if op == "disconnect":
            try:
                self._disconnect_client(client_name, str(session) if session else "")
            except Exception as exc:
                return {"status": "error", "error": str(exc)}
            return {"status": "ok", "disconnected": True}

        try:
            self._ensure_connected(client_name, str(session) if session else None)
        except Exception as exc:
            return {"status": "error", "error": str(exc)}
        self._note_client_seen(client_name)

        if op == "acquire":
            resource = request.get("resource")
            if not resource:
                return {"status": "error", "error": "missing resource"}
            return {"status": "ok", "acquired": self.db.acquire(str(resource), client_name)}

        if op == "release":
            resource = request.get("resource")
            if not resource:
                return {"status": "error", "error": "missing resource"}
            return {"status": "ok", "released": self.db.release(str(resource), client_name)}

        if op == "owner_of":
            resource = request.get("resource")
            if not resource:
                return {"status": "error", "error": "missing resource"}
            return {"status": "ok", "owner": self.db.owner_of(str(resource))}

        if op == "owners_of":
            resources = request.get("resources")
            if not isinstance(resources, list):
                return {"status": "error", "error": "resources must be a list"}
            owners = {str(resource): self.db.owner_of(str(resource)) for resource in resources}
            return {"status": "ok", "owners": owners}

        if op == "set_link_groups":
            groups = request.get("groups")
            if not isinstance(groups, list) or not all(isinstance(g, list) for g in groups):
                return {"status": "error", "error": "groups must be a list of resource lists"}
            try:
                count = self.db.set_link_groups(client_name, [[str(r) for r in g] for g in groups])
            except Exception as exc:
                return {"status": "error", "error": f"failed to store link groups: {exc}"}
            return {"status": "ok", "groups": count}

        if op == "list_link_groups":
            try:
                registered = self.db.all_link_groups()
            except Exception as exc:
                return {"status": "error", "error": f"failed to load link groups: {exc}"}
            return {"status": "ok", "link_groups": registered}

        if op != "call":
            return {"status": "error", "error": f"unsupported operation: {op}"}

        function = request.get("function")
        if not function:
            return {"status": "error", "error": "missing function"}
        fn_spec = self.functions.get(str(function))
        if fn_spec is None:
            return {"status": "error", "error": f"unknown function: {function}"}

        args = request.get("args", [])
        kwargs = request.get("kwargs", {})
        handle = request.get("handle")
        resources = request.get("resources")
        if resources is None:
            resources = self._resolve_resources(fn_spec, list(args), dict(kwargs))

        for resource in resources:
            owner = self.db.owner_of(str(resource))
            if owner != client_name:
                return {
                    "status": "error",
                    "error": f"resource '{resource}' is owned by '{owner}'",
                }

        try:
            result = self._dispatch(
                fn_spec,
                str(function),
                list(args),
                dict(kwargs),
                handle=str(handle) if handle else None,
                client=client_name,
            )
        except Exception:
            return {
                "status": "error",
                "error": f"backend call failed: {traceback.format_exc(limit=2)}",
            }
        return {"status": "ok", "result": result}

    def shutdown(self) -> None:
        with self._sessions_lock:
            clients = list(self._sessions_by_name.keys())
            self._sessions_by_name.clear()
            self._sessions_by_id.clear()
        for client in clients:
            self._release_client_handles(client)
        self.db.close()


class _TCPHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        assert isinstance(self.server, _ManagerTCPServer)
        request = recv_message(self.request)
        response = self.server.core.handle(request)
        send_message(self.request, response)


class _ManagerTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], core: ManagerCore):
        super().__init__(server_address, _TCPHandler)
        self.core = core


def serve_manager(
    backend_module: str,
    host: str,
    port: int,
    db_path: str,
    functions: dict[str, RuntimeFunctionSpec],
    init_function: str | None = None,
    deinit_function: str | None = None,
    init_file: str | None = None,
    deinit_file: str | None = None,
    init_file_function: str = "init",
    deinit_file_function: str = "deinit",
    hook_args: dict[str, str] | None = None,
    extra_args: list[str] | None = None,
    periodic_function: str | None = None,
    periodic_file: str | None = None,
    periodic_file_function: str = "periodic",
    periodic_interval_sec: float = 0.0,
    singleton_function: str | None = None,
    singleton_file: str | None = None,
    singleton_file_function: str = "get_singleton",
    verbose: bool = False,
    client_lease_sec: float = 90.0,
) -> None:
    if init_file and init_function:
        raise ValueError("init_file and init_function are mutually exclusive")
    if deinit_file and deinit_function:
        raise ValueError("deinit_file and deinit_function are mutually exclusive")
    if periodic_file and periodic_function:
        raise ValueError("periodic_file and periodic_function are mutually exclusive")
    if singleton_file and singleton_function:
        raise ValueError("singleton_file and singleton_function are mutually exclusive")

    core = ManagerCore(
        backend_module=backend_module,
        db_path=db_path,
        functions=functions,
        verbose=verbose,
        client_lease_sec=client_lease_sec,
    )
    hook_context: dict[str, Any] = {
        "backend": core.backend,
        "backend_module": backend_module,
        "host": host,
        "port": port,
        "db_path": db_path,
        "hook_args": dict(hook_args or {}),
        "extra_args": list(extra_args or []),
        # Device-specific extensions (e.g. safety monitors in hook files)
        # can use core.db, core.is_client_live, and the ownership/link
        # registry through this reference.
        "core": core,
    }
    periodic_thread: Thread | None = None
    periodic_stop = Event()
    try:
        init_result: Any = None
        init_cb = None
        if init_file:
            init_cb = _resolve_file_callable(init_file, init_file_function)
        elif init_function:
            init_cb = _resolve_backend_callable(core.backend, init_function)
        if init_cb:
            init_result = _invoke_hook(init_cb, hook_context)
            hook_context["init_result"] = init_result

        singleton_cb = None
        if singleton_file:
            singleton_cb = _resolve_file_callable(singleton_file, singleton_file_function)
        elif singleton_function:
            singleton_cb = _resolve_backend_callable(core.backend, singleton_function)

        if singleton_cb:
            core._singleton_object = _invoke_hook(singleton_cb, hook_context)
        elif init_result is not None:
            core._singleton_object = init_result
        hook_context["singleton"] = core._singleton_object

        periodic_cb = None
        if periodic_file:
            periodic_cb = _resolve_file_callable(periodic_file, periodic_file_function)
        elif periodic_function:
            periodic_cb = _resolve_backend_callable(core.backend, periodic_function)

        interval_sec = float(periodic_interval_sec or 0.0)
        if periodic_cb and interval_sec > 0.0:
            def _periodic_loop() -> None:
                while not periodic_stop.wait(interval_sec):
                    try:
                        _invoke_hook(periodic_cb, hook_context)
                    except Exception as exc:
                        core._log(f"periodic hook failed: {exc}")

            periodic_thread = Thread(target=_periodic_loop, name="devman-periodic-hook", daemon=True)
            periodic_thread.start()
            core._log(f"periodic hook started interval={interval_sec:.3f}s")

        with _ManagerTCPServer((host, port), core) as server:
            server.serve_forever()
    finally:
        periodic_stop.set()
        if periodic_thread is not None:
            periodic_thread.join(timeout=2.0)
        try:
            deinit_cb = None
            if deinit_file:
                deinit_cb = _resolve_file_callable(deinit_file, deinit_file_function)
            elif deinit_function:
                deinit_cb = _resolve_backend_callable(core.backend, deinit_function)
            if deinit_cb:
                _invoke_hook(deinit_cb, hook_context)
        finally:
            core.shutdown()
