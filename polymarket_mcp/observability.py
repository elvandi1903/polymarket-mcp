"""Local guard audit and MCP event-loop watchdog."""
from __future__ import annotations

import asyncio
import json
import os
import stat
import tempfile
import time
from collections import deque
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from uuid import uuid4

from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder

from .safety import ValidationError

HEARTBEAT_INTERVAL_S = 10


def _state_dir() -> Path:
    return Path(os.environ.get("POLY_GUARD_STATE_DIR") or "~/.local/state/polymarket-mcp").expanduser()


def _private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise PermissionError("Guard state directory must be private")


class GuardState:
    def __init__(self) -> None:
        self.recent: deque[dict] = deque(maxlen=10)
        self.audit_failures = 0
        self.audit_ok: bool | None = None
        self.heartbeat_failures = 0
        self.heartbeat_ok: bool | None = None
        self.heartbeat_at: float | None = None

    def _append(self, row: dict) -> None:
        directory = _state_dir()
        _private_dir(directory)
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK
        fd = os.open(directory / "guard-audit.jsonl", flags, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1):
                raise PermissionError("Guard audit file must be private")
            line = (json.dumps(row, separators=(",", ":")) + "\n").encode()
            if os.write(fd, line) != len(line):
                raise OSError("Incomplete audit write")
            os.fsync(fd)
        finally:
            os.close(fd)

    def record(self, request_id: str, tool: str, event: str, outcome: str,
               index: int | None = None, detail: dict | None = None) -> bool:
        row = {
            "timestamp": time.time(), "request_id": request_id, "tool": tool,
            "event": event, "outcome": outcome,
        }
        if index is not None:
            row["index"] = index
        if detail:
            row["detail"] = detail
        try:
            self._append(row)
        except (OSError, RuntimeError, ValueError):
            self.audit_failures += 1
            self.audit_ok = False
        else:
            self.audit_ok = True
        if event == "decision":
            self.recent.append({**row, "outcome": "audit_unavailable"} if outcome == "allowed" and not self.audit_ok else row)
        return self.audit_ok

    def status(self) -> dict:
        return {
            "recent_decisions": list(self.recent),
            "audit": {"ok": self.audit_ok, "failures": self.audit_failures},
            "heartbeat": {
                "ok": self.heartbeat_ok, "failures": self.heartbeat_failures,
                "timestamp": self.heartbeat_at, "interval_s": HEARTBEAT_INTERVAL_S,
            },
        }

    def heartbeat(self) -> None:
        stamp = time.time()
        temporary = None
        try:
            directory = _state_dir()
            _private_dir(directory)
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, delete=False) as handle:
                temporary = Path(handle.name)
                json.dump({"pid": os.getpid(), "timestamp": stamp}, handle)
            temporary.replace(directory / "guard-heartbeat.json")
        except (OSError, RuntimeError, ValueError):
            self.heartbeat_failures += 1
            self.heartbeat_ok = False
        else:
            self.heartbeat_at = stamp
            self.heartbeat_ok = True
        finally:
            if temporary is not None:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)


state = GuardState()


@dataclass
class _AuditCall:
    request_id: str
    tool: str
    decided: bool = False
    authorized: bool = False
    submit_started: bool = False

    def record(self, event: str, outcome: str, index: int | None = None,
               detail: dict | None = None) -> bool:
        return state.record(self.request_id, self.tool, event, outcome, index, detail)


_call: ContextVar[_AuditCall] = ContextVar("guard_audit_call")


def audit_tool(func):
    @wraps(func)
    async def wrapped(*args, **kwargs):
        call = _AuditCall(uuid4().hex, func.__name__)
        token = _call.set(call)
        try:
            result = await func(*args, **kwargs)
            if not call.decided:
                decision("dry_run")
            return result
        except (Exception, asyncio.CancelledError) as exc:
            if not call.decided:
                decision("rejected" if isinstance(exc, ValidationError) else "error")
            elif call.authorized and not call.submit_started:
                call.record("order", "not_submitted")
            raise
        finally:
            _call.reset(token)

    return wrapped


def decision(outcome: str) -> None:
    call = _call.get()
    call.decided = True
    ok = call.record("decision", outcome)
    if outcome == "allowed":
        if not ok:
            raise ValidationError("Guard audit unavailable; live operation refused")
        call.authorized = True


def _result_outcome(response) -> str:
    if isinstance(response, AcceptedOrder) and response.status in ("live", "matched", "delayed"):
        return response.status
    if isinstance(response, RejectedOrder):
        if response.code in ("unmatched", "not_enough_balance", "fak_not_filled", "fok_not_filled"):
            return response.code
        return "unknown" if response.code == "unknown" else "rejected"
    return "response_received"


def _response_detail(response) -> dict | None:
    """Machine-readable outcome only.

    The venue's free-text ``message`` is deliberately excluded: it is
    remote-controlled text that could echo identifiers, and the repo's leak
    regression forbids it in the audit log.
    """
    if isinstance(response, RejectedOrder):
        return {"code": str(response.code)}
    if isinstance(response, AcceptedOrder):
        return {"status": str(response.status), "trade_ids": len(response.trade_ids)}
    return None


async def submit(method, *args, **kwargs):
    call = _call.get()
    if not call.authorized or not call.record("order", "submit_started"):
        if call.authorized:
            decision("audit_unavailable")
        raise ValidationError("Guard audit unavailable; live operation refused")
    call.submit_started = True
    try:
        response = await method(*args, **kwargs)
    except (Exception, asyncio.CancelledError):
        call.record("order", "unknown")
        raise
    if isinstance(response, (tuple, list)) and response:
        for index, item in enumerate(response):
            call.record("order", _result_outcome(item), index, _response_detail(item))
    else:
        call.record("order", _result_outcome(response), detail=_response_detail(response))
    return response


async def run_with_heartbeat(transport, **kwargs) -> None:
    async def pulse() -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)
            state.heartbeat()

    state.heartbeat()
    task = asyncio.create_task(pulse())
    try:
        await transport(**kwargs)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
