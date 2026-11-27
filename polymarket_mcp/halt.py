"""Operator HALT latch. Blocks new BUYs only; never SELL, cancel or reads."""
from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path

from . import observability


def path() -> Path:
    return observability._state_dir() / "HALT"


def read() -> dict | None:
    target = path()
    try:
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise PermissionError("HALT file must be private")
        raw = os.read(fd, 4096).decode("utf-8", "replace")
    finally:
        os.close(fd)
    try:
        payload = json.loads(raw)
    except ValueError:
        return {"reason": "unreadable_halt_file"}
    return payload if isinstance(payload, dict) else {"reason": "unreadable_halt_file"}


def write(reason: str, **detail) -> dict:
    directory = observability._state_dir()
    observability._private_dir(directory)
    payload = {"reason": reason, "timestamp": time.time(), **detail}
    target = path()
    temporary = directory / f".HALT.{os.getpid()}.tmp"
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            line = (json.dumps(payload, separators=(",", ":")) + "\n").encode()
            if os.write(fd, line) != len(line):
                raise OSError("Incomplete HALT write")
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)
    return payload
