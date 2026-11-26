"""Round rollover and authenticated user-stream tools.

Every behaviour here is backed by the official SDK 0.20.1 API:
``RolloverWatcher`` (repo code) uses ``AsyncPublicClient.subscribe(MarketSpec)``;
the user stream uses ``AsyncSecureClient.subscribe(UserSpec)`` documented by
Polymarket under Real-Time Order Updates (topic ``user``, events ``order`` and
``trade``). No event is invented and no secret is exposed.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any

from polymarket.streams import UserSpec

from . import client as clientmod
from .config import Settings
from .live import (
    AssetStream,
    RolloverWatcher,
    boundaries,
    get_manager,
    get_shared_client,
)

_USER_EVENTS_MAX = 200
_USER_EVENTS: deque[Any] = deque(maxlen=_USER_EVENTS_MAX)
_watch_task: asyncio.Task | None = None
_watch_stop: asyncio.Event | None = None
_prefetched: set[str] = set()
_user_client: Any = None
_user_handle: Any = None
_user_task: asyncio.Task | None = None
_user_error: str | None = None


async def _round_boundaries() -> dict:
    current, nxt, current_start, next_start = boundaries(int(time.time()))
    return {
        "current_slug": current,
        "next_slug": nxt,
        "current_window_start": current_start,
        "next_window_start": next_start,
        "seconds_to_boundary": current_start + 300 - int(time.time()),
    }


def _round_outcome(value: str) -> str:
    from . import safety

    canonical = safety.validate_outcome(value)
    return "UP" if canonical == "yes" else "DOWN"


async def subscribe_round(slug: str = "", outcome: str = "UP", both: bool = True) -> dict:
    from .server import _resolve_asset_id

    info = await _round_boundaries()
    target = slug or info["current_slug"]
    pub = clientmod.build_public_client()
    try:
        market = await pub.get_market(slug=target)
        outcomes = ("UP", "DOWN") if both else (_round_outcome(outcome),)
        resolved = [(side, *_resolve_asset_id(market, side)) for side in outcomes]
    finally:
        await pub.close()
    manager = get_manager()
    window_start = info["current_window_start"] if target == info["current_slug"] else 0
    if manager._client is None:
        manager._client = await get_shared_client()
    books = []
    for side, asset_id, how in resolved:
        st = manager._streams.get(asset_id)
        if st is None:
            st = AssetStream(asset_id=asset_id, slug=target, window_start=window_start)
            manager._streams[asset_id] = st
        await manager._rest_fallback(st)
        stream = await manager.subscribe(asset_id, slug=target, window_start=window_start)
        books.append({"outcome": side, "asset_id": asset_id, "resolved_by": how,
                      "snapshot": stream.snapshot_dict()})
    global _watch_task, _watch_stop, _prefetched
    if _watch_task is None or _watch_task.done():
        _prefetched = set()
        _watch_stop = asyncio.Event()
        _watch_task = asyncio.create_task(
            RolloverWatcher(manager).watch_loop(stop=_watch_stop, prefetched=_prefetched)
        )
    return {
        "status": "subscribed",
        "slug": target,
        "asset_ids": [book["asset_id"] for book in books],
        "outcomes": [book["outcome"] for book in books],
        "books": books,
        "auto_rollover": True,
        "rest_bootstrap_first": True,
        "boundaries": info,
    }


async def stop_round_watch() -> dict:
    global _watch_task, _watch_stop, _prefetched
    if _watch_stop is not None:
        _watch_stop.set()
    task = _watch_task
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    _watch_task = None
    _watch_stop = None
    _prefetched = set()
    return {"status": "stopped", "auto_rollover": False}


async def round_status() -> dict:
    info = await _round_boundaries()
    manager = get_manager()
    running = _watch_task is not None and not _watch_task.done()
    return {
        "auto_rollover": running,
        "prefetched_slugs": sorted(_prefetched),
        "boundaries": info,
        "manager": manager.status_snapshot(),
    }


async def start_user_stream(markets: list[str] | None = None) -> dict:
    global _user_client, _user_handle, _user_task, _user_error
    if _user_task is not None and not _user_task.done():
        return {"status": "already_running", "events": len(_USER_EVENTS)}
    settings = Settings.from_env()
    client = await clientmod.build_secure_client(settings)
    spec = UserSpec(markets=tuple(markets) if markets else None)
    handle = await client.subscribe(spec)
    _user_client = client
    _user_handle = handle
    _user_error = None
    _user_task = asyncio.create_task(_consume_user_events(handle))
    return {
        "status": "started",
        "topic": "user",
        "market_filter": list(markets) if markets else None,
        "events": len(_USER_EVENTS),
    }


async def _consume_user_events(handle: Any) -> None:
    global _user_error
    try:
        async for event in handle:
            _USER_EVENTS.append(event)
            if len(_USER_EVENTS) > _USER_EVENTS_MAX:
                _USER_EVENTS.popleft()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _user_error = f"{type(exc).__name__}: {exc}"


async def stop_user_stream() -> dict:
    global _user_task, _user_handle, _user_client, _user_error
    task = _user_task
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    handle = _user_handle
    if handle is not None:
        close = getattr(handle, "close", None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result
    client = _user_client
    if client is not None:
        close = getattr(client, "close", None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result
    _user_task = None
    _user_handle = None
    _user_client = None
    _user_error = None
    return {"status": "stopped", "events": len(_USER_EVENTS)}


async def user_stream_status() -> dict:
    return {
        "running": _user_task is not None and not _user_task.done(),
        "topic": "user",
        "events": len(_USER_EVENTS),
        "error": _user_error,
    }


async def user_stream_events(limit: int = 50) -> dict:
    from .server import _to_jsonable

    count = max(1, min(200, int(limit)))
    return {
        "events": [_to_jsonable(event) for event in list(_USER_EVENTS)[-count:]],
        "truncated": len(_USER_EVENTS) > count,
    }
