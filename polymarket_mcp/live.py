"""Live order-book streaming over the official SDK WebSocket (read-only).

Durability design (all facts taken from the SDK's own source + docs, no
assumptions):

* One AsyncPublicClient per process in ``LazyPublicClient`` is shared by
  REST reads and the unbounded reader task. The SDK's
  ``ClobMarketStreamManager`` multiplexes many MarketSpec handles onto one
  socket; a second ``subscribe()`` on the same open socket only sends a diff
  frame — it does NOT reconnect. Confirmed by live probe (both the current
  and the next BTC 5m book were returned on one subscription).
* Socket loss is recovered by the SDK's ReconnectScheduler (jittered
  exponential backoff, base 0.25s cap 30s). App-level PING every 10s and a
  30s stale watchdog live in the SDK. We do not duplicate them.
* Our add-on — the part the SDK does NOT own — is DATA freshness: the SDK
  will happily keep a socket alive while the book stops moving. We therefore
  give every asset a watchdog with explicit ``availability`` semantics and
  force a resync when events go quiet, drop, or go out of order.
* Market events carry no sequence number (that field exists only on the
  PolyBolt reference-price channel). We never invent ordering: we track
  ``payload.timestamp`` only, and event timestamps are in the past/equal for
  streaks — a monotonicity violation is dropped and counted.

Rollover (BTC 5m): the series rolls every 300s with invariant slug
``btc-updown-5m-{epoch//300*300}``. ``RolloverWatcher`` prefetches the next
window's asset_id long before the boundary and adds it to the SAME socket via
a second ``subscribe()`` call, so the WS is never torn down at the boundary.
"""

from __future__ import annotations

import asyncio
import decimal
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from polymarket.streams import MarketSpec

from . import client as clientmod
from .server import _resolve_asset_id  # reuse the one canonical resolver

log = logging.getLogger("polymarket_mcp.live")

# ---- defaults (see design doc; all env-overridable) -----------------------
FRESH_MS = 2000
STALE_MS = 15000
NO_EVENT_MS = 8000
BUFFER_EVENTS = 200
FALLBACK_COOLDOWN_S = 2.0
WATCHDOG_TICK_S = 5.0
PREFETCH_LEAD_S = 30.0
SUBSCRIBE_TIMEOUT_S = 2.0
CUSTOM_FEATURE_ENABLED = True
UPDOWN_SLUG_PREFIX = "btc-updown-5m-"


def _env_int(name: str, default: int) -> int:
    import os

    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    import os

    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


DEFAULT_CONFIG = {
    "fresh_ms": _env_int("POLY_LIVE_FRESH_MS", FRESH_MS),
    "stale_ms": _env_int("POLY_LIVE_STALE_MS", STALE_MS),
    "no_event_ms": _env_int("POLY_LIVE_NO_EVENT_MS", NO_EVENT_MS),
    "buffer_events": _env_int("POLY_LIVE_BUFFER_EVENTS", BUFFER_EVENTS),
    "fallback_cooldown_s": _env_float("POLY_LIVE_FALLBACK_COOLDOWN_S", FALLBACK_COOLDOWN_S),
    "watchdog_tick_s": _env_float("POLY_LIVE_WATCHDOG_TICK_S", WATCHDOG_TICK_S),
    "prefetch_lead_s": _env_float("POLY_LIVE_PREFETCH_LEAD_S", PREFETCH_LEAD_S),
    "subscribe_timeout_s": _env_float("POLY_LIVE_SUBSCRIBE_TIMEOUT_S", SUBSCRIBE_TIMEOUT_S),
    "custom_feature_enabled": True,
}


class LegsContextError(RuntimeError):
    pass


def _now_ms() -> int:
    return int(time.time() * 1000)


def _event_ts_ms(payload: Any) -> int | None:
    ts = getattr(payload, "timestamp", None)
    if ts is None:
        return None
    if isinstance(ts, datetime):
        z = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
        return int(z.timestamp() * 1000)
    try:
        return int(ts)  # epoch ms
    except (TypeError, ValueError):
        return None


def _dec_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, decimal.Decimal):
        return str(v)
    return str(v)


@dataclass
class AssetStream:
    """Per-asset durable state. snapshot is the ONLY authoritative book."""

    asset_id: str
    slug: str = ""
    window_start: int = 0
    min_order_size: str | None = None
    tick_size: str | None = None
    neg_risk: bool | None = None

    connected_at_ms: int = 0
    last_event_at_ms: int = 0        # local receipt time of last event
    last_payload_ts_ms: int = 0      # server ts of last APPLIED event
    book: dict | None = None         # snapshot + applied price_change deltas
    book_source: str | None = None
    # Local receipt of the event that produced the current book projection.
    # last_event_at_ms is the last event of ANY type; a stream of deltas can
    # keep that fresh while the book itself is old, so freshness must use this.
    book_received_at_ms: int = 0
    best_bid_ask: dict | None = None
    last_trade: dict | None = None
    last_applied_ts_ms: int = 0

    events_seen: int = 0
    reconnections: int = 0
    out_of_order_drops: int = 0
    invalidated: int = 0
    last_drop_count: int = 0

    ring: deque = field(default_factory=lambda: deque(maxlen=BUFFER_EVENTS))

    def age_ms(self, now_ms: int | None = None) -> int:
        now = now_ms if now_ms is not None else _now_ms()
        return now - self.last_event_at_ms if self.last_event_at_ms else 10**15

    def apply_book(self, payload: Any, local_now_ms: int, source: str = "clob_ws") -> None:
        if source not in ("clob_ws", "clob_rest_snapshot"):
            raise ValueError("invalid_book_source")
        bids = getattr(payload, "bids", ()) or ()
        asks = getattr(payload, "asks", ()) or ()
        self.book = {
            "bids": [{"price": _dec_str(x.price), "size": _dec_str(x.size)} for x in bids],
            "asks": [{"price": _dec_str(x.price), "size": _dec_str(x.size)} for x in asks],
            "hash": getattr(payload, "hash", None),
            "min_order_size": _dec_str(getattr(payload, "min_order_size", None)),
            "tick_size": _dec_str(getattr(payload, "tick_size", None)),
            "neg_risk": getattr(payload, "neg_risk", None),
            "last_trade_price": _dec_str(getattr(payload, "last_trade_price", None)),
            "timestamp": getattr(payload, "timestamp", None),
        }
        self.book_source = source
        self.book_received_at_ms = local_now_ms

    def book_age_ms(self, now_ms: int | None = None) -> int:
        """Age of the book projection, not of the last arbitrary event."""
        now = now_ms if now_ms is not None else _now_ms()
        if self.book is not None and self.book_received_at_ms:
            return now - self.book_received_at_ms
        return self.age_ms(now)

    @staticmethod
    def _level_map(rows: Any) -> dict:
        levels: dict = {}
        for row in rows or ():
            try:
                levels[decimal.Decimal(str(row["price"]))] = decimal.Decimal(
                    str(row["size"])
                )
            except (KeyError, TypeError, decimal.InvalidOperation):
                continue
        return levels

    def apply_price_changes(self, payload: Any, local_now_ms: int) -> None:
        """Apply the venue's incremental book deltas (official contract).

        ``price_change`` carries side/price/size per level; size 0 removes the
        level. The wire ``book`` message is only a snapshot, so without this
        the exposed depth stays frozen while the market moves.
        """
        if not isinstance(self.book, dict) or self.book_source != "clob_ws":
            return
        bids = self._level_map(self.book.get("bids"))
        asks = self._level_map(self.book.get("asks"))
        changes = getattr(payload, "price_changes", ()) or ()
        for change in changes:
            change_asset = getattr(change, "asset_id", None)
            if change_asset is not None and str(change_asset) != self.asset_id:
                continue
            side = str(getattr(change, "side", "")).upper()
            if side not in ("BUY", "SELL"):
                continue
            try:
                price = decimal.Decimal(str(change.price))
                size = decimal.Decimal(str(change.size))
            except (TypeError, ValueError, decimal.InvalidOperation):
                continue
            if not decimal.Decimal(0) <= price <= 1 or size < 0:
                continue
            levels = bids if side == "BUY" else asks
            if size == 0:
                levels.pop(price, None)
            else:
                levels[price] = size
        self.book["bids"] = [
            {"price": str(price), "size": str(size)}
            for price, size in sorted(bids.items())
        ]
        self.book["asks"] = [
            {"price": str(price), "size": str(size)}
            for price, size in sorted(asks.items(), reverse=True)
        ]
        timestamp = getattr(payload, "timestamp", None)
        if timestamp is not None:
            self.book["timestamp"] = timestamp
        for change in changes:
            if getattr(change, "hash", None):
                self.book["hash"] = change.hash
        # Even a no-op delta is proof the projected book still matches the
        # venue, so the receipt advances with every applied delta.
        self.book_received_at_ms = local_now_ms

    def availability(self, now_ms: int | None = None, cfg: dict | None = None) -> str:
        cfg = cfg or DEFAULT_CONFIG
        age = self.book_age_ms(now_ms)
        if age <= cfg["fresh_ms"]:
            return "live"
        if age < cfg["stale_ms"]:
            return "aging"
        return "stale"

    def is_stale(self, now_ms: int | None = None, cfg: dict | None = None) -> bool:
        cfg = cfg or DEFAULT_CONFIG
        age = self.book_age_ms(now_ms)
        return age > cfg["stale_ms"] or age > cfg["no_event_ms"]

    def invalidate(self) -> None:
        self.book = None
        self.invalidated += 1

    def snapshot_dict(self, now_ms: int | None = None, cfg: dict | None = None) -> dict:
        cfg = cfg or DEFAULT_CONFIG
        now = now_ms if now_ms is not None else _now_ms()
        book = self.book or {}
        best_bb = self.best_bid_ask or {}
        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        best = {}
        if best_bb:
            best = {
                "best_bid": best_bb.get("best_bid"),
                "best_ask": best_bb.get("best_ask"),
                "spread": best_bb.get("spread"),
                "timestamp": best_bb.get("timestamp"),
            }
        return {
            "asset_id": self.asset_id,
            "slug": self.slug,
            "window_start": self.window_start,
            "availability": self.availability(now, cfg),
            "age_ms": self.age_ms(now),
            "age_seconds": round(self.age_ms(now) / 1000.0, 3),
            "book_received_at_ms": self.book_received_at_ms or None,
            "book_age_ms": self.book_age_ms(now),
            "connected_at_ms": self.connected_at_ms,
            "last_event_at_ms": self.last_event_at_ms,
            "last_payload_ts_ms": self.last_payload_ts_ms,
            "book_source": self.book_source,
            "reconnections": self.reconnections,
            "out_of_order_drops": self.out_of_order_drops,
            "invalidated": self.invalidated,
            "events_seen": self.events_seen,
            "dropped_events": self.last_drop_count,
            "book_snapshot": {
                "bids": bids,
                "asks": asks,
                "hash": book.get("hash") if isinstance(book, dict) else None,
                "min_order_size": book.get("min_order_size") if isinstance(book, dict) else None,
                "tick_size": book.get("tick_size") if isinstance(book, dict) else None,
                "neg_risk": book.get("neg_risk") if isinstance(book, dict) else None,
                "last_trade_price": book.get("last_trade_price") if isinstance(book, dict) else None,
                "timestamp": book.get("timestamp") if isinstance(book, dict) else None,
            } if book else None,
            "best_bid_ask": best or None,
            # Docs: bids ascending (best LAST), asks descending (best LAST).
            "best_bid": (bids[-1]["price"] if bids else best.get("best_bid")),
            "best_ask": (asks[-1]["price"] if asks else best.get("best_ask")),
            "tick_size": getattr(self, "tick_size", None) or (book.get("tick_size") if isinstance(book, dict) else None),
            "min_order_size": getattr(self, "min_order_size", None) or (book.get("min_order_size") if isinstance(book, dict) else None),
            "neg_risk": getattr(self, "neg_risk", None),
            "last_trade_price": (self.last_trade or {}).get("price") if self.last_trade else (book.get("last_trade_price") if isinstance(book, dict) else None),
            "stale": self.is_stale(now, cfg),
        }


# ---- shared public client (one per process) -------------------------------
class LazyPublicClient:
    """One AsyncPublicClient shared by REST + WS reader (FK#5 safe).

    The multiplexing and reconnect ownership live inside the SDK's stream
    manager; creating a second client would create a second manager/socket.
    """

    def __init__(self) -> None:
        self._client = None
        self._lock = asyncio.Lock()

    async def get(self):
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = clientmod.build_public_client()
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None


_SHARED = LazyPublicClient()


async def get_shared_client():
    return await _SHARED.get()


# ---- manager: one unbounded reader task over one handle per asset --------
class LiveBookManager:
    def __init__(self, config: dict | None = None) -> None:
        self.cfg = dict(DEFAULT_CONFIG)
        if config:
            self.cfg.update(config)
        self._client: Any = None
        self._streams: dict[str, AssetStream] = {}
        self._handles: dict[str, Any] = {}
        self._reader_tasks: dict[str, asyncio.Task] = {}
        self._watchdog_task: asyncio.Task | None = None
        self._watchdog_on = False
        self._last_fallback_at = 0.0

    # -- subscription lifecycle -------------------------------------------
    async def subscribe(
        self,
        asset_id: str,
        *,
        slug: str = "",
        window_start: int = 0,
        fresh_game: bool = True,
    ) -> AssetStream:
        if asset_id in self._streams and asset_id in self._handles:
            return self._streams[asset_id]
        st = self._streams.get(asset_id)
        if st is None:
            st = AssetStream(asset_id=asset_id, slug=slug, window_start=window_start)
            self._streams[asset_id] = st
        if self._client is None:
            self._client = await get_shared_client()
        spec = MarketSpec(
            asset_ids=[asset_id],
            custom_feature_enabled=bool(self.cfg["custom_feature_enabled"]),
        )
        # AsyncPublicClient.subscribe is a coroutine (returns a handle).
        handle = await self._client.subscribe(spec)
        # An asyncio.wait_for around an already-awaited handle would be a no-op;
        # enforce the documented timeout at the preflight REST probe instead
        # (see RolloverWatcher) and keep this as the single async boundary.
        self._handles[asset_id] = handle
        st.connected_at_ms = _now_ms()
        if st.last_event_at_ms == 0:
            st.last_event_at_ms = _now_ms()
        self._start_reader(asset_id, handle)
        self._ensure_watchdog()
        return st

    def _start_reader(self, asset_id: str, handle: Any) -> None:
        async def _reader() -> None:
            try:
                async for event in handle:
                    self._on_event(asset_id, event, handle)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # keep generic; never leak secrets
                st = self._streams.get(asset_id)
                if st is not None:
                    st.invalidate()
                    st.reconnections += 1
                log.warning("reader for %s ended: %s", asset_id[:12], type(exc).__name__)

        task = asyncio.create_task(_reader(), name=f"live-reader-{asset_id[:12]}")
        self._reader_tasks[asset_id] = task

    def _on_event(self, asset_id: str, event: Any, handle: Any) -> None:
        st = self._streams.get(asset_id)
        if st is None:
            return
        local_now = _now_ms()
        payload = getattr(event, "payload", None)
        ev_ts = _event_ts_ms(payload) if payload is not None else None
        ev_type = getattr(event, "type", "")
        # monotonicity check (no invented seq): drop strict out-of-order
        if ev_ts is not None and st.last_applied_ts_ms and ev_ts < st.last_applied_ts_ms:
            st.out_of_order_drops += 1
            return
        if ev_ts is not None:
            st.last_applied_ts_ms = ev_ts
            st.last_payload_ts_ms = ev_ts
        st.last_event_at_ms = local_now
        st.events_seen += 1
        st.ring.append({"type": ev_type, "at_ms": local_now, "payload_ts_ms": ev_ts})
        dropped = getattr(handle, "dropped", 0)
        if dropped != st.last_drop_count:
            if dropped > st.last_drop_count:
                st.invalidate()
            st.last_drop_count = dropped
        if ev_type == "book":
            st.apply_book(payload, local_now, "clob_ws")
        elif ev_type == "price_change":
            st.apply_price_changes(payload, local_now)
        elif ev_type == "best_bid_ask":
            st.best_bid_ask = {
                "best_bid": _dec_str(getattr(payload, "best_bid", None)),
                "best_ask": _dec_str(getattr(payload, "best_ask", None)),
                "spread": _dec_str(getattr(payload, "spread", None)),
                "timestamp": getattr(payload, "timestamp", None),
            }
        elif ev_type == "last_trade_price":
            st.last_trade = {"price": _dec_str(getattr(payload, "price", None))}
        elif ev_type == "tick_size_change":
            st.tick_size = _dec_str(getattr(payload, "new_tick_size", None))

    def _ensure_watchdog(self) -> None:
        if self._watchdog_on:
            return
        self._watchdog_on = True
        self._watchdog_task = asyncio.create_task(self._watchdog(), name="live-watchdog")

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(float(self.cfg["watchdog_tick_s"]))
            now = _now_ms()
            for st in list(self._streams.values()):
                if st.is_stale(now, self.cfg):
                    st.invalidate()
                    if time.time() - self._last_fallback_at >= float(
                        self.cfg["fallback_cooldown_s"]
                    ):
                        self._last_fallback_at = time.time()
                        await self._rest_fallback(st)

    async def _rest_fallback(self, st: AssetStream) -> None:
        """Refresh one asset from REST when WS is stale. Sets availability live."""
        if self._client is None:
            return
        try:
            book = await self._client.get_order_book(asset_id=st.asset_id)
            class _B:
                pass
            b = _B()
            b.bids = book.bids
            b.asks = book.asks
            b.hash = book.hash
            b.min_order_size = book.min_order_size
            b.tick_size = book.tick_size
            b.neg_risk = book.neg_risk
            b.last_trade_price = book.last_trade_price
            b.timestamp = book.timestamp
            st.apply_book(b, _now_ms(), "clob_rest_snapshot")
            st.last_event_at_ms = _now_ms()
            st.last_payload_ts_ms = _event_ts_ms(b) or st.last_payload_ts_ms
        except Exception as exc:
            log.debug("REST fallback miss %s: %s", st.asset_id[:12], type(exc).__name__)

    async def unsubscribe(self, asset_id: str) -> bool:
        handle = self._handles.pop(asset_id, None)
        task = self._reader_tasks.pop(asset_id, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if handle is not None:
            try:
                await handle.close()
            except Exception:
                pass
        self._streams.pop(asset_id, None)
        return True

    def get_state(self, asset_id: str) -> AssetStream | None:
        return self._streams.get(asset_id)

    def status_snapshot(self) -> dict:
        now = _now_ms()
        return {
            "assets": {
                aid: st.snapshot_dict(now, self.cfg)
                for aid, st in self._streams.items()
            },
            "count": len(self._streams),
            "timestamp_ms": now,
            "config": self.cfg,
        }

    def events_snapshot(self, asset_id: str, limit: int = 50) -> list:
        st = self._streams.get(asset_id)
        if st is None:
            return []
        return list(st.ring)[-limit:]

    async def close(self) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except (asyncio.CancelledError, Exception):
                pass
            self._watchdog_task = None
            self._watchdog_on = False
        for task in list(self._reader_tasks.values()):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        for handle in list(self._handles.values()):
            try:
                await handle.close()
            except Exception:
                pass
        self._reader_tasks.clear()
        self._handles.clear()
        self._streams.clear()


# ---- rollover -------------------------------------------------------------
def slug_for_epoch(epoch_s: int) -> str:
    return f"{UPDOWN_SLUG_PREFIX}{(epoch_s // 300) * 300}"


def slug_for_window(window_start: int) -> str:
    return f"{UPDOWN_SLUG_PREFIX}{window_start}"


def boundaries(now_s: int) -> tuple[str, str, int, int]:
    """(current_slug, next_slug, current_window_start, next_window_start)."""
    cur = (now_s // 300) * 300
    nxt = cur + 300
    return slug_for_window(cur), slug_for_window(nxt), cur, nxt


class RolloverWatcher:
    """Tracks the current 5m window and prefetches the next BEFORE expiry.

    Rules that prevent stale/cross-round data:
    * Each asset's book lives in its own AssetStream; current and next use
      different keys (different condition_ids), so no snapshot can bleed
      across the boundary.
    * At most one prefetch per window (per asset_id).
    * Prefetch failures back off inside the watch loop; they never block the
      current round's live stream and never throw out of the watcher.
    """

    def __init__(self, manager: LiveBookManager) -> None:
        self.manager = manager

    async def watch_loop(
        self,
        *,
        stop: asyncio.Event | None = None,
        poll_s: float = 1.0,
        prefetched: set[str] | None = None,
    ) -> None:
        prefetched = prefetched if prefetched is not None else set()
        while stop is None or not stop.is_set():
            await self.tick_once(prefetched)
            await asyncio.sleep(poll_s)

    async def tick_once(self, prefetched: set[str] | None = None) -> dict:
        prefetched = prefetched if prefetched is not None else set()
        now = int(time.time())
        cur_slug, nxt_slug, cur_start, nxt_start = boundaries(now)
        seconds_to_boundary = (cur_start + 300) - now
        result = {
            "current_slug": cur_slug,
            "next_slug": nxt_slug,
            "current_window_start": cur_start,
            "next_window_start": nxt_start,
            "seconds_to_boundary": seconds_to_boundary,
            "prefetch_due": seconds_to_boundary <= float(
                self.manager.cfg["prefetch_lead_s"]
            ),
            "prefetched": False,
        }
        if result["prefetch_due"] and nxt_slug not in prefetched:
            client = self.manager._client or await get_shared_client()
            self.manager._client = client
            try:
                pub = client
                market = await pub.get_market(slug=nxt_slug)
                asset_ids = []
                for side in ("UP", "DOWN"):
                    asset_id, _how = _resolve_asset_id(market, side)
                    st = self.manager._streams.get(asset_id)
                    if st is None:
                        st = AssetStream(asset_id=asset_id, slug=nxt_slug, window_start=nxt_start)
                        self.manager._streams[asset_id] = st
                    await self.manager._rest_fallback(st)
                    await self.manager.subscribe(asset_id, slug=nxt_slug, window_start=nxt_start)
                    asset_ids.append(asset_id)
                prefetched.add(nxt_slug)
                result["prefetched"] = True
                result["rest_bootstrap_first"] = True
                result["next_asset_ids"] = asset_ids
            except Exception as exc:
                result["prefetch_error"] = f"{type(exc).__name__}: {exc}"
        return result


# ---- module-level singleton for tools -------------------------------------
_MANAGER: LiveBookManager | None = None


def get_manager() -> LiveBookManager:
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = LiveBookManager()
    return _MANAGER
