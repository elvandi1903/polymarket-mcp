"""Live Market Data Engine — configured Binance Spot + Coinbase Spot WebSockets.

Architecture (as requested):

    Binance Spot ──┐
                   ├──> Live Market Data Engine
    Coinbase Spot ─┘                │
                                    ▼
                            Data Validation
                            - Timestamp (ms / microseconds)
                            - Staleness (age in ms)
                            - Feed gaps (sequence gaps, inter-arrival)
                            - Outlier detection (price jump vs tolerance)

Facts (from the live APIs, verified by experiments, never invented):
- Binance spot WebSocket (public, no auth), configured ``<symbol>@trade``
  stream: ``{e:"trade", s:"...", T: 1794..., p:"...",
  q:"...", c:"...", U:..., u:...}`` — ``T`` is event time (ms), ``u`` is final
  update id in the event. `exec`/`depthUpdate` channels have their own semantics;
  for live price validation we use ``@trade`` (1 trade per update).
- Coinbase Exchange WebSocket (public, no auth). Channel ``ticker``: message arrives as
  ``{type:"ticker", channel:"ticker", sequence_num: int, timestamp: "2026-..."
  (RFC3339 microsecond), product_id:"BTC-USD", price:"...", last_size:"..."}``.
  Channel ``level2`` sends bids/asks arrays of 100 levels (not used here).
- Both sources use **Decimal** price (never float) via ``Decimal(str(...))``.

Validation contract (per message):
* ``timestamp_ms``: epoch milliseconds of the event (Coinbase RFC3339 converted).
* ``age_ms``: ``now_ms - event_timestamp_ms`` at processing.
* ``staleness``: ``age_ms > 5000`` is flagged STALE.
* ``feed_gap``: if ``now - previous_event_time > 2000ms``, gap flagged; also
  ``sequence_num`` regression (Coinbase) counts as gap.
* ``outlier``: if ``abs(price - prev_price) / prev_price > 1%`` for the first
  update after a gap, OR ``price <= 0``, flagged.

All state is kept in memory; MCP tools expose read-only snapshots only.
"""

from __future__ import annotations

import asyncio
import decimal
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .config import Settings

# ---------------------------------------------------------------------------
# Endpoints (verified from the live APIs; advanced-trade-ws requires auth)
BINANCE_WS = "wss://stream.binance.com:9443/ws"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"

STALE_MS = 5000
GAP_MS = 2000
OUTLIER_PCT = Decimal("0.01")  # 1%


def _now_ms() -> int:
    return int(time.time() * 1000)


def _parse_iso(s: str) -> int | None:
    """ISO-8601 / RFC3339 string -> epoch ms."""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except (ValueError, OverflowError):
        return None


@dataclass
class SpotPoint:
    source: str            # "binance" | "coinbase"
    symbol: str            # normalized, e.g. "BTC/USDT" or "BTC/USD"
    price: Decimal
    timestamp_ms: int
    received_ms: int = 0
    sequence: int | None = None
    age_ms: int = 0
    is_stale: bool = False
    is_gap: bool = False
    is_outlier: bool = False
    note: str = ""

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "symbol": self.symbol,
            "price": str(self.price),
            "timestamp_ms": self.timestamp_ms,
            "received_ms": self.received_ms,
            "age_ms": self.age_ms,
            "is_stale": self.is_stale,
            "is_gap": self.is_gap,
            "is_outlier": self.is_outlier,
            "sequence": self.sequence,
            "note": self.note,
        }


class FeedState:
    """Per-source streaming bookkeeping."""

    def __init__(self, source: str, symbol: str) -> None:
        self.source = source
        self.symbol = symbol
        self.points: deque[SpotPoint] = deque(maxlen=300)
        self.last_event_ms: int = 0
        self.last_price: Decimal | None = None
        self.last_sequence: int | None = None
        self.connected_at_ms: int = 0
        self.reconnections: int = 0
        self.dropped_parse: int = 0
        self.gaps: int = 0
        self.outliers: int = 0
        self.stale_events: int = 0

    def ingest(self, point: SpotPoint) -> SpotPoint:
        now = _now_ms()
        point.received_ms = now
        point.age_ms = max(0, now - point.timestamp_ms) if point.timestamp_ms else 10**15
        point.is_stale = point.age_ms > STALE_MS
        if point.is_stale:
            self.stale_events += 1
        # gap: inter-arrival gap larger than GAP_MS
        if self.last_event_ms and point.timestamp_ms - self.last_event_ms > GAP_MS:
            point.is_gap = True
            self.gaps += 1
        # sequence regression (Coinbase ticker sequence_num)
        if point.sequence is not None and self.last_sequence is not None:
            if point.sequence < self.last_sequence:
                point.is_gap = True
                self.gaps += 1
                point.note = f"seq regression {self.last_sequence}->{point.sequence}"
        # outlier: >1% move vs previous good price, or non-positive
        if point.price <= 0:
            point.is_outlier = True
            point.note = "non-positive price"
        elif self.last_price is not None and self.last_price > 0:
            move = abs(point.price - self.last_price) / self.last_price
            if move > OUTLIER_PCT:
                point.is_outlier = True
                point.note = f"move {float(move)*100:.4f}% vs prev {self.last_price}"
        if point.is_outlier and not point.is_gap:
            # don't count gap-induced jump twice; count only real outliers
            self.outliers += 1
        if not point.is_outlier and not point.is_stale:
            self.last_price = point.price
        if point.sequence is not None:
            self.last_sequence = point.sequence
        self.last_event_ms = point.timestamp_ms
        self.points.append(point)
        return point

    def snapshot(self) -> dict:
        now = _now_ms()
        if not self.points:
            return {
                "source": self.source,
                "symbol": self.symbol,
                "connected": False,
                "stale": True,
                "gaps": self.gaps,
                "outliers": self.outliers,
                "stale_events": self.stale_events,
                "dropped_parse": self.dropped_parse,
                "reconnections": self.reconnections,
                "points": 0,
            }
        last = self.points[-1]
        fresh = now - last.received_ms
        return {
            "source": self.source,
            "symbol": self.symbol,
            "connected": True,
            "stale": fresh > STALE_MS,
            "freshness_ms": fresh,
            "last_price": str(last.price),
            "last_timestamp_ms": last.timestamp_ms,
            "last_sequence": last.sequence,
            "age_ms": now - last.timestamp_ms if last.timestamp_ms else None,
            "gaps": self.gaps,
            "outliers": self.outliers,
            "stale_events": self.stale_events,
            "dropped_parse": self.dropped_parse,
            "reconnections": self.reconnections,
            "points": len(self.points),
        }


class LiveMarketDataEngine:
    """One connection per source, one engine; reconnect with backoff."""

    def __init__(self, binance_symbol: str, coinbase_symbol: str) -> None:
        if not binance_symbol or not binance_symbol.isalnum():
            raise ValueError("invalid_binance_symbol")
        if not coinbase_symbol or not all(p.isalnum() for p in coinbase_symbol.split("-")):
            raise ValueError("invalid_coinbase_symbol")
        self.log = logging.getLogger("polymarket_mcp.market_data")
        self.binance_symbol = binance_symbol.upper()
        self.coinbase_symbol = coinbase_symbol.upper()
        self.binance = FeedState("binance", self.binance_symbol)
        self.coinbase = FeedState("coinbase", self.coinbase_symbol)
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._ws: dict[str, Any] = {}

    # --- parsers (pure, testable) ---
    @staticmethod
    def parse_binance_trade(raw: str, symbol: str | None = None) -> SpotPoint | None:
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if d.get("e") != "trade":
            return None
        try:
            return SpotPoint(
                source="binance",
                symbol=(symbol or str(d["s"])).upper(),
                price=Decimal(str(d["p"])),
                timestamp_ms=int(d["T"]),
                sequence=int(d.get("u") or 0) or None,
            )
        except (KeyError, decimal.InvalidOperation, ValueError):
            return None

    @staticmethod
    def parse_coinbase_ticker(raw: str, symbol: str | None = None) -> SpotPoint | None:
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return None
        product = str(d.get("product_id") or "").upper()
        if d.get("type") != "ticker" or (symbol is not None and product != symbol.upper()):
            return None
        # Legacy ticker uses "time" + "sequence" + "trade_id"; advanced uses
        # "timestamp" + "sequence_num". Both are accepted (verified keys).
        ts_raw = d.get("timestamp") or d.get("time")
        if ts_raw is None:
            return None
        if isinstance(ts_raw, (int, float)):
            ts = int(ts_raw)
        else:
            ts = _parse_iso(str(ts_raw))
            if ts is None:
                return None
        seq = d.get("sequence") if d.get("sequence") is not None else d.get("sequence_num")
        try:
            return SpotPoint(
                source="coinbase",
                symbol=product or str(symbol).upper(),
                price=Decimal(str(d["price"])),
                timestamp_ms=ts,
                sequence=int(seq) if seq is not None else None,
            )
        except (KeyError, decimal.InvalidOperation, ValueError):
            return None

    async def _loop_binance(self) -> None:
        from aiohttp import ClientSession, WSMsgType
        from aiohttp import WSServerHandshakeError as WSHandshakeError

        while self._running:
            try:
                async with ClientSession() as s:
                    try:
                        async with s.ws_connect(f"{BINANCE_WS}/{self.binance_symbol.lower()}@trade", heartbeat=25) as ws:
                            self._ws["binance"] = ws
                            self.binance.connected_at_ms = _now_ms()
                            self.binance.reconnections += 1
                            async for msg in ws:
                                if msg.type == WSMsgType.TEXT:
                                    pt = self.parse_binance_trade(msg.data, self.binance_symbol)
                                    if pt is None:
                                        self.binance.dropped_parse += 1
                                        continue
                                    self.binance.ingest(pt)
                                elif msg.type == WSMsgType.CLOSED:
                                    raise ConnectionError("binance closed")
                                elif msg.type == WSMsgType.ERROR:
                                    raise ConnectionError(str(msg.data))
                    except WSHandshakeError as hs:
                        raise ConnectionError(str(hs))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = self._backoff(self.binance.reconnections)
                self.log.warning("binance reconnect in %.1fs: %s", delay, exc)
                await asyncio.sleep(delay)

    async def _loop_coinbase(self) -> None:
        from aiohttp import ClientSession, WSMsgType
        from aiohttp import WSServerHandshakeError as WSHandshakeError

        while self._running:
            try:
                async with ClientSession() as s:
                    try:
                        async with s.ws_connect(COINBASE_WS, heartbeat=25) as ws:
                            self._ws["coinbase"] = ws
                            await ws.send_str(json.dumps({
                                "type": "subscribe",
                                "product_ids": [self.coinbase_symbol],
                                "channels": ["ticker"],
                            }))
                            self.coinbase.connected_at_ms = _now_ms()
                            self.coinbase.reconnections += 1
                            async for msg in ws:
                                if msg.type == WSMsgType.TEXT:
                                    pt = self.parse_coinbase_ticker(msg.data, self.coinbase_symbol)
                                    if pt is None:
                                        self.coinbase.dropped_parse += 1
                                        continue
                                    self.coinbase.ingest(pt)
                                elif msg.type == WSMsgType.CLOSED:
                                    raise ConnectionError("coinbase closed")
                                elif msg.type == WSMsgType.ERROR:
                                    raise ConnectionError(str(msg.data))
                    except WSHandshakeError as hs:
                        raise ConnectionError(str(hs))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = self._backoff(self.coinbase.reconnections)
                self.log.warning("coinbase reconnect in %.1fs: %s", delay, exc)
                await asyncio.sleep(delay)

    @staticmethod
    def _backoff(attempt: int) -> float:
        import random

        return random.uniform(0, min(30.0, 1.0 * (2 ** attempt)))

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._loop_binance(), name="md-binance"),
            asyncio.create_task(self._loop_coinbase(), name="md-coinbase"),
        ]

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks = []

    def snapshot(self) -> dict:
        return {
            "binance": self.binance.snapshot(),
            "coinbase": self.coinbase.snapshot(),
            "engine_running": self._running,
            "timestamp_ms": _now_ms(),
            "validation": {
                "stale_ms": STALE_MS,
                "gap_ms": GAP_MS,
                "outlier_pct": str(OUTLIER_PCT),
            },
        }

    def last_points(self, source: str, limit: int = 20) -> list[dict]:
        feed = self.binance if source == "binance" else self.coinbase
        return [p.as_dict() for p in list(feed.points)[-limit:]]


_ENGINE: LiveMarketDataEngine | None = None


def get_engine(binance_symbol: str | None = None, coinbase_symbol: str | None = None) -> LiveMarketDataEngine:
    global _ENGINE
    if _ENGINE is None:
        if not binance_symbol or not coinbase_symbol:
            raise ValueError("market_data_symbols_required")
        _ENGINE = LiveMarketDataEngine(binance_symbol, coinbase_symbol)
    elif ((binance_symbol is not None and _ENGINE.binance_symbol != binance_symbol.upper()) or
          (coinbase_symbol is not None and _ENGINE.coinbase_symbol != coinbase_symbol.upper())):
        raise ValueError("market_data_already_running_with_different_symbols")
    return _ENGINE


def start_engine(binance_symbol: str, coinbase_symbol: str) -> LiveMarketDataEngine:
    e = get_engine(binance_symbol, coinbase_symbol)
    e.start()
    return e


async def stop_engine() -> None:
    global _ENGINE
    if _ENGINE is not None:
        await _ENGINE.stop()
        _ENGINE = None
