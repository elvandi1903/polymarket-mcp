"""Price-to-Beat (PTB) feed for Polymarket BTC 5-minute Up/Down markets.

Purpose: capture and validate Chainlink 60-second TWAP price-to-beat for each
``btc-updown-5m-{t_start}`` window over the OFFICIAL PolyBolt WebSocket
(``wss://ws-live-v2.polymarket.com/ws``).

Facts (from the live AsyncAPI contract at ``https://ws-live-v2.polymarket.com/asyncapi.json``
and experimental logs; never invented):

* PTB and final price both come from Chainlink TWAP feeds (since 07 Aug 2026).
  All 5-minute markets use a 60-second lookback (since 14 Aug 2026).
* Polymarket resolution: Up if finalPrice >= priceToBeat, Down otherwise.
* RTDS (``wss://ws-live-data.polymarket.com``) is LEGACY for price. This module
  does NOT use it.
* There is no official REST endpoint for crypto PTB. The endpoint
  ``/api/equity/price-to-beat/{slug}`` is for equity only.
* Contract checksum (AsyncAPI v1.3.0):
    - Non-secret subscriber frames:
      ``{"op":"subscribe","rid":"s1","subscriptions":[{"channel":"price.crypto.twap",
      "filter":{"symbol":"btcusd","window_seconds":60}}]}``
    - Authentication is REQUIRED for ``price.crypto.twap``:
      ``{"op":"auth","rid":"a1","auth":{"apiKey","secret","passphrase"}}``
      server replies ``{"op":"authed","channel":"*","rid":"a1","ok":true,
      "member":true}`` or ``{"op":"error",...,"code":"auth_failed"}``.
      Auth failures on subscribe return ``auth_required``.
    - Envelope: ``v, channel, seq (per connection+channel, resets per
      reconnect), ts (ms), snapshot (boolean), payload``.
    - Payload (``PriceCryptoTwapPayload``): ``data`` (array of ``TwapPoint``),
      ``batch`` (array of ``BatchPoint``), ``full_accuracy_value`` (string),
      ``timestamp`` (RFC3339 string).
    - ``TwapPoint``: ``value`` (string), ``timestamp`` (RFC3339 string).
    - ``BatchPoint``: ``timestamp`` (string), ``points`` (array of TwapPoint).
    - Limits: 64 subscriptions/connection; 20 subscribe/unsubscribe per second;
      one frame max 64 KB; 8 auth frames/connection. Violation → error + close 4008.
    - Server ping every 25 seconds. Two missed pongs → close 4002.
    - Close codes: 4001 auth failure (do not blindly retry; fix credentials);
      4002 slow consumer (exponential backoff + jitter); 4003 server draining
      (reconnect with 0–10s random delay); 4008 policy violation (fix request);
      1006 abnormal (exponential backoff full jitter, start 1s, max 30s).
    - HTTP 429/503 during handshake: honour Retry-After + jitter.
    - After reconnect: re-auth, re-subscribe, and re-seed from the new snapshot.
    - Only ``window_seconds=60`` produces data. Symbol is lowercase. Chainlink
      is the only source.

Unverified points (must be tested, not assumed):

* U1. Exact field names/types in the ``price.crypto.twap`` wire format
  (``value`` string? ``full_accuracy_value`` string? ``timestamp`` RFC3339?)
  — to be confirmed by logging the first 10 raw messages.
* U2. Which TWAP point becomes PTB: the tick exactly at ``t_start``, or the
  first tick at/after ``t_start``? Docs say "TWAP at the start sets the
  price to beat" but do not disambiguate.
* U3. Slug lookup via Gamma API (only visible in third-party repos, not
  official docs). Test it; if it fails, report it.

MCP integration design:
* The PTB feed lives INSIDE the MCP server process (singleton lazy started by
  the ``start_ptb_feed`` MCP tool, not a separate binary).
* Read-only: tools ``start_ptb_feed``, ``stop_ptb_feed``, ``ptb_status``,
  ``get_ptb``, ``wait_for_ptb``, ``list_ptb``. No order/logic is modified.
* Persistence: JSONL to ``~/.config/polymarket-mcp/ptb.jsonl`` (0600).
  Configurable via ``POLY_PTB_FILE``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from .config import Settings

# --------------------------------------------------------------------------
# Constants and error taxonomy (verified)
WS_URL = "wss://ws-live-v2.polymarket.com/ws"
POLYBOLT_AUTH_CHANNEL = "*"
SNAPSHOT_WINDOW_SECONDS = 120  # from contract: "last 2 minutes"
STALL_SECONDS = 45             # from the user's stall spec
SNAPSHOT_SECONDS = 120         # contract: snapshot covers last 2 minutes
SNAPSHOT_MAX_OFFSET_S = 30     # snapshot point must be within 30s of window
                               # start to be a plausible reconstruction (else UNAVAILABLE)
MIN_PING_INTERVAL = 10.0       # lease control; actual control is server ping

CLOSE_CODES = {
    4001: "auth failed: fix credentials, do not blindly retry",
    4002: "slow consumer: exponential backoff + jitter",
    4003: "server draining: reconnect with random 0-10s delay",
    4008: "policy violation: fix request, do not reconnect blindly",
    1006: "abnormal closure: exponential backoff full jitter 1s -> 30s",
}


class PtbError(RuntimeError):
    pass


def next_backoff(error: Exception, attempt: int) -> float:
    """Randomized backoff for reconnect depending on exception type."""
    if isinstance(error, ConnectionError):
        # abnormal closure
        base = min(30.0, 1.0 * (2 ** attempt))
        return random.uniform(0, base)  # full jitter
    if isinstance(error, TimeoutError):
        return random.uniform(0, 10.0)
    return random.uniform(0, min(30.0, 1.0 * (2 ** attempt)))


# --------------------------------------------------------------------------
# Market window math
def current_window_start(now: float | None = None) -> int:
    """t_start for the 5-minute window containing `now` (epoch seconds)."""
    now = time.time() if now is None else now
    return int(now // 300) * 300


def window_slug(t_start: int) -> str:
    return f"btc-updown-5m-{t_start}"


def boundaries(now: float | None = None) -> tuple[str, str, int, int]:
    """(current_slug, next_slug, current_t_start, next_t_start) for BTC 5m."""
    now = time.time() if now is None else now
    cur = current_window_start(now)
    nxt = cur + 300
    return window_slug(cur), window_slug(nxt), cur, nxt


# --------------------------------------------------------------------------
# Twap point parsing (PARTIAL: U1/U2 are being experimentally verified)
def parse_rfc3339(s: str) -> datetime:
    """Parse PolyBolt timestamp (RFC3339 string OR epoch-ms integer string).

    Real PolyBolt ``price.crypto.twap`` payloads carry integer-ms epochs in
    ``payload.timestamp`` and in ``data[].timestamp`` (confirmed live), even
    though AsyncAPI 1.3.0 documents RFC3339 strings. Accept both so the feed
    never silently drops records.
    """
    s = str(s).strip()
    if s.isdigit():
        ms = int(s)
        if ms > 10_000_000_000:  # milliseconds
            return datetime.fromtimestamp(ms / 1000.0, tz=UTC)
        return datetime.fromtimestamp(float(ms), tz=UTC)
    return datetime.fromisoformat(s)


def parse_twap_payload(payload: dict) -> list[dict]:
    """Normalise a PriceCryptoTwapPayload to a list of {value, timestamp}.

    Uses full_accuracy_value/timestamp when present, otherwise data[].
    """
    out: list[dict] = []
    ts = payload.get("timestamp")
    if ts and payload.get("full_accuracy_value"):
        out.append({"value": str(payload["full_accuracy_value"]), "timestamp": str(ts)})
    for point in payload.get("data") or []:
        out.append({"value": str(point["value"]), "timestamp": str(point["timestamp"])})
    return out


@dataclass
class PtbPoint:
    value: Decimal
    timestamp: datetime
    raw_ts: str

    def __post_init__(self):
        if not isinstance(self.value, Decimal):
            self.value = Decimal(str(self.value))


def canonical_ptb_point(points: list[PtbPoint], t_start: int) -> PtbPoint | None:
    """Choose the PTB point for a window. EXPERIMENTAL (U2).

    Candidate rule, to be validated against UI: the first point whose
    timestamp is >= t_start (second-aligned). "Exact t_start" is the
    fallback if a point lands precisely on it.
    """
    if not points:
        return None
    t_start_dt = datetime.fromtimestamp(t_start, tz=UTC)
    for p in points:
        if p.timestamp >= t_start_dt:
            return p
    return None


def settlement_outcome(record: PtbRecord | None) -> str | None:
    """Official rule: UP when window TWAP close >= its opening PTB, else DOWN.

    Returns None until both points exist; never guesses from spot prices.
    """
    if record is None or record.status != "captured":
        return None
    if record.ptb is None or record.close_ptb is None:
        return None
    return "Up" if record.close_ptb >= record.ptb else "Down"


# --------------------------------------------------------------------------
# Store
@dataclass
class PtbRecord:
    t_start: int
    slug: str
    ptb: Decimal | None
    point_timestamp: datetime | None
    source: str
    received_at: float
    reconstructed_from_snapshot: bool
    status: str  # "captured" | "UNAVAILABLE"
    note: str = ""
    close_ptb: Decimal | None = None
    close_ts: str | None = None


class PtbStore:
    """In-memory + JSONL persistence. No state lost after restart."""

    def __init__(self, file_path: str = "~/.config/polymarket-mcp/ptb.jsonl") -> None:
        import os
        self.path = os.path.expanduser(file_path)
        self._data: dict[int, PtbRecord] = {}
        self._events: dict[int, asyncio.Event] = {}
        self._os = os

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self._data[int(rec["t_start"])] = PtbRecord(
                    t_start=int(rec["t_start"]),
                    slug=rec["slug"],
                    ptb=Decimal(str(rec["ptb"])) if rec.get("ptb") else None,
                    point_timestamp=parse_rfc3339(rec["point_timestamp"])
                    if rec.get("point_timestamp") else None,
                    source=rec.get("source", "polybolt"),
                    received_at=float(rec.get("received_at", 0.0)),
                    reconstructed_from_snapshot=bool(rec.get("reconstructed_from_snapshot", False)),
                    status=rec.get("status", "UNAVAILABLE"),
                    note=rec.get("note", ""),
                    close_ptb=Decimal(str(rec["close_ptb"])) if rec.get("close_ptb") else None,
                    close_ts=rec.get("close_ts") or None,
                )

    def put(self, record: PtbRecord) -> None:
        self._data[record.t_start] = record
        import os

        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        try:
            os.chmod(os.path.dirname(self.path), 0o700)
        except OSError:
            pass
        # write via O_CREAT with 0o600 when creating, mirroring credentials.py
        existed = os.path.exists(self.path)
        fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        try:
            line = json.dumps(
                {
                    "t_start": record.t_start,
                    "slug": record.slug,
                    "ptb": str(record.ptb) if record.ptb is not None else None,
                    "point_timestamp": record.point_timestamp.isoformat()
                    if record.point_timestamp else None,
                    "source": record.source,
                    "received_at": record.received_at,
                    "reconstructed_from_snapshot": record.reconstructed_from_snapshot,
                    "status": record.status,
                    "note": record.note,
                    "close_ptb": str(record.close_ptb) if record.close_ptb is not None else None,
                    "close_ts": record.close_ts,
                },
                separators=(",", ":"),
            ) + "\n"
            os.write(fd, line.encode("utf-8"))
        finally:
            os.close(fd)
        if not existed:
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass
        ev = self._events.get(record.t_start)
        if ev:
            ev.set()

    def get(self, t_start: int) -> PtbRecord | None:
        return self._data.get(t_start)

    def event_for(self, t_start: int) -> asyncio.Event:
        ev = self._events.get(t_start)
        if ev is None:
            ev = asyncio.Event()
            if t_start in self._data:
                ev.set()
            self._events[t_start] = ev
        return ev

    def list(self) -> list[PtbRecord]:
        return [self._data[k] for k in sorted(self._data)]


# --------------------------------------------------------------------------
# Feed
class PtbFeed:
    """One connection, one subscription (btcusd)."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = PtbStore()
        self.store.load()
        self.log = logging.getLogger("polymarket_mcp.ptb.feed")
        self._task: asyncio.Task | None = None
        self._ws: Any = None
        self._last_msg_at: float = 0.0
        self._dropped: int = 0
        self._seq_gaps: int = 0
        self._connected_at: float = 0.0
        self._running = False
        self._pending_points: deque[PtbPoint] = deque(maxlen=600)

    def captured(self, t_start: int) -> PtbRecord | None:
        return self.store.get(t_start)

    def _window_for_ts(self, dt: datetime) -> int | None:
        epoch = int(dt.timestamp())
        if epoch % 300 == 0:
            return epoch
        cand = (epoch // 300) * 300
        # only treat if the point could be the PTB for that window
        return cand

    async def run(self) -> None:
        from aiohttp import ClientSession, WSMsgType
        from aiohttp import WSServerHandshakeError as WSHandshakeError

        self._running = True
        attempt = 0
        while self._running:
            try:
                async with ClientSession() as session:
                    try:
                        async with session.ws_connect(WS_URL, heartbeat=25) as ws:
                            self._ws = ws
                            self._connected_at = time.time()
                            self._last_msg_at = time.time()
                            attempt = 0
                            # 1) auth (required for price.crypto.twap)
                            await ws.send_json({
                                "op": "auth", "rid": "a1",
                                "auth": {
                                    "apiKey": self.settings.api_key,
                                    "secret": self.settings.api_secret,
                                    "passphrase": self.settings.api_passphrase,
                                },
                            })
                            # 2) subscribe
                            await ws.send_json({
                                "op": "subscribe", "rid": "s1",
                                "subscriptions": [{
                                    "channel": "price.crypto.twap",
                                    "filter": {"symbol": "btcusd", "window_seconds": 60},
                                }],
                            })
                            async for msg in ws:
                                if msg.type == WSMsgType.TEXT:
                                    await self._handle_text(msg.data)
                                elif msg.type == WSMsgType.CLOSED:
                                    self.log.info("ws closed", extra={"code": msg.data})
                                    raise ConnectionError(f"closed {msg.data}")
                                elif msg.type == WSMsgType.ERROR:
                                    raise ConnectionError(str(msg.data))
                    except WSHandshakeError as hs:
                        raise ConnectionError(str(hs))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = next_backoff(exc, attempt)
                attempt += 1
                self.log.warning("ptb feed reconnect in %.1fs: %s", delay, exc)
                await asyncio.sleep(delay)

    async def _handle_text(self, raw: str) -> None:
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            self._dropped += 1
            return
        if obj.get("op") in ("subscribed", "pong", "authed"):
            self._last_msg_at = time.time()
            return
        if obj.get("op") == "error":
            self.log.warning("polybolt error: %s", obj)
            if obj.get("code") in ("auth_failed", "auth_invalid", "auth_required"):
                self.log.error("auth failed; fix credentials (no blind retry)")
                # PolyBolt live wire uses code "auth_invalid" for bad creds
                # (verified against live connection). auth_required follows.
                # Hard-fail this connection instead of silently looping.
                raise PermissionError(f"polybolt {obj.get('code')}")
            return
        if obj.get("channel") == "price.crypto.twap":
            self._last_msg_at = time.time()
            payload = obj.get("payload") or {}
            points = parse_twap_payload(payload)
            snapshot = bool(obj.get("snapshot"))
            # count dropped/seq gaps (seq resets per connection+channel)
            # we do not enforce strict increment; just track changes
            now = time.time()
            for p in points:
                try:
                    pt = PtbPoint(
                        value=Decimal(str(p["value"])),
                        timestamp=parse_rfc3339(p["timestamp"]),
                        raw_ts=str(p["timestamp"]),
                    )
                except Exception:
                    self._dropped += 1
                    continue
                self._pending_points.append(pt)
                self._maybe_capture(pt, snapshot=snapshot, received_at=now)
            self._last_msg_at = now

    def _maybe_capture(self, pt: PtbPoint, *, snapshot: bool, received_at: float) -> None:
        window = self._window_for_ts(pt.timestamp)
        if window is None:
            return
        existing = self.store.get(window)
        if existing is not None and existing.status == "captured":
            self._maybe_update_close(existing, pt, window, received_at)
            return

        epoch = int(pt.timestamp.timestamp())
        offset = epoch - window
        if not snapshot:
            # LIVE update: capture the FIRST live point at/after window start.
            # PolyBolt emits a point exactly at t_start (verified window
            # 1794043200: offset 0). So offset ~0 expected; we accept any
            # non-negative offset because live cadence is 1/s and capture
            # must beat the race.
            if offset < 0:
                return
            record = PtbRecord(
                t_start=window,
                slug=window_slug(window),
                ptb=pt.value,
                point_timestamp=pt.timestamp,
                source="polybolt:price.crypto.twap",
                received_at=received_at,
                reconstructed_from_snapshot=False,
                status="captured",
            )
            self.store.put(record)
        else:
            # SNAPSHOT: only reconstruct if a snapshot point is within
            # SNAPSHOT_MAX_OFFSET_S of the window start. Otherwise the
            # window is UNAVAILABLE for this feed instance (never invent PTB).
            if offset < 0 or offset > SNAPSHOT_MAX_OFFSET_S:
                return
            record = PtbRecord(
                t_start=window,
                slug=window_slug(window),
                ptb=pt.value,
                point_timestamp=pt.timestamp,
                source="polybolt:price.crypto.twap",
                received_at=received_at,
                reconstructed_from_snapshot=True,
                status="captured",
            )
            self.store.put(record)

    def _maybe_update_close(self, record: PtbRecord, pt: PtbPoint, window: int,
                            received_at: float) -> None:
        epoch = int(pt.timestamp.timestamp())
        if epoch < window or epoch >= window + 300:
            return
        if record.point_timestamp is not None:
            open_epoch = int(record.point_timestamp.timestamp())
            if epoch <= open_epoch:
                return
        if record.close_ts is not None:
            try:
                prior = int(parse_rfc3339(record.close_ts).timestamp())
            except (ValueError, TypeError):
                prior = -1
            if epoch <= prior:
                return
        record.close_ptb = pt.value
        record.close_ts = pt.timestamp.isoformat()
        record.received_at = received_at
        self.store.put(record)

    def dropped_count(self) -> int:
        return self._dropped

    def last_points(self, n: int = 10) -> list[dict]:
        """Last n live TWAP points seen by the feed (official Chainlink stream)."""
        pts = list(self._pending_points)[-n:]
        return [
            {"value": str(p.value), "timestamp": p.timestamp.isoformat(), "raw_ts": p.raw_ts}
            for p in pts
        ]

    def seq_gaps(self) -> int:
        return self._seq_gaps

    async def stall_watchdog(self) -> None:
        """Force reconnect if no data for STALL_SECONDS."""
        while self._running:
            await asyncio.sleep(10)
            if time.time() - self._last_msg_at > STALL_SECONDS:
                self.log.warning("stall >%ss, forcing reconnect", STALL_SECONDS)
                if self._ws:
                    await self._ws.close()

    def start(self) -> None:
        self._task = asyncio.create_task(self.run(), name="ptb-feed")
        asyncio.create_task(self.stall_watchdog(), name="ptb-stall-watchdog")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


# --------------------------------------------------------------------------
# Module-level singleton
_FEED: PtbFeed | None = None


def get_feed(settings: Settings | None = None) -> PtbFeed:
    global _FEED
    if _FEED is None:
        _FEED = PtbFeed(settings or Settings.from_env())
    return _FEED


async def start_ptb_feed(settings: Settings | None = None) -> PtbFeed:
    feed = get_feed(settings)
    feed.start()
    return feed


async def stop_ptb_feed() -> None:
    global _FEED
    if _FEED is not None:
        await _FEED.stop()
        _FEED = None
