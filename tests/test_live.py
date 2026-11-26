"""Live-book durability (offline, no network/keys).

Covers: book snapshot authority, out-of-order drops, dropped-counter
invalidation, availability thresholds, stale detection, ring buffer bounds,
and per-asset isolation (the rollover invariant).
"""
import asyncio
from types import SimpleNamespace as NS

from polymarket_mcp.live import AssetStream, LiveBookManager


def _book(price, ts_ms):
    return NS(
        bids=[NS(price=price, size=100)],
        asks=[NS(price=0.99, size=100)],
        hash="h",
        min_order_size=5,
        tick_size=0.01,
        neg_risk=False,
        last_trade_price=None,
        timestamp=ts_ms,
    )


def _evt(ts, price=0.61):
    return NS(type="book", payload=_book(price, ts))


class _Handle:
    dropped = 0


async def _run(coro_fn):
    return await coro_fn()


def _manager_with(handle_dropped: int = 0):
    m = LiveBookManager()
    h = _Handle()
    h.dropped = handle_dropped
    st = AssetStream(asset_id="A1")
    m._streams["A1"] = st
    return m, h, st


def test_book_applies():
    m, h, st = _manager_with()
    st.apply_book(_book(0.60, 1000), local_now_ms=1000)
    assert st.book and st.book["bids"][0]["price"] == "0.6"


def test_out_of_order_drop():
    m, h, st = _manager_with()
    st.apply_book(_book(0.60, 1000), 1000)
    m._on_event("A1", _evt(900), h)
    n1 = st.out_of_order_drops
    m._on_event("A1", _evt(800), h)  # older than last applied (900)
    assert st.out_of_order_drops == n1 + 1


def test_dropped_counter_invalidates():
    m, h, st = _manager_with()
    st.apply_book(_book(0.60, 1000), 1000)
    m._on_event("A1", _evt(1001), h)
    h.dropped = 7
    m._on_event("A1", _evt(1002), h)
    assert st.invalidated >= 1


def test_availability_thresholds():
    m = LiveBookManager()
    for age_ms, expected in [(1500, "live"), (4000, "aging"), (16000, "stale")]:
        x = AssetStream(asset_id=f"A{age_ms}")
        x.last_event_at_ms = 1000
        assert x.availability(1000 + age_ms, m.cfg) == expected


def test_is_stale_via_no_event():
    m = LiveBookManager()
    x = AssetStream(asset_id="D")
    x.last_event_at_ms = 1000
    assert x.is_stale(10000, m.cfg) is True   # age 9000 > no_event 8000
    assert x.is_stale(3000, m.cfg) is False   # age 2000


def test_ring_buffer_bounded():
    fake = AssetStream(asset_id="RING")
    for i in range(250):
        fake.ring.append(i)
    assert len(fake.ring) == 200 and fake.ring[0] == 50


def test_per_asset_isolation_rollover():
    cur = AssetStream(asset_id="CUR")
    nxt = AssetStream(asset_id="NEXT")
    cur.apply_book(_book(0.70, 2000), 2000)
    nxt.apply_book(_book(0.30, 2000), 2000)
    assert cur.book["bids"][0]["price"] == "0.7"
    assert nxt.book["bids"][0]["price"] == "0.3"


def test_boundaries_slug_math():
    from polymarket_mcp.live import boundaries, slug_for_window

    cur_slug, nxt_slug, cur_start, nxt_start = boundaries(1794021600 + 100)
    assert cur_slug == "btc-updown-5m-1794021600"
    assert nxt_slug == "btc-updown-5m-1794021900"
    assert nxt_start - cur_start == 300
    assert slug_for_window(1794021600) == "btc-updown-5m-1794021600"


def _levels(prices, size=100):
    return [NS(price=p, size=size) for p in prices]


def test_best_bid_ask_are_last_entries():
    """Docs: bids ascending (best LAST), asks descending (best LAST)."""
    from polymarket_mcp.live import AssetStream

    st = AssetStream(asset_id="B")
    book = NS(bids=_levels([0.01, 0.20, 0.33]), asks=_levels([0.99, 0.50, 0.35]),
              hash="h", min_order_size=5, tick_size="0.01", neg_risk=False,
              last_trade_price=None, timestamp=2000)
    st.apply_book(book, local_now_ms=2000)
    snap = st.snapshot_dict(now_ms=2000)
    assert snap["best_bid"] == "0.33", "best bid must be last (highest) bid"
    assert snap["best_ask"] == "0.35", "best ask must be last (lowest) ask"


def _change(price, size, side, asset_id="A1", h="h"):
    return NS(asset_id=asset_id, price=price, size=size, side=side, hash=h,
              best_bid=None, best_ask=None)


def test_price_change_deltas_update_live_book():
    """price_change is the real-time update; book alone is only a snapshot."""
    m, h, st = _manager_with()
    st.apply_book(_book(0.60, 1000), local_now_ms=1000)
    payload = NS(timestamp=1100, price_changes=(
        _change(0.99, 0, "SELL", h="h1"),
        _change(0.98, 50, "SELL", h="h2"),
        _change(0.62, 30, "BUY", h="h3")))
    m._on_event("A1", NS(type="price_change", payload=payload), h)
    asks = {row["price"]: row["size"] for row in st.book["asks"]}
    bids = {row["price"]: row["size"] for row in st.book["bids"]}
    assert "0.99" not in asks
    assert asks["0.98"] == "50"
    assert bids["0.62"] == "30"
    assert st.book["hash"] == "h3"
    assert st.book_received_at_ms == st.last_event_at_ms
    snap = st.snapshot_dict(now_ms=st.last_event_at_ms)
    assert snap["best_bid"] == "0.62"
    assert snap["best_ask"] == "0.98"


def test_freshness_uses_book_receipt_not_event_traffic():
    """Deltas kept last_event_at_ms fresh while the book itself was old."""
    m, h, st = _manager_with()
    st.apply_book(_book(0.60, 1000), local_now_ms=1000)
    st.last_event_at_ms = 9000
    snap = st.snapshot_dict(now_ms=9000)
    assert snap["book_received_at_ms"] == 1000
    assert snap["book_age_ms"] == 8000
    assert snap["availability"] == "aging"
    assert st.snapshot_dict(now_ms=20000)["availability"] == "stale"


def test_live_top_of_book_reports_book_receipt_and_fresh_book_levels(monkeypatch):
    from polymarket_mcp import live as livemod
    from polymarket_mcp.server import get_live_top_of_book

    m = LiveBookManager()
    st = AssetStream(asset_id="A1")
    st.apply_book(_book(0.60, 2000), local_now_ms=2000)
    st.last_event_at_ms = 9000
    st.best_bid_ask = {"best_bid": "0.10", "best_ask": "0.11",
                       "spread": "0.01", "timestamp": 1000}
    m._streams["A1"] = st
    monkeypatch.setattr(livemod, "get_manager", lambda: m)
    out = asyncio.run(get_live_top_of_book("A1"))
    assert out["received_at_ms"] == 2000
    assert out["bid"] == "0.6"
    assert out["ask"] == "0.99"


def test_live_top_of_book_bootstraps_cold_subscription_from_rest(monkeypatch):
    from polymarket_mcp import live as livemod
    from polymarket_mcp.server import get_live_top_of_book

    m = LiveBookManager()
    st = AssetStream(asset_id="A1")

    async def subscribe(asset_id):
        m._streams[asset_id] = st
        return st

    async def rest_fallback(state):
        state.apply_book(_book(0.60, 2000), local_now_ms=2000, source="clob_rest_snapshot")

    monkeypatch.setattr(m, "subscribe", subscribe)
    monkeypatch.setattr(m, "_rest_fallback", rest_fallback)
    monkeypatch.setattr(livemod, "get_manager", lambda: m)

    out = asyncio.run(get_live_top_of_book("A1"))

    assert out["bid"] == "0.6"
    assert out["ask"] == "0.99"
    assert out["tick_size"] == "0.01"
    assert out["source"] == "clob_rest_snapshot"


def test_midpoint_matches_book():
    """get_live_book midpoint should agree with REST midpoint logic."""
    from decimal import Decimal

    from polymarket_mcp.live import AssetStream

    st = AssetStream(asset_id="M")
    book = NS(bids=_levels([0.01, 0.33]), asks=_levels([0.99, 0.35]),
              hash="h", min_order_size=5, tick_size="0.01", neg_risk=False,
              last_trade_price=None, timestamp=2000)
    st.apply_book(book, local_now_ms=2000)
    snap = st.snapshot_dict(now_ms=2000)
    mid = (Decimal(str(snap["best_bid"])) + Decimal(str(snap["best_ask"]))) / 2
    assert mid == Decimal("0.34")  # (0.33+0.35)/2
