"""PTB feed unit tests (offline, no network/credentials needed)."""
import asyncio
import json
import os
import tempfile
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_mcp.config import Settings
from polymarket_mcp.ptb import (
    PtbFeed,
    PtbPoint,
    PtbRecord,
    PtbStore,
    boundaries,
    canonical_ptb_point,
    current_window_start,
    parse_twap_payload,
    window_slug,
)


def test_window_math():
    assert current_window_start(1794029100) == 1794029100
    assert current_window_start(1794029100 + 299) == 1794029100
    assert current_window_start(1794029100 + 300) == 1794029400
    assert window_slug(1794029100) == "btc-updown-5m-1794029100"
    cur, nxt, c, n = boundaries(1794029100 + 7)
    assert (c, n) == (1794029100, 1794029400)
    assert cur == "btc-updown-5m-1794029100" and nxt == "btc-updown-5m-1794029400"


def test_parse_twap_payload_variations():
    p1 = parse_twap_payload({"timestamp": "2026-11-07T08:35:00.000Z", "full_accuracy_value": "75891.4170031904", "data": [], "batch": []})
    assert p1 == [{"value": "75891.4170031904", "timestamp": "2026-11-07T08:35:00.000Z"}]
    p2 = parse_twap_payload({"data": [{"value": "75900", "timestamp": "2026-11-07T08:35:01Z"}]})
    assert p2[0]["value"] == "75900"


def test_canonical_ptb_point_first_at_or_after():
    t0 = datetime(2026, 11, 7, 8, 35, tzinfo=UTC)
    pts = [
        PtbPoint(Decimal(100), t0, "2026-11-07T08:35:00Z"),
        PtbPoint(Decimal(101), t0, "2026-11-07T08:35:01Z"),
    ]
    assert canonical_ptb_point(pts, int(t0.timestamp())).value == Decimal(100)
    assert canonical_ptb_point([], int(t0.timestamp())) is None


def test_store_roundtrip_and_jsonl(tmp_path):
    path = str(tmp_path / "ptb.jsonl")
    store = PtbStore(path)
    rec = PtbRecord(
        t_start=1794029100, slug="btc-updown-5m-1794029100", ptb=Decimal("75891.4170031904"),
        point_timestamp=datetime(2026, 11, 7, 5, 25, tzinfo=UTC),
        source="polybolt:price.crypto.twap", received_at=1794029101.0,
        reconstructed_from_snapshot=True, status="captured",
        close_ptb=Decimal("75900.5"), close_ts="2026-11-07T05:29:59+00:00",
    )
    store.put(rec)
    assert os.stat(path).st_mode & 0o777 == 0o600
    store2 = PtbStore(path)
    store2.load()
    loaded = store2.get(1794029100)
    assert loaded.ptb == Decimal("75891.4170031904")
    assert loaded.reconstructed_from_snapshot is True
    assert loaded.close_ptb == Decimal("75900.5")
    assert loaded.close_ts == "2026-11-07T05:29:59+00:00"


def test_handle_text_snapshot_vs_update():
    """Snapshot points within 30s of window are reconstructed; far ones are not.

    The snapshot here has its payload timestamp exactly at the window start
    (payload.timestamp is a single int-ms, not per-point), which is the
    offset-0 case. A snapshot point 312s after window start must be ignored.
    """
    feed = PtbFeed(Settings(private_key="0x" + "11" * 32, wallet="0x6Cd089333445c89b55B7693c6009DCc229613738"))

    feed.store = PtbStore(os.path.join(tempfile.mkdtemp(), "ptb.jsonl"))

    async def go():
        raw = json.dumps({
            "v": 1, "channel": "price.crypto.twap", "seq": 1, "ts": 1794029100000,
            "snapshot": True,
            # payload.timestamp is int-ms; data[].timestamp is also int-ms (both
            # need epoch-ms parsing — see parse_rfc3339 fix for int-ms).
            "payload": {"timestamp": 1794029100000, "full_accuracy_value": "75891.4170031904",
                        "data": [{"value": "75891.4170031904", "timestamp": 1794029100000}],
                        "batch": []},
        })
        await feed._handle_text(raw)

    asyncio.new_event_loop().run_until_complete(go())
    got = feed.store.get(1794029100)
    assert got is not None and got.ptb == Decimal("75891.4170031904")
    assert got.reconstructed_from_snapshot is True
    assert got.point_timestamp.isoformat() == "2026-11-07T05:25:00+00:00"


def _twap_msg(ts_ms, value, *, snapshot=False):
    return json.dumps({
        "v": 1, "channel": "price.crypto.twap", "seq": 1, "ts": ts_ms,
        "snapshot": snapshot,
        "payload": {"timestamp": ts_ms, "full_accuracy_value": str(value),
                    "data": [{"value": str(value), "timestamp": ts_ms}], "batch": []},
    })


def test_close_point_captured_and_settled():
    feed = PtbFeed(Settings(private_key="0x" + "11" * 32, wallet="0x6Cd089333445c89b55B7693c6009DCc229613738"))
    feed.store = PtbStore(os.path.join(tempfile.mkdtemp(), "ptb-close-gap.jsonl"))

    async def go():
        t0 = 1794029100
        await feed._handle_text(_twap_msg(t0 * 1000, "61000"))
        await feed._handle_text(_twap_msg((t0 + 5) * 1000, "61010"))
        await feed._handle_text(_twap_msg((t0 + 295) * 1000, "61100"))

    asyncio.new_event_loop().run_until_complete(go())
    rec = feed.store.get(1794029100)
    assert rec.ptb == Decimal(61000)
    assert rec.close_ptb == Decimal(61100)
    assert rec.close_ts is not None
    from polymarket_mcp.ptb import settlement_outcome
    assert settlement_outcome(rec) == "Up"


def test_boundary_point_belongs_to_next_window_and_down_outcome():
    feed = PtbFeed(Settings(private_key="0x" + "11" * 32, wallet="0x6Cd089333445c89b55B7693c6009DCc229613738"))
    feed.store = PtbStore(os.path.join(tempfile.mkdtemp(), "ptb-boundary.jsonl"))

    async def go():
        t0 = 1794029100
        await feed._handle_text(_twap_msg(t0 * 1000, "61000"))
        await feed._handle_text(_twap_msg((t0 + 290) * 1000, "60990"))
        await feed._handle_text(_twap_msg((t0 + 300) * 1000, "62000"))

    asyncio.new_event_loop().run_until_complete(go())
    first = feed.store.get(1794029100)
    second = feed.store.get(1794029400)
    assert first.close_ptb == Decimal(60990)
    assert second.ptb == Decimal(62000)
    assert second.close_ptb is None
    from polymarket_mcp.ptb import settlement_outcome
    assert settlement_outcome(first) == "Down"
    assert settlement_outcome(second) is None


def test_get_ptb_tool_exposes_close_and_outcome():
    from types import SimpleNamespace
    from unittest.mock import patch

    from polymarket_mcp.server import get_ptb

    rec = PtbRecord(
        t_start=1794029100, slug=window_slug(1794029100), ptb=Decimal(61420),
        point_timestamp=datetime(2026, 11, 7, 5, 25, tzinfo=UTC),
        source="test", received_at=1794029101.0,
        reconstructed_from_snapshot=False, status="captured",
        close_ptb=Decimal(61512), close_ts="2026-11-07T05:29:59+00:00",
    )
    feed = SimpleNamespace(store=SimpleNamespace(get=lambda t: rec))
    with patch("polymarket_mcp.ptb.get_feed", lambda: feed):
        out = asyncio.new_event_loop().run_until_complete(get_ptb(1794029100))
    assert out["ptb"] == "61420"
    assert out["close"] == "61512"
    assert out["outcome"] == "Up"


def test_snapshot_far_point_is_unavailable():
    """A snapshot point 312s after window start must NOT be captured."""
    feed = PtbFeed(Settings(private_key="0x" + "11" * 32, wallet="0x6Cd089333445c89b55B7693c6009DCc229613738"))

    feed.store = PtbStore(os.path.join(tempfile.mkdtemp(), "ptb2.jsonl"))

    async def go():
        far_ts_ms = (1794051692) * 1000  # 312s after window 1794051600
        raw = json.dumps({
            "v": 1, "channel": "price.crypto.twap", "seq": 1, "ts": far_ts_ms,
            "snapshot": True,
            "payload": {"timestamp": far_ts_ms, "full_accuracy_value": "75919.0",
                        "data": [{"value": "75919.0", "timestamp": far_ts_ms}], "batch": []},
        })
        await feed._handle_text(raw)

    asyncio.new_event_loop().run_until_complete(go())
    assert feed.store.get(1794051600) is None


def test_live_update_captures_exact_boundary():
    """Live (non-snapshot) point exactly at t_start is captured."""
    feed = PtbFeed(Settings(private_key="0x" + "11" * 32, wallet="0x6Cd089333445c89b55B7693c6009DCc229613738"))

    feed.store = PtbStore(os.path.join(tempfile.mkdtemp(), "ptb3.jsonl"))

    async def go():
        raw = json.dumps({
            "v": 1, "channel": "price.crypto.twap", "seq": 2, "ts": 1794051600000,
            "payload": {"timestamp": 1794051600000, "full_accuracy_value": "76042.67207536352",
                        "data": [], "batch": []},
        })
        await feed._handle_text(raw)

    asyncio.new_event_loop().run_until_complete(go())
    got = feed.store.get(1794051600)
    assert got is not None and got.ptb == Decimal("76042.67207536352")
    assert got.reconstructed_from_snapshot is False


def test_error_handling_auth_failed_does_not_loop_blindly():
    feed = PtbFeed(Settings(private_key="0x" + "11" * 32, wallet="0x6Cd089333445c89b55B7693c6009DCc229613738"))
    async def go():
        raw = json.dumps({"op": "error", "channel": "price.crypto.twap", "rid": "a1", "code": "auth_failed", "message": "bad creds"})
        with pytest.raises(PermissionError):
            await feed._handle_text(raw)
    asyncio.new_event_loop().run_until_complete(go())


def test_error_handling_auth_invalid_code_is_also_hard_failure():
    """PolyBolt live wire uses code 'auth_invalid' (not only 'auth_failed').

    Reproduced: when POLY_API_* are empty the server replies auth_invalid,
    then subscribe replies auth_required. Both must be fatal for this
    connection, otherwise the feed silently loops and ptb_status stays stale.
    """
    feed = PtbFeed(Settings(private_key="0x" + "11" * 32, wallet="0x6Cd089333445c89b55B7693c6009DCc229613738"))
    async def go():
        raw = json.dumps({"op": "error", "rid": "a1", "code": "auth_invalid"})
        with pytest.raises(PermissionError):
            await feed._handle_text(raw)
    asyncio.new_event_loop().run_until_complete(go())
