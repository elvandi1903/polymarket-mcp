import asyncio
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from polymarket_mcp import live, ptb, server
from polymarket_mcp.config import Settings

START = 1_794_029_100
NOW = START + 42
SLUG = f"btc-updown-5m-{START}"


@pytest.mark.parametrize(("failure", "expected"), [
    ("ptb_missing", "ptb_not_captured"), ("ptb_stale", "ptb_stale"),
    ("ptb_invalid_config", "invalid_POLY_MAX_PTB_AGE_S"),
    ("book_missing", "book_DOWN_missing"), ("book_stale", "book_UP_stale"),
    ("book_future", "book_UP_stale"), ("accepting", "market_not_accepting_orders"),
    ("closed", "closed_only"), ("missing_closed", "closed_only_unverified"),
    ("rollover", "round_changed"), (None, None),
])
def test_buy_readiness_is_diagnostic_and_fail_closed(monkeypatch, failure, expected):
    clock = Mock(return_value=NOW)
    monkeypatch.setattr(server, "time", NS(time=clock), raising=False)
    monkeypatch.setenv("POLY_MAX_PTB_AGE_S", "nan" if failure == "ptb_invalid_config" else "10")
    market = NS(slug=SLUG, state=NS(accepting_orders=failure != "accepting"), version=None,
                outcomes=NS(yes=NS(token_id="111"), no=NS(token_id="222")))
    public = Mock(get_market=AsyncMock(return_value=market), close=AsyncMock())
    feed = NS(_running=True, auth_failed=False, last_twap_at=NOW - (90 if failure == "ptb_stale" else 1),
              captured=lambda _: NS(status="pending" if failure == "ptb_missing" else "captured", ptb=Decimal(10)))
    monkeypatch.setattr(ptb, "_FEED", feed)
    monkeypatch.setattr(server.clientmod, "build_public_client", lambda: public)
    monkeypatch.setattr(server, "_settings", lambda: Settings())
    manager = live.LiveBookManager()
    for asset in ("111", "222"):
        if asset == "222" and failure == "book_missing":
            continue
        age = {"book_stale": 20_000, "book_future": -100}.get(failure, 100) if asset == "111" else 100
        stream = live.AssetStream(asset_id=asset)
        stream.apply_book(NS(bids=[NS(price=Decimal("0.4"), size=Decimal(5))],
                             asks=[NS(price=Decimal("0.5"), size=Decimal(5))]), local_now_ms=NOW * 1000 - age)
        manager._streams[asset] = stream
    monkeypatch.setattr(live, "get_manager", lambda: manager)

    async def closed_only():
        if failure == "rollover":
            clock.return_value = START + 300
        return None if failure == "missing_closed" else failure == "closed"

    account = Mock(get_closed_only_mode=closed_only, close=AsyncMock())
    monkeypatch.setattr(server.clientmod, "build_readonly_account_client", AsyncMock(return_value=account))
    trading = AsyncMock(side_effect=AssertionError)
    monkeypatch.setattr(server.clientmod, "build_secure_client", trading)

    result = asyncio.run(server.check_buy_readiness())
    assert result["window_start"] == START
    assert result["live_buy_allowed"] is False
    assert result["prerequisites_met"] is (expected is None)
    assert result["reasons"] == ([] if expected is None else [expected])
    trading.assert_not_awaited()
