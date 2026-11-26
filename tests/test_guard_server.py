"""Offline tests for the BUY depth preview and live submission boundary."""
import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest
from polymarket._internal.actions.orders.estimate import _calculate_buy_market_price
from polymarket.models.clob.order_book import OrderBook

from polymarket_mcp import safety, server
from polymarket_mcp.config import Settings


def test_preview_buy_depth_is_registered():
    tools = asyncio.run(server.mcp.list_tools())
    assert "preview_buy_depth" in {tool.name for tool in tools}


@pytest.mark.parametrize(
    ("asks", "expected_shares", "expected_notional", "expected_worst"),
    [
        ([("0.73", "100"), ("0.55", "9"), ("0.50", "2")], "5", "2.65", "0.55"),
        ([("0.72", "5")], "5", "3.60", "0.72"),
        ([("0.73", "100"), ("0.50", "2.5")], "2.5", "1.25", "0.50"),
        ([("0.73", "100")], "0", "0", None),
        ([], "0", "0", None),
    ],
)
def test_preview_buy_depth_uses_sdk_book(monkeypatch, asks, expected_shares, expected_notional, expected_worst):
    book = OrderBook.model_validate({
        "market": "0x" + "11" * 32,
        "asset_id": "111",
        "timestamp": "1794029100000",
        "bids": [],
        "asks": [{"price": price, "size": size} for price, size in asks],
        "min_order_size": "5",
        "tick_size": "0.01",
        "neg_risk": False,
        "hash": "test",
    })
    resolve = AsyncMock(return_value=(None, "111", "token_id", None))
    public = Mock(get_order_book=AsyncMock(return_value=book), close=AsyncMock())
    monkeypatch.setattr(server, "_resolve_market", resolve)
    monkeypatch.setattr(server.clientmod, "build_public_client", lambda: public)
    monkeypatch.setattr(server.clientmod, "build_secure_client", AsyncMock(side_effect=AssertionError))
    monkeypatch.setattr(server, "_settings", Mock(side_effect=AssertionError))

    result = asyncio.run(server.preview_buy_depth("btc-updown-5m-1794029100", "UP"))

    resolve.assert_awaited_once_with("btc-updown-5m-1794029100", "yes")
    public.get_order_book.assert_awaited_once_with(asset_id="111")
    public.close.assert_awaited_once()
    assert result["status"] == "DRY_RUN"
    assert Decimal(result["estimated_shares"]) == Decimal(expected_shares)
    assert Decimal(result["estimated_notional_before_fees"]) == Decimal(expected_notional)
    assert result["worst_price"] == expected_worst
    if expected_worst is not None:
        assert _calculate_buy_market_price(
            book.asks, Decimal(result["estimated_notional_before_fees"]), "FOK"
        ) == Decimal(expected_worst)
    assert result["book_timestamp"] == book.timestamp.isoformat()
    assert result["max_spend_including_fees"] == "5"
    assert result["all_in_spend_verified"] is False
    assert result["live_buy_allowed"] is False


def test_preview_rejects_wrong_asset(monkeypatch):
    monkeypatch.setattr(server, "_resolve_market", AsyncMock(return_value=(None, "111", "token_id", None)))
    public = Mock(get_order_book=AsyncMock(return_value=Mock(asset_id="222")), close=AsyncMock())
    monkeypatch.setattr(server.clientmod, "build_public_client", lambda: public)

    with pytest.raises(safety.ValidationError, match="asset"):
        asyncio.run(server.preview_buy_depth("btc-updown-5m-1794029100", "UP"))
    public.close.assert_awaited_once()


@pytest.mark.parametrize(("price", "size"), [("0", "1"), ("0.50", "-1")])
def test_preview_rejects_invalid_ask(monkeypatch, price, size):
    from polymarket.models.clob.order_book import OrderBookLevel

    book = Mock(asset_id="111", asks=(OrderBookLevel(price=price, size=size),))
    public = Mock(get_order_book=AsyncMock(return_value=book), close=AsyncMock())
    monkeypatch.setattr(server, "_resolve_market", AsyncMock(return_value=(None, "111", "token_id", None)))
    monkeypatch.setattr(server.clientmod, "build_public_client", lambda: public)

    with pytest.raises(safety.ValidationError, match="invalid ask"):
        asyncio.run(server.preview_buy_depth("round", "UP"))
    public.close.assert_awaited_once()


@pytest.mark.parametrize("batch", [False, True])
def test_live_buy_blocked_before_lookup_or_signing(monkeypatch, batch):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    resolve = AsyncMock(side_effect=AssertionError)
    build_secure = AsyncMock(side_effect=AssertionError)
    monkeypatch.setattr(server, "_resolve_market", resolve)
    monkeypatch.setattr(server.clientmod, "build_secure_client", build_secure)

    with pytest.raises(safety.ValidationError, match="Live BUY is disabled"):
        if batch:
            asyncio.run(server.place_batch_order(
                '[{"market":"round","outcome":"UP","side":"SELL","shares":"1"},'
                '{"market":"round","outcome":"UP","side":"BUY","amount":"2.50","max_price":"0.72"}]',
                confirm=True,
            ))
        else:
            asyncio.run(server.place_market_order("round", "UP", "BUY", amount="2.50", max_price="0.72", confirm=True))

    resolve.assert_not_awaited()
    build_secure.assert_not_awaited()


@pytest.mark.parametrize(("allow_live", "confirm"), [(False, True), (True, False)])
def test_legacy_buy_dry_run_unchanged(monkeypatch, allow_live, confirm):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=allow_live))
    monkeypatch.setattr(server, "_resolve_market", AsyncMock(return_value=(None, "111", "token_id", None)))
    monkeypatch.setattr(server.clientmod, "build_secure_client", AsyncMock(side_effect=AssertionError))

    result = asyncio.run(server.place_market_order("round", "UP", "BUY", amount="2.50", confirm=confirm))

    assert result["status"] == "DRY_RUN"
    assert result["amount_notional"] == "2.50"
    assert result["max_price"] is None


def test_batch_buy_dry_run_unchanged(monkeypatch):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=False))
    monkeypatch.setattr(server, "_resolve_market", AsyncMock(return_value=(None, "111", "token_id", None)))
    build_secure = AsyncMock(side_effect=AssertionError)
    monkeypatch.setattr(server.clientmod, "build_secure_client", build_secure)

    result = asyncio.run(server.place_batch_order(
        '[{"market":"round","outcome":"UP","side":"BUY","amount":"2.50","max_price":"0.72"}]',
        confirm=True,
    ))

    assert result["status"] == "DRY_RUN"
    assert result["orders"][0]["amount_notional"] == "2.50"
    assert result["orders"][0]["max_price"] == "0.72"
    build_secure.assert_not_awaited()
