import asyncio

import pytest

from polymarket_mcp import server


@pytest.mark.parametrize("name", ["cancel_order", "cancel_all_orders", "setup_trading_approvals",
                                  "save_api_credentials", "derive_api_credentials", "revoke_api_credentials"])
def test_autonomous_never_allows_privileged_tools(monkeypatch, name):
    monkeypatch.setenv("POLY_AUTONOMOUS", "1")
    monkeypatch.setenv("POLY_AUTONOMOUS_TOOLS", name + ",guard_status")
    assert server._autonomous_tool_allowed(name) is False
    assert server._autonomous_tool_allowed("guard_status") is True
    assert server._autonomous_tool_allowed("search_markets") is False


def test_autonomous_order_stays_dry_run(monkeypatch):
    from unittest.mock import AsyncMock

    from polymarket_mcp.config import Settings

    monkeypatch.setenv("POLY_AUTONOMOUS", "1")
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    monkeypatch.setattr(server, "_resolve_market", AsyncMock(return_value=(None, "111", "token_id", None)))
    secure = AsyncMock(side_effect=AssertionError)
    monkeypatch.setattr(server.clientmod, "build_secure_client", secure)
    assert asyncio.run(server.place_market_order("round", "UP", "SELL", shares="1", confirm=True))["status"] == "DRY_RUN"
    secure.assert_not_awaited()
