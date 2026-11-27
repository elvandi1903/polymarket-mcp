import asyncio
import sqlite3
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest
from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder

from polymarket_mcp import halt, ledger_admin, safety, server
from polymarket_mcp.config import Settings
from polymarket_mcp.round_guard import BuyLedger

START = 1_794_029_100
SLUG = f"btc-updown-5m-{START}"
BASE_ENV = {
    "POLY_MAX_NOTIONAL": "5", "POLY_MAX_DAILY_NOTIONAL": "5",
    "POLY_MAX_DAILY_LOSS": "5", "POLY_BUY_USD": "1.00", "POLY_FEE_BUFFER": "0.10",
}
OPTIONAL = ("POLY_MAX_BUY_PRICE", "POLY_MAX_SLIPPAGE_TICKS", "POLY_ENTRY_MIN_S",
            "POLY_ENTRY_MAX_S", "POLY_MAX_PTB_AGE_S", "POLY_MAX_CONSECUTIVE_LOSSES")


def _env(monkeypatch, **over):
    for name in OPTIONAL:
        monkeypatch.delenv(name, raising=False)
    for key, value in (BASE_ENV | over).items():
        monkeypatch.setenv(key, value)


def _accepted(status="matched"):
    return AcceptedOrder(order_id="oid", status=status, making_amount=Decimal(1),
                         taking_amount=Decimal("1.6"), trade_ids=(), transactions_hashes=())


def _wire(monkeypatch, tmp_path, *, ask="0.60", tick="0.01", reasons=(), now=START + 40,
          response=None, build_error=None):
    path = tmp_path / "buy-ledger.sqlite3"
    monkeypatch.setenv("POLY_GUARD_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(server, "_LIVE_BUY_AT_START", True)
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    monkeypatch.setattr(server, "time", NS(time=lambda: now), raising=False)
    readiness = {
        "window_start": START, "slug": SLUG, "reasons": list(reasons),
        "assets": {"UP": "111", "DOWN": "222"},
        "books": {side: {"best_ask": Decimal(ask), "tick_size": Decimal(tick)}
                  for side in ("UP", "DOWN")},
    }
    monkeypatch.setattr(server, "_buy_readiness", AsyncMock(return_value=readiness))
    submit = AsyncMock(return_value=_accepted() if response is None else response)
    client = Mock(place_market_order=submit, close=AsyncMock())
    if build_error is not None:
        monkeypatch.setattr(server.clientmod, "build_secure_client",
                            AsyncMock(side_effect=build_error))
    else:
        monkeypatch.setattr(server.clientmod, "build_secure_client", AsyncMock(return_value=client))
    monkeypatch.setattr(server, "_buy_ledger", lambda: BuyLedger(path))
    monkeypatch.setattr(server, "_resolution_winner", AsyncMock(return_value=None))
    return NS(ledger=BuyLedger(path), submit=submit, client=client)


def _buy(outcome="UP", max_price="0.61"):
    return asyncio.run(server.place_btc5m_buy(outcome, max_price))


def test_accepted_buy_sends_fak_without_max_spend(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)

    result = _buy(max_price="0.61")

    assert result == {"slug": SLUG, "outcome": "UP", "status": "matched"}
    kwargs = w.submit.await_args.kwargs
    assert kwargs["side"] == "BUY"
    assert kwargs["amount"] == "1.00"
    assert kwargs["order_type"] == "FAK"
    assert kwargs["asset_id"] == "111"
    assert Decimal(kwargs["max_price"]) <= safety.MAX_BUY_PRICE
    assert "max_spend" not in kwargs and "shares" not in kwargs and "min_price" not in kwargs
    # An accepted FAK response is not proof of a full fill.
    assert w.ledger.claim(START) == "unknown"


def test_tool_not_registered_by_default():
    tools = {tool.name for tool in asyncio.run(server.mcp.list_tools())}
    assert "place_btc5m_buy" not in tools


def test_enable_flag_off_rejects(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    monkeypatch.setattr(server, "_LIVE_BUY_AT_START", False)

    assert _buy()["code"] == "not_enabled"
    w.submit.assert_not_awaited()


def test_allow_live_off_rejects(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=False))

    assert _buy()["code"] == "live_disabled"
    w.submit.assert_not_awaited()


def test_halt_blocks_buy_but_not_sell(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    halt.write("daily_loss_limit")

    assert _buy()["code"] == "halted"
    w.submit.assert_not_awaited()

    monkeypatch.setattr(server, "_resolve_market",
                        AsyncMock(return_value=(NS(), "111", "token_id", None)))
    sell = asyncio.run(server.place_market_order("round", "UP", "SELL", shares="1", confirm=True))
    assert sell["status"] == "matched"
    w.submit.assert_awaited_once()


@pytest.mark.parametrize("elapsed", [5, 120])
def test_outside_entry_window_rejects(monkeypatch, tmp_path, elapsed):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path, now=START + elapsed)

    assert _buy()["code"] == "outside_entry_window"
    w.submit.assert_not_awaited()
    assert w.ledger.claim(START) is None


def test_missing_required_env_names_the_variable(monkeypatch, tmp_path):
    _env(monkeypatch)
    monkeypatch.delenv("POLY_MAX_DAILY_LOSS")
    w = _wire(monkeypatch, tmp_path)

    result = _buy()
    assert result["code"] == "invalid_config"
    assert "POLY_MAX_DAILY_LOSS" in result["message"]
    w.submit.assert_not_awaited()


@pytest.mark.parametrize(("env", "ask", "limit", "code"), [
    ({}, "0.60", "0.99", "price_above_cap"),
    ({}, "0.60", "0.605", "price_off_tick"),
    ({}, "0.65", "0.61", "ask_above_limit"),
    ({}, "0.73", "0.72", "ask_above_limit"),
    ({}, "0.19", "0.20", "ask_below_floor"),
    ({}, "0.60", "0.65", "slippage_exceeded"),
    ({"POLY_MAX_BUY_PRICE": "0.70"}, "0.71", "0.72", "price_above_cap"),
    ({"POLY_MAX_BUY_PRICE": "0.80"}, "0.60", "0.61", "invalid_config"),
    ({"POLY_BUY_USD": "0.50"}, "0.60", "0.61", "invalid_config"),
    ({"POLY_BUY_USD": "6.00"}, "0.60", "0.61", "invalid_config"),
])
def test_price_and_size_guards(monkeypatch, tmp_path, env, ask, limit, code):
    _env(monkeypatch, **env)
    w = _wire(monkeypatch, tmp_path, ask=ask)

    assert _buy(max_price=limit)["code"] == code
    w.submit.assert_not_awaited()
    assert w.ledger.claim(START) is None


def test_readiness_failure_rejects_before_reserve(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path, reasons=["ptb_stale"])

    assert _buy()["code"] == "prerequisite_failed"
    w.submit.assert_not_awaited()
    assert w.ledger.claim(START) is None


def test_entry_at_cap_is_accepted(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path, ask="0.72")

    assert _buy(max_price="0.72")["status"] == "matched"
    assert w.submit.await_args.kwargs["max_price"] == "0.72"


def test_lowered_env_cap_accepts_lower_ask(monkeypatch, tmp_path):
    _env(monkeypatch, POLY_MAX_BUY_PRICE="0.70")
    _wire(monkeypatch, tmp_path, ask="0.70")

    assert _buy(max_price="0.70")["status"] == "matched"


def test_two_parallel_calls_send_exactly_once(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)

    async def both():
        return await asyncio.gather(server.place_btc5m_buy("UP", "0.61"),
                                    server.place_btc5m_buy("DOWN", "0.61"))

    results = asyncio.run(both())
    assert w.submit.await_count == 1
    assert sorted(r["status"] for r in results) == ["matched", "rejected"]


def test_daily_budget_exhausted(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    w.ledger.reserve(START - 300, Decimal("4.00"), Decimal(5), now=START - 300)
    w.ledger.resolve(START - 300, "matched")

    assert _buy()["code"] == "reservation_rejected"
    w.submit.assert_not_awaited()


def test_client_failure_aborts_claim(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path, build_error=RuntimeError("no client"))

    with pytest.raises(RuntimeError):
        _buy()
    assert w.ledger.claim(START) is None


def test_submit_exception_stays_unknown_and_charged(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    w.submit.side_effect = TimeoutError("network")

    with pytest.raises(TimeoutError):
        _buy()
    assert w.ledger.claim(START) == "unknown"
    with pytest.raises(safety.ValidationError, match="already claimed"):
        w.ledger.reserve(START, Decimal("1.10"), Decimal(5), now=START)


def test_unreconciled_claim_blocks_until_resolved(monkeypatch, tmp_path):
    _env(monkeypatch)
    _wire(monkeypatch, tmp_path)
    assert _buy()["status"] == "matched"

    monkeypatch.setattr(server, "time", NS(time=lambda: START + 340), raising=False)
    assert _buy()["code"] == "unreconciled_claim"

    assert ledger_admin.main(["resolve", str(START), "matched"]) == 0
    assert _buy()["status"] == "matched"


def test_daily_loss_limit_writes_halt(monkeypatch, tmp_path):
    _env(monkeypatch, POLY_MAX_DAILY_LOSS="0.50")
    w = _wire(monkeypatch, tmp_path)
    w.ledger.reserve(START - 300, Decimal("1.10"), Decimal(5), now=START - 300,
                     outcome="UP", notional="1.00")
    w.ledger.resolve(START - 300, "matched")
    monkeypatch.setattr(server, "_resolution_winner", AsyncMock(return_value="DOWN"))

    assert _buy()["code"] == "loss_limit"
    w.submit.assert_not_awaited()
    assert halt.read()["reason"] == "daily_loss_limit"


def test_consecutive_losses_write_halt(monkeypatch, tmp_path):
    _env(monkeypatch, POLY_MAX_CONSECUTIVE_LOSSES="2", POLY_MAX_DAILY_LOSS="99")
    w = _wire(monkeypatch, tmp_path)
    for offset in (600, 300):
        w.ledger.reserve(START - offset, Decimal("1.10"), Decimal(99), now=START - offset,
                         outcome="UP", notional="1.00")
        w.ledger.resolve(START - offset, "matched")
    monkeypatch.setattr(server, "_resolution_winner", AsyncMock(return_value="DOWN"))

    assert _buy()["code"] == "loss_limit"
    assert halt.read()["reason"] == "consecutive_loss_limit"


def test_rejected_order_releases_budget_not_claim(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path,
              response=RejectedOrder(code="unmatched", message="no match"))

    result = _buy()
    assert result["code"] == "unmatched"
    assert w.ledger.claim(START) == "no_fill"
    monkeypatch.setattr(server, "time", NS(time=lambda: START + 340), raising=False)
    assert _buy()["status"] != "unreconciled_claim"


def test_migration_of_old_schema(tmp_path):
    path = tmp_path / "old.sqlite3"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE claims (round INTEGER PRIMARY KEY, day INTEGER NOT NULL, "
               "amount TEXT NOT NULL, status TEXT NOT NULL)")
    db.execute("INSERT INTO claims VALUES (?, ?, ?, 'no_fill')", (START, START // 86400, "1.1"))
    db.commit()
    db.close()
    path.chmod(0o600)

    ledger = BuyLedger(path)
    assert ledger.claim(START) == "no_fill"
    ledger.reserve(START + 300, Decimal("1.10"), Decimal(5), now=START + 300,
                   slug="s", outcome="UP", max_price="0.6", notional="1.00")
    assert ledger.claim(START + 300) == "unknown"


def test_ledger_admin_rejects_unknown_status(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("POLY_GUARD_STATE_DIR", str(tmp_path))
    assert ledger_admin.main(["resolve", str(START), "matched"]) == 1
    assert "rejected" in capsys.readouterr().out


def test_batch_and_market_orders_still_refuse_live_buy(monkeypatch):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    build = AsyncMock(side_effect=AssertionError)
    monkeypatch.setattr(server.clientmod, "build_secure_client", build)
    with pytest.raises(safety.ValidationError, match="Live BUY is disabled"):
        asyncio.run(server.place_market_order("round", "UP", "BUY", amount="2.50",
                                              max_price="0.72", confirm=True))
    with pytest.raises(safety.ValidationError, match="Live BUY is disabled"):
        asyncio.run(server.place_batch_order(
            '[{"market":"round","outcome":"UP","side":"BUY","amount":"2.50","max_price":"0.72"}]',
            confirm=True))
    build.assert_not_awaited()
