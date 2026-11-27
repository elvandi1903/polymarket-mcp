import asyncio
import sqlite3
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest
from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder

from polymarket_mcp import halt, safety, server
from polymarket_mcp.config import Settings
from polymarket_mcp.round_guard import BuyLedger, loss_report

START = 1_794_029_100
SLUG = f"btc-updown-5m-{START}"
BASE_ENV = {
    "POLY_MAX_NOTIONAL": "5", "POLY_MAX_DAILY_NOTIONAL": "5",
    "POLY_MAX_DAILY_LOSS": "5", "POLY_BUY_USD": "1.00", "POLY_FEE_BUFFER": "0.10",
}


def _env(monkeypatch, **over):
    for name in ("POLY_MAX_BUY_PRICE", "POLY_MAX_SLIPPAGE_TICKS", "POLY_ENTRY_MIN_S",
                 "POLY_ENTRY_MAX_S", "POLY_MAX_PTB_AGE_S", "POLY_MAX_CONSECUTIVE_LOSSES"):
        monkeypatch.delenv(name, raising=False)
    for key, value in (BASE_ENV | over).items():
        monkeypatch.setenv(key, value)


def _accepted(status="matched", making="1.00", taking="1.60", trades=("t1",)):
    return AcceptedOrder(order_id="oid", status=status, making_amount=Decimal(making),
                         taking_amount=Decimal(taking), trade_ids=trades,
                         transactions_hashes=())


def _wire(monkeypatch, tmp_path, *, ask="0.60", response=None, now=START + 40):
    path = tmp_path / "buy-ledger.sqlite3"
    monkeypatch.setenv("POLY_GUARD_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(server, "_LIVE_BUY_AT_START", True)
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    monkeypatch.setattr(server, "time", NS(time=lambda: now), raising=False)
    monkeypatch.setattr(server, "_buy_readiness", AsyncMock(return_value={
        "window_start": START, "slug": SLUG, "reasons": [],
        "assets": {"UP": "111", "DOWN": "222"},
        "books": {side: {"best_ask": Decimal(ask), "tick_size": Decimal("0.01")}
                  for side in ("UP", "DOWN")},
    }))
    submit = AsyncMock(return_value=_accepted() if response is None else response)
    client = Mock(place_market_order=submit, close=AsyncMock())
    monkeypatch.setattr(server.clientmod, "build_secure_client", AsyncMock(return_value=client))
    monkeypatch.setattr(server, "_buy_ledger", lambda: BuyLedger(path))
    monkeypatch.setattr(server, "_resolution_winner", AsyncMock(return_value=None))
    return NS(ledger=BuyLedger(path), submit=submit, client=client, path=path)


def _buy(outcome="UP", max_price="0.61"):
    return asyncio.run(server.place_btc5m_buy(outcome, max_price))


# -- 1. response classification -------------------------------------------
def test_fak_not_filled_releases_budget_but_keeps_round(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path,
              response=RejectedOrder(code="fak_not_filled", message="no match"))

    assert _buy()["code"] == "fak_not_filled"
    assert w.ledger.claim(START) == "no_fill"
    with pytest.raises(safety.ValidationError, match="already claimed"):
        w.ledger.reserve(START, Decimal("1.10"), Decimal(5), now=START)
    # budget released: a different round can still reserve the full cap
    w.ledger.reserve(START + 300, Decimal(5), Decimal(5), now=START + 300)


@pytest.mark.parametrize("code", ["unmatched", "not_enough_balance", "fok_not_filled",
                                  "market_not_ready", "post_only_mode"])
def test_no_fill_codes(monkeypatch, tmp_path, code):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path, response=RejectedOrder(code=code, message="x"))
    _buy()
    assert w.ledger.claim(START) == "no_fill"


def test_matched_records_fill_and_does_not_block_next_round(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)

    assert _buy()["status"] == "matched"
    row = w.ledger.all_rows()[-1]
    assert row["status"] == "matched"
    assert row["making"] == "1.00" and row["taking"] == "1.60"
    assert Decimal(row["price"]) == Decimal("0.625")
    assert row["trade_ids"] == 1

    monkeypatch.setattr(server, "time", NS(time=lambda: START + 340), raising=False)
    assert _buy()["status"] == "matched"


def test_partial_fak_stays_matched_with_actual_fill(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path, response=_accepted(making="0.40", taking="0.65"))

    _buy()
    row = w.ledger.all_rows()[-1]
    assert row["status"] == "matched"
    assert row["making"] == "0.40" and row["taking"] == "0.65"


@pytest.mark.parametrize("response", [
    _accepted(status="live"),
    _accepted(status="delayed"),
    _accepted(taking="0"),
    RejectedOrder(code="unknown", message="ambiguous"),
    None,
])
def test_undefined_responses_stay_unknown(monkeypatch, tmp_path, response):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path, response=response)
    if response is None:
        w.submit.side_effect = TimeoutError("no response")
        with pytest.raises(TimeoutError):
            _buy()
    else:
        _buy()
    assert w.ledger.claim(START) == "unknown"


# -- 2. 12-round blocking window -----------------------------------------
def test_unknown_blocks_only_within_twelve_rounds(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    old = START - 13 * 300
    w.ledger.reserve(old, Decimal("1.10"), Decimal(99), now=old, outcome="UP")

    assert w.ledger.unknown_rounds() == [old]
    assert w.ledger.unreconciled(now=START) == []      # outside the window
    assert _buy()["status"] == "matched"               # so a BUY proceeds

    recent = START - 300
    w.ledger.reserve(recent, Decimal("1.10"), Decimal(99), now=recent, outcome="UP")
    assert w.ledger.unreconciled(now=START) == [recent]


def test_guard_status_lists_old_unknown_without_blocking(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    old = START - 13 * 300
    w.ledger.reserve(old, Decimal("1.10"), Decimal(99), now=old, outcome="UP")

    status = asyncio.run(server.guard_status())
    assert status["ledger"]["unknown_rounds"] == [old]
    assert status["ledger"]["blocking_unknown"] == []


# -- 3. loss uses the actual spend, cached resolutions -------------------
def test_loss_uses_filled_amount(tmp_path):
    rows = [{"round": START, "day": 0, "amount": "1.10", "notional": "1.00",
             "making": "0.40", "outcome": "UP", "status": "matched"}]
    report = loss_report(rows, {START: "DOWN"}, max_daily_loss=Decimal(5),
                         max_consecutive_losses=3)
    assert report["daily_loss_usd"] == "0.40"


def test_unknown_resolved_round_counts_as_loss(tmp_path):
    rows = [{"round": START, "day": 0, "amount": "1.10", "notional": "1.00",
             "making": "", "outcome": "UP", "status": "unknown"}]
    report = loss_report(rows, {START: "DOWN"}, max_daily_loss=Decimal(5),
                         max_consecutive_losses=3)
    assert report["daily_loss_usd"] == "1.00"
    assert report["consecutive_losses"] == 1


def test_resolution_is_cached_per_round(monkeypatch):
    calls = []

    async def fake(window_start):
        calls.append(window_start)
        return {"state": "resolved", "winner": "Up"}

    monkeypatch.setattr("polymarket_mcp.resolution.get_btc5m_resolution", fake)
    server._RESOLUTION_CACHE.clear()

    assert asyncio.run(server._resolution_winner(START)) == "UP"
    assert asyncio.run(server._resolution_winner(START)) == "UP"
    assert calls == [START]


def test_pending_resolution_is_cached_briefly(monkeypatch):
    calls = []

    async def fake(window_start):
        calls.append(window_start)
        return {"state": "pending"}

    monkeypatch.setattr("polymarket_mcp.resolution.get_btc5m_resolution", fake)
    server._RESOLUTION_CACHE.clear()

    asyncio.run(server._resolution_winner(START))
    asyncio.run(server._resolution_winner(START))
    assert calls == [START]


# -- 4. canonical outcome ------------------------------------------------
def test_yes_and_up_store_the_same_canonical_outcome(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    assert _buy("yes")["outcome"] == "UP"
    monkeypatch.setattr(server, "time", NS(time=lambda: START + 340), raising=False)
    assert _buy("up")["outcome"] == "UP"
    assert [row["outcome"] for row in w.ledger.all_rows()] == ["UP", "UP"]


def test_legacy_outcome_values_are_migrated(tmp_path):
    path = tmp_path / "old.sqlite3"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE claims (round INTEGER PRIMARY KEY, day INTEGER NOT NULL, "
               "amount TEXT NOT NULL, status TEXT NOT NULL, outcome TEXT NOT NULL)")
    db.execute("INSERT INTO claims VALUES (?, 0, '1.1', 'matched', 'YES')", (START,))
    db.commit()
    db.close()
    path.chmod(0o600)

    ledger = BuyLedger(path)
    ledger.all_rows()
    assert ledger.all_rows()[0]["outcome"] == "UP"
    report = loss_report(ledger.all_rows(), {START: "UP"},
                         max_daily_loss=Decimal(5), max_consecutive_losses=3)
    assert report["daily_loss_usd"] == "0"


# -- 5. autonomous binding ----------------------------------------------
def test_live_buy_without_autonomous_refuses_start(monkeypatch):
    monkeypatch.setattr(server, "_LIVE_BUY_REQUESTED", True)
    monkeypatch.setattr(server, "_AUTONOMOUS_AT_START", False)
    with pytest.raises(SystemExit, match="POLY_AUTONOMOUS=1"):
        server.main(["--transport", "stdio"])


def test_live_buy_requires_autonomous_and_allowlist():
    # Defaults in this test process: neither flag set at import time.
    assert server._LIVE_BUY_REQUESTED is False
    assert server._live_buy_registrable() is False
    assert server._LIVE_BUY_AT_START is False


def test_allowlist_can_exclude_the_buy_tool(monkeypatch):
    monkeypatch.setattr(server, "_LIVE_BUY_REQUESTED", True)
    monkeypatch.setattr(server, "_AUTONOMOUS_AT_START", True)
    monkeypatch.setattr(server, "_AUTONOMOUS_TOOLS_AT_START", "guard_status,get_btc5m_snapshot")
    assert server._live_buy_registrable() is False
    monkeypatch.setattr(server, "_AUTONOMOUS_TOOLS_AT_START", "guard_status,place_btc5m_buy")
    assert server._live_buy_registrable() is True


# -- 6. audit failure before the SDK call releases the claim -------------
def test_audit_unavailable_aborts_claim(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    monkeypatch.setattr(server.observability, "submit",
                        AsyncMock(side_effect=server.observability.AuditUnavailable("down")))

    with pytest.raises(server.observability.AuditUnavailable):
        _buy()
    assert w.ledger.claim(START) is None          # released, not "unknown"
    w.submit.assert_not_awaited()


def test_exception_after_sdk_call_stays_unknown(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    w.submit.side_effect = TimeoutError("after send")

    with pytest.raises(TimeoutError):
        _buy()
    assert w.ledger.claim(START) == "unknown"


def test_halt_file_does_not_block_sell(monkeypatch, tmp_path):
    _env(monkeypatch)
    w = _wire(monkeypatch, tmp_path)
    halt.write("manual")
    assert _buy()["code"] == "halted"
    monkeypatch.setattr(server, "_resolve_market",
                        AsyncMock(return_value=(NS(), "111", "token_id", None)))
    assert asyncio.run(server.place_market_order(
        "round", "UP", "SELL", shares="1", confirm=True))["status"] == "matched"
    w.submit.assert_awaited_once()
