import asyncio
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest
from polymarket.models.clob.order_response import RejectedOrder

from polymarket_mcp import client, safety, server
from polymarket_mcp.config import Settings

ROUND = 1_794_029_100
CONDITION = "0x" + "ab" * 32
DAY_END_ROUND = 86_100


def _ledger(tmp_path):
    from polymarket_mcp.round_guard import BuyLedger

    return BuyLedger(tmp_path / "buy-ledger.sqlite3")


def test_rejection_returns_budget_not_round_claim(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.reserve(ROUND, Decimal(3), Decimal(5), now=ROUND)
    ledger.finish(ROUND, RejectedOrder(code="unmatched", message="not stored"))
    assert ledger.claim(ROUND) == "no_fill"
    with pytest.raises(safety.ValidationError, match="already claimed"):
        ledger.reserve(ROUND, Decimal(1), Decimal(5), now=ROUND)
    ledger.reserve(ROUND + 300, Decimal(5), Decimal(5), now=ROUND + 300)


@pytest.mark.parametrize("response", [None, RejectedOrder(code="unknown", message="ambiguous")])
def test_uncertain_result_retains_budget_across_instances(tmp_path, response):
    ledger = _ledger(tmp_path)
    ledger.reserve(ROUND, Decimal("3.25"), Decimal(5), now=ROUND)
    ledger.finish(ROUND, response)
    assert _ledger(tmp_path).claim(ROUND) == "unknown"
    with pytest.raises(safety.ValidationError, match="daily"):
        _ledger(tmp_path).reserve(ROUND + 300, Decimal(2), Decimal(5), now=ROUND + 300)


def test_unfinished_claim_is_unknown_and_budget_rolls_at_utc_midnight(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.reserve(DAY_END_ROUND, Decimal(5), Decimal(5), now=DAY_END_ROUND)
    assert _ledger(tmp_path).claim(DAY_END_ROUND) == "unknown"
    ledger.reserve(86_400, Decimal(5), Decimal(5), now=86_400)


def test_same_round_can_only_be_reserved_once_concurrently(tmp_path):
    def reserve(_):
        try:
            _ledger(tmp_path).reserve(ROUND, Decimal(2), Decimal(5), now=ROUND)
        except safety.ValidationError as exc:
            assert "already claimed" in str(exc)
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(reserve, range(2))) == [False, True]


def test_reconcile_filters_account_evidence_and_never_resolves_unknown(tmp_path, monkeypatch):
    ledger = _ledger(tmp_path)
    ledger.reserve(ROUND, Decimal(3), Decimal(5), now=ROUND)
    monkeypatch.setattr(server, "_buy_ledger", lambda: ledger, raising=False)
    monkeypatch.setattr(server, "_settings", lambda: Settings())
    public = Mock(get_market=AsyncMock(return_value=NS(slug=f"btc-updown-5m-{ROUND}", condition_id=CONDITION)),
                  close=AsyncMock())

    async def pages():
        yield NS(items=[NS(condition_id=CONDITION, model_dump=lambda **_: {"condition_id": CONDITION}),
                        NS(condition_id="other")])

    account = Mock(list_open_orders=Mock(side_effect=lambda **_: pages()),
                   list_positions=Mock(side_effect=lambda **_: pages()), close=AsyncMock())
    monkeypatch.setattr(server.clientmod, "build_public_client", lambda: public)
    monkeypatch.setattr(server.clientmod, "build_readonly_account_client", AsyncMock(return_value=account), raising=False)
    report = asyncio.run(server.reconcile_round(ROUND))
    assert report["claim_status"] == "unknown"
    assert report["open_orders"] == report["positions"] == [{"condition_id": CONDITION}]
    account.list_open_orders.assert_called_once_with(market=CONDITION)
    account.list_positions.assert_called_once_with(condition_id=CONDITION, full_history=True)
    assert ledger.claim(ROUND) == "unknown"
    public.close.assert_awaited_once()
    account.close.assert_awaited_once()


def test_readonly_account_never_bootstraps_missing_credentials(monkeypatch):
    create = AsyncMock(side_effect=AssertionError)
    monkeypatch.setattr(client.AsyncSecureClient, "_create", create)
    with pytest.raises(client.PreflightError, match="existing CLOB credentials"):
        asyncio.run(client.build_readonly_account_client(Settings()))
    create.assert_not_awaited()


def test_readonly_account_disables_sdk_credential_recovery(monkeypatch):
    monkeypatch.setattr(client, "assert_proxy_wallet", lambda _settings: {"wallet_type": "POLY_PROXY"})
    account = Mock(wallet_type="POLY_PROXY")
    create = AsyncMock(return_value=account)
    monkeypatch.setattr(client.AsyncSecureClient, "_create", create)
    result = asyncio.run(client.build_readonly_account_client(Settings(
        private_key="dummy-key", wallet="dummy-wallet", api_key="dummy", api_secret="dummy", api_passphrase="dummy",
    )))
    assert result is account
    assert create.await_args.kwargs["validate_credentials"] is False
    assert create.await_args.kwargs["credentials"] is not None
