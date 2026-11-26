"""Offline guard audit and process watchdog regressions."""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from polymarket.models.clob.order_response import RejectedOrder

from polymarket_mcp import safety, server
from polymarket_mcp.config import Settings


@pytest.fixture
def state(monkeypatch, tmp_path):
    monkeypatch.setenv("POLY_GUARD_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(server.observability, "state", server.observability.GuardState())
    monkeypatch.setattr(server, "_settings", lambda: Settings())
    monkeypatch.setattr(server, "_resolve_market", AsyncMock(return_value=(None, "111", "token_id", None)))
    monkeypatch.setattr(server.clientmod, "build_secure_client", AsyncMock(side_effect=AssertionError))
    return tmp_path


def _rows(state):
    return [json.loads(line) for line in (state / "guard-audit.jsonl").read_text().splitlines()]


def test_guard_audit_appends_and_status_keeps_last_ten(state):
    for _ in range(12):
        result = asyncio.run(server.place_market_order("SECRET_MARKET", "UP", "BUY", amount="2.5"))
        assert result["status"] == "DRY_RUN"

    assert (state / "guard-audit.jsonl").is_file()
    rows = _rows(state)
    assert len(rows) == 12
    assert len({row["request_id"] for row in rows}) == 12
    assert all(row["event"] == "decision" and row["outcome"] == "dry_run" for row in rows)
    assert "SECRET_MARKET" not in json.dumps(rows)
    assert (state / "guard-audit.jsonl").stat().st_mode & 0o777 == 0o600
    status = asyncio.run(server.guard_status())
    assert status["recent_decisions"] == rows[-10:]
    assert status["audit"]["failures"] == 0


def test_blocked_live_buy_is_audited(state, monkeypatch):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    with pytest.raises(safety.ValidationError, match="Live BUY is disabled"):
        asyncio.run(server.place_market_order("round", "UP", "BUY", amount="2.50", confirm=True))
    assert [(row["event"], row["outcome"]) for row in _rows(state)] == [("decision", "live_buy_blocked")]
    server.clientmod.build_secure_client.assert_not_awaited()


def test_audit_failure_does_not_change_dry_run_but_blocks_live(state, monkeypatch):
    (state / "guard-audit.jsonl").mkdir()
    result = asyncio.run(server.place_market_order("round", "UP", "BUY", amount="2.50"))
    assert result["status"] == "DRY_RUN"
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    with pytest.raises(safety.ValidationError, match="audit unavailable"):
        asyncio.run(server.place_market_order("round", "UP", "SELL", shares="1", confirm=True))
    server.clientmod.build_secure_client.assert_not_awaited()
    status = asyncio.run(server.guard_status())
    assert status["audit"]["failures"] >= 2
    assert status["recent_decisions"][-1]["outcome"] == "audit_unavailable"


def test_audit_does_not_follow_symlinks(state):
    target = state / "untouched"
    target.write_text("original", encoding="utf-8")
    (state / "guard-audit.jsonl").symlink_to(target)

    result = asyncio.run(server.place_market_order("round", "UP", "BUY", amount="2.50"))

    assert result["status"] == "DRY_RUN"
    assert target.read_text(encoding="utf-8") == "original"
    assert asyncio.run(server.guard_status())["audit"] == {"ok": False, "failures": 1}


def test_audit_failure_after_client_creation_blocks_submit(state, monkeypatch):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    client = Mock(place_market_order=AsyncMock(side_effect=AssertionError), close=AsyncMock())

    async def build(_settings):
        log = state / "guard-audit.jsonl"
        log.rename(state / "saved-audit.jsonl")
        log.mkdir()
        return client

    monkeypatch.setattr(server.clientmod, "build_secure_client", build)
    with pytest.raises(safety.ValidationError, match="audit unavailable"):
        asyncio.run(server.place_market_order("round", "UP", "SELL", shares="1", confirm=True))

    client.place_market_order.assert_not_awaited()
    client.close.assert_awaited_once()
    status = asyncio.run(server.guard_status())
    assert status["audit"]["ok"] is False
    assert status["recent_decisions"][-1]["outcome"] == "audit_unavailable"


def test_validation_rejection_never_logs_raw_input(state):
    with pytest.raises(safety.ValidationError):
        asyncio.run(server.place_market_order("round", "UP", "BUY", amount="SECRET_INVALID_AMOUNT"))
    [row] = _rows(state)
    assert row["outcome"] == "rejected"
    assert set(row) == {"timestamp", "request_id", "tool", "event", "outcome"}
    assert "SECRET_INVALID_AMOUNT" not in json.dumps(row)


@pytest.mark.parametrize("batch", [False, True])
def test_sdk_rejection_is_logged_without_raw_response(state, monkeypatch, batch):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    response = RejectedOrder(code="unmatched", message="SECRET_RESPONSE")
    submit = AsyncMock(return_value=[response] if batch else response)
    client = Mock(place_market_order=submit, post_orders=submit, create_market_order=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(server.clientmod, "build_secure_client", AsyncMock(return_value=client))

    if batch:
        asyncio.run(server.place_batch_order('[{"market":"round","outcome":"UP","side":"SELL","shares":"1"}]', confirm=True))
    else:
        asyncio.run(server.place_market_order("round", "UP", "SELL", shares="1", confirm=True))

    submit.assert_awaited_once()
    rows = _rows(state)
    assert [(row["event"], row["outcome"]) for row in rows] == [
        ("decision", "allowed"), ("order", "submit_started"), ("order", "unmatched"),
    ]
    assert len({row["request_id"] for row in rows}) == 1
    assert "SECRET_RESPONSE" not in json.dumps(rows)


def test_sdk_exception_is_unknown_and_not_retried(state, monkeypatch):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))
    submit = AsyncMock(side_effect=TimeoutError("SECRET_EXCEPTION"))
    client = Mock(place_market_order=submit, close=AsyncMock())
    monkeypatch.setattr(server.clientmod, "build_secure_client", AsyncMock(return_value=client))
    with pytest.raises(TimeoutError):
        asyncio.run(server.place_market_order("round", "UP", "SELL", shares="1", confirm=True))
    submit.assert_awaited_once()
    assert _rows(state)[-1]["outcome"] == "unknown"
    assert "SECRET_EXCEPTION" not in json.dumps(_rows(state))


def test_audit_failure_after_submit_preserves_sdk_response(state, monkeypatch):
    monkeypatch.setattr(server, "_settings", lambda: Settings(allow_live=True))

    async def submit(**_kwargs):
        log = state / "guard-audit.jsonl"
        log.rename(state / "saved-audit.jsonl")
        log.mkdir()
        return {"status": "matched"}

    client = Mock(place_market_order=AsyncMock(side_effect=submit), close=AsyncMock())
    monkeypatch.setattr(server.clientmod, "build_secure_client", AsyncMock(return_value=client))
    response = asyncio.run(server.place_market_order("round", "UP", "SELL", shares="1", confirm=True))
    assert response == {"status": "matched"}
    client.place_market_order.assert_awaited_once()
    assert asyncio.run(server.guard_status())["audit"]["failures"] == 1


def test_heartbeat_write_failure_is_visible_and_recovers(state):
    heartbeat = state / "guard-heartbeat.json"
    heartbeat.mkdir()

    async def check():
        server.observability.state.heartbeat()
        status = (await server.guard_status())["heartbeat"]
        assert status["ok"] is False
        assert status["failures"] == 1
        assert status["timestamp"] is None
        assert list(state.iterdir()) == [heartbeat]

        heartbeat.rmdir()
        server.observability.state.heartbeat()
        status = (await server.guard_status())["heartbeat"]
        assert status["ok"] is True
        assert status["failures"] == 1
        assert status["timestamp"] == json.loads(heartbeat.read_text())["timestamp"]

    asyncio.run(check())


def test_cli_heartbeat_updates_and_stops_when_transport_exits(state, monkeypatch):
    monkeypatch.setattr(server, "_MCP_V2", True)
    monkeypatch.setattr(server.observability, "HEARTBEAT_INTERVAL_S", 0.01)
    path = Path(state / "guard-heartbeat.json")
    samples = []

    async def transport():
        samples.append(json.loads(path.read_text()))
        async with asyncio.timeout(1):
            while json.loads(path.read_text())["timestamp"] == samples[0]["timestamp"]:
                await asyncio.sleep(0.01)
        samples.append(json.loads(path.read_text()))

    monkeypatch.setattr(server.mcp, "run_stdio_async", transport)
    server.main([])

    assert samples[0]["pid"] == samples[1]["pid"]
    assert samples[0]["timestamp"] < samples[1]["timestamp"]
    assert json.loads(path.read_text()) == samples[1]
    assert path.stat().st_mode & 0o777 == 0o600
