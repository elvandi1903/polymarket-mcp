"""Resolution tools: all HTTP mocked, no secrets, no disk writes."""
import asyncio
import time

from polymarket_mcp import resolution
from polymarket_mcp.config import Settings  # noqa: F401  (compat check)

WINDOW = (1795400000 // 300) * 300


class Resp:
    def __init__(self, status, data=None, headers=None):
        self.status_code = status
        self._data = data
        self.headers = headers or {}

    def json(self):
        if self._data is None:
            raise ValueError("no json")
        return self._data


class Client:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        return self.handler(url, dict(params or {}))


def event(condition="0x" + "ab" * 32, version="v1"):
    return {
        "slug": "btc-updown-5m-WINDOW",
        "resolutionSource": "Chainlink BTC/USD TWAP",
        "markets": [{
            "conditionId": condition, "version": version,
            "resolutionSource": "Chainlink BTC/USD TWAP",
            "outcomes": '["Up","Down"]',
            "umaResolutionStatus": "resolved",
        }],
    }


def resolved_handler():
    def handler(url, params):
        if "gamma-api" in url:
            return Resp(200, event())
        if url.endswith("/resolutions"):
            return Resp(200, {"data": [{"condition_id": params.get("condition"),
                                        "status": "resolved",
                                        "last_update_timestamp": "1795400300"}]})
        if url.endswith("/tokens"):
            return Resp(200, {"data": [
                {"token_id": "1", "outcome_index": 0, "outcome": "Up",
                 "resolved": True, "final_price": 1},
                {"token_id": "2", "outcome_index": 1, "outcome": "Down",
                 "resolved": True, "final_price": 0},
            ]})
        return Resp(404)
    return handler


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_resolved(monkeypatch):
    client = Client(resolved_handler())
    monkeypatch.setattr(resolution.httpx, "AsyncClient", lambda **kw: client)
    out = run(resolution.get_btc5m_resolution(WINDOW))
    assert out["state"] == "resolved" and out["winner"] == "Up"
    assert out["condition_id"] == "0x" + "ab" * 32
    assert out["gamma_resolution_status"] == "resolved"
    assert out["data_api_resolution"]["status"] == "resolved"
    assert [o["outcome"] for o in out["outcomes"]] == ["Up", "Down"]


def test_pending(monkeypatch):
    def handler(url, params):
        if "gamma-api" in url:
            return Resp(200, event())
        if url.endswith("/resolutions"):
            return Resp(200, {"data": []})
        return Resp(200, {"data": [
            {"token_id": "1", "outcome_index": 0, "outcome": "Up",
             "resolved": False, "final_price": None},
        ]})
    client = Client(handler)
    monkeypatch.setattr(resolution.httpx, "AsyncClient", lambda **kw: client)
    out = run(resolution.get_btc5m_resolution(WINDOW))
    assert out["state"] == "pending" and out["winner"] is None


def test_window_not_ended_skips_http(monkeypatch):
    client = Client(resolved_handler())
    monkeypatch.setattr(resolution.httpx, "AsyncClient", lambda **kw: client)
    current = int(time.time()) // 300 * 300
    out = run(resolution.get_btc5m_resolution(current))
    assert out["state"] == "window_not_ended"
    assert out["seconds_remaining"] > 0
    assert client.calls == []


def test_market_not_found(monkeypatch):
    client = Client(lambda url, params: Resp(404))
    monkeypatch.setattr(resolution.httpx, "AsyncClient", lambda **kw: client)
    out = run(resolution.get_btc5m_resolution(WINDOW))
    assert out["state"] == "market_not_found"


def test_429_then_success(monkeypatch):
    state = {"gamma": 0}

    def handler(url, params):
        if "gamma-api" in url:
            state["gamma"] += 1
            if state["gamma"] == 1:
                return Resp(429, headers={"Retry-After": "0"})
            return Resp(200, event())
        if url.endswith("/resolutions"):
            return Resp(200, {"data": [{"condition_id": params.get("condition"),
                                        "status": "resolved"}]})
        return Resp(200, {"data": [
            {"token_id": "1", "outcome_index": 0, "outcome": "Up",
             "resolved": True, "final_price": 1},
        ]})
    client = Client(handler)
    monkeypatch.setattr(resolution.httpx, "AsyncClient", lambda **kw: client)
    out = run(resolution.get_btc5m_resolution(WINDOW))
    assert out["state"] == "resolved" and state["gamma"] == 2


def test_invalid_and_clamped_inputs():
    assert run(resolution.get_btc5m_resolution(WINDOW + 1))["error"] == "invalid_window_start"
    assert run(resolution.get_btc5m_resolution(WINDOW, "x"))["error"] == "invalid_wait_seconds"
    out = run(resolution.get_recent_btc5m_resolutions(13))
    assert isinstance(out, list) and len(out) == 12


def test_list_tools_exposes_both():
    from polymarket_mcp.server import mcp
    names = [t.name for t in asyncio.new_event_loop().run_until_complete(mcp.list_tools())]
    assert "get_btc5m_resolution" in names
    assert "get_recent_btc5m_resolutions" in names
