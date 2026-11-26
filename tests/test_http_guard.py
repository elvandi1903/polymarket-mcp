import asyncio

import httpx
import pytest

from polymarket_mcp import server


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.0.2.10", "example.test"])
def test_nonloopback_without_token_never_starts(monkeypatch, host):
    monkeypatch.delenv("POLY_HTTP_TOKEN", raising=False)
    monkeypatch.setattr(server, "_configured_http_token", "", raising=False)

    def must_not_run(_transport):
        pytest.fail("HTTP transport started without a token")

    monkeypatch.setattr(server, "_transport_runner", must_not_run)
    with pytest.raises(SystemExit, match="POLY_HTTP_TOKEN"):
        server.main(["--transport", "streamable-http", "--host", host])


def test_wildcard_bind_requires_explicit_host_list(monkeypatch):
    monkeypatch.delenv("POLY_HTTP_ALLOWED_HOSTS", raising=False)
    with pytest.raises(ValueError, match="POLY_HTTP_ALLOWED_HOSTS"):
        server._transport_security("0.0.0.0", port=8000)


def test_http_token_host_and_origin_are_enforced(monkeypatch):
    token = "synthetic-test-token"
    monkeypatch.setenv("POLY_HTTP_TOKEN", token)
    monkeypatch.setenv("POLY_HTTP_ALLOWED_HOSTS", "mcp.example.test")
    monkeypatch.setenv("POLY_HTTP_ALLOWED_ORIGINS", "https://client.example.test")

    async def check():
        mcp = server._make_server(host="0.0.0.0")
        app = mcp.streamable_http_app(
            stateless_http=True, transport_security=server._transport_security("0.0.0.0", port=8000),
        )
        async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://mcp.example.test",
        ) as client:
            headers = {"Accept": "application/json, text/event-stream"}
            init = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "offline-test", "version": "1"},
            }}
            for extra, expected in [
                ({}, 401), ({"Authorization": "Bearer wrong"}, 401),
                ({"Authorization": f"Bearer {token}", "Host": "other.example.test"}, 421),
                ({"Authorization": f"Bearer {token}", "Origin": "https://other.example.test"}, 403),
                ({"Authorization": f"Bearer {token}", "Origin": "https://client.example.test"}, 200),
            ]:
                response = await client.post("/mcp", headers=headers | extra, json=init)
                assert response.status_code == expected
                assert token not in response.text

    asyncio.run(check())
