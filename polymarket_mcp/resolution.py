"""BTC 5m official settlement reads from Gamma and Data API v2 only.

# RESMI: https://docs.polymarket.com/api-reference/data-api/overview
# RESMI: https://docs.polymarket.com/market-data/public-analytics.md
# RESMI: https://docs.polymarket.com/migrate/polymarket-v2/api-integrations
# RESMI: https://docs.polymarket.com/api-spec/gamma-openapi.yaml
# RESMI: https://data-api.polymarket.com/v2/openapi.json
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

try:
    # RESMI: https://docs.polymarket.com/getting-started/python.md
    from mcp.server.mcpserver import MCPServer  # type: ignore
except ImportError:
    # RESMI: installed mcp 1.x exposes FastMCP; 2.x exposes MCPServer.
    from mcp.server.fastmcp import FastMCP as MCPServer  # type: ignore  # noqa: F401

# RESMI: https://docs.polymarket.com/api-reference/data-api/overview
DATA_API_BASE = "https://data-api.polymarket.com/v2"
# RESMI: https://docs.polymarket.com/api-spec/gamma-openapi.yaml
GAMMA_BASE = "https://gamma-api.polymarket.com"
HTTP_TIMEOUT_S = 10.0
MAX_ATTEMPTS = 3
MAX_CONCURRENCY = 3
SLUG_PREFIX = "btc-updown-5m-"
# EMPIRIS: slug prefix and 300s cadence observed on sequential public BTC markets;
# not stated in the official docs.


class ResolutionError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def window_bounds(window_start: int | None) -> tuple[int, int, str]:
    if window_start is None:
        window_start = ((int(time.time()) // 300) - 1) * 300
    if type(window_start) is not int or window_start % 300 != 0:
        raise ResolutionError("invalid_window_start", "window_start must be a multiple of 300")
    return window_start, window_start + 300, f"{SLUG_PREFIX}{window_start}"


async def _get_json(client: httpx.AsyncClient, url: str,
                    params: dict[str, str]) -> Any:
    for attempt in range(MAX_ATTEMPTS):
        response = await client.get(url, params=params)
        if response.status_code == 429 and attempt + 1 < MAX_ATTEMPTS:
            raw = response.headers.get("Retry-After", "1")
            try:
                delay = max(0.0, float(raw))
            except ValueError:
                delay = 1.0
            if delay:
                await asyncio.sleep(min(delay, HTTP_TIMEOUT_S))
            continue
        # RESMI: documented miss is data: null or [] (Data API v2 overview).
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise ResolutionError(f"http_{response.status_code}",
                                  f"request failed with HTTP {response.status_code}")
        try:
            return response.json()
        except ValueError:
            raise ResolutionError("invalid_json", "response was not valid JSON") from None
    raise ResolutionError("rate_limited", "429 persisted after 3 attempts")


async def _fetch_one(client: httpx.AsyncClient, window_start: int | None) -> dict[str, Any]:
    start, end, slug = window_bounds(window_start)
    now = int(time.time())
    base: dict[str, Any] = {
        "slug": slug, "window_start": start, "window_end": end,
        "state": "pending", "winner": None, "condition_id": None,
        "market_version": None, "gamma_resolution_status": None,
        "resolution_source": None, "data_api_resolution": None,
        "outcomes": [],
    }
    if now < end:
        base.update(state="window_not_ended", seconds_remaining=end - now)
        return base

    # RESMI: GET /events/slug/{slug} returns one Event object; 404 = miss.
    event = await _get_json(client, f"{GAMMA_BASE}/events/slug/{slug}", {})
    if not isinstance(event, dict):
        base["state"] = "market_not_found"
        return base
    markets = event.get("markets") or []
    if not markets:
        base["state"] = "market_not_found"
        return base
    market = markets[0]
    condition = market.get("conditionId")
    version = market.get("version")
    base["condition_id"] = condition
    base["market_version"] = version
    base["resolution_source"] = event.get("resolutionSource") or market.get("resolutionSource")
    # RESMI: V2 migration doc defines resolutionStatus for v2; gamma OpenAPI
    # lists umaResolutionStatus for v1.
    base["gamma_resolution_status"] = (market.get("resolutionStatus") if version == "v2"
                                        else market.get("umaResolutionStatus"))
    if not condition:
        base["state"] = "market_not_found"
        return base

    # RESMI: /v2/resolutions?condition=<conditionId>
    resolutions = await _get_json(client, f"{DATA_API_BASE}/resolutions",
                                  {"condition": str(condition)})
    rows = (resolutions or {}).get("data") or []
    base["data_api_resolution"] = rows[0] if rows else None

    # RESMI: /v2/tokens?condition=<conditionId>
    tokens = await _get_json(client, f"{DATA_API_BASE}/tokens",
                             {"condition": str(condition)})
    token_rows = (tokens or {}).get("data") or []
    labels: list[str] = []
    raw_outcomes = market.get("outcomes")
    if isinstance(raw_outcomes, str):
        try:
            parsed = json.loads(raw_outcomes)
            if isinstance(parsed, list):
                labels = [str(x) for x in parsed]
        except ValueError:
            labels = []
    for row in token_rows:
        index = row.get("outcome_index")
        label = row.get("outcome")
        if labels and type(index) is int and 0 <= index < len(labels):
            label = labels[index]
        base["outcomes"].append({
            "outcome": label,
            "resolved": bool(row.get("resolved")),
            "final_price": row.get("final_price"),
            "token_id": row.get("token_id"),
            "outcome_index": index,
        })
        # RESMI: winner = resolved==true and final_price == 1.
        # EMPIRIS: toleransi 0,999 untuk nilai float yang dibulatkan.
        if row.get("resolved") is True and (row.get("final_price") or 0) >= 0.999:
            base["winner"] = label if label in ("Up", "Down") else None
    settled = bool(base["outcomes"]) and all(o["resolved"] for o in base["outcomes"])
    base["state"] = "resolved" if settled else "pending"
    return base


async def get_btc5m_resolution(window_start: int | None = None,
                               wait_seconds: int = 0) -> dict[str, Any]:
    if type(wait_seconds) is not int:
        return {"state": "error", "error": "invalid_wait_seconds",
                "message": "wait_seconds must be an integer"}
    wait_seconds = max(0, min(60, wait_seconds))
    try:
        start, _end, _slug = window_bounds(window_start)
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
            started = time.monotonic()
            result = await _fetch_one(client, start)
            while result["state"] == "pending" and \
                    time.monotonic() - started < wait_seconds:
                # RESMI: settlement data can take about a minute to update.
                await asyncio.sleep(5)
                result = await _fetch_one(client, start)
            return result
    except ResolutionError as exc:
        return {"state": "error", "error": exc.code, "message": exc.message}
    except (httpx.HTTPError, OSError, ValueError, TypeError) as exc:
        return {"state": "error", "error": "request_failed",
                "message": f"{type(exc).__name__}: {exc}"}


async def get_recent_btc5m_resolutions(count: int = 6) -> list[dict[str, Any]]:
    if type(count) is not int:
        return [{"state": "error", "error": "invalid_count",
                 "message": "count must be an integer"}]
    count = max(1, min(12, count))
    last = ((int(time.time()) // 300) - 1) * 300
    starts = [last - i * 300 for i in range(count)]
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
        async def limited(value: int) -> dict[str, Any]:
            async with semaphore:
                try:
                    return await _fetch_one(client, value)
                except ResolutionError as exc:
                    return {"state": "error", "error": exc.code,
                            "message": exc.message}
                except (httpx.HTTPError, OSError, ValueError, TypeError) as exc:
                    return {"state": "error", "error": "request_failed",
                            "message": f"{type(exc).__name__}: {exc}"}

        return list(await asyncio.gather(*(limited(s) for s in starts)))
