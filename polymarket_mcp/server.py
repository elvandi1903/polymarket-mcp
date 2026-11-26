"""MCP server for Polymarket trading (read tools + gated order tools).

Transports (MCPServer 2.x with FastMCP 1.x fallback):
* stdio (default) - works with Hermes' ``mcp_servers`` stdio entries and
  any MCP client::

      polymarket-mcp --transport stdio
* Streamable HTTP - serves as a web service (e.g. in InstaBox with port
  forwarding). Host/port are constructor args on FastMCP; they are not
  environment-driven by the SDK, so this module parses CLI flags::

      polymarket-mcp --transport http --host 127.0.0.1 --port 8000

This file is standalone: it does NOT modify ~/.hermes/config.yaml;
registering it is a separate, explicit step the user performs after
reviewing this code.

Read tools run on an unauthenticated AsyncPublicClient (no keys needed).
Order tools run through the secure client, gated by (POLY_ALLOW_LIVE=1 AND
confirm=true), and only after the Proxy-Wallet preflight passes.
"""
from __future__ import annotations

import argparse
import asyncio
import decimal
import hmac
import ipaddress
import math
import os
import time
from typing import Any
from urllib.parse import urlsplit

try:
    # RESMI: mcp >= 2.x
    from mcp.server.mcpserver import MCPServer as _Server
    _MCP_V2 = True
except ImportError:
    # RESMI: mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server
    _MCP_V2 = False

from . import client as clientmod
from . import observability, safety
from .config import Settings

INSTRUCTIONS = (
    "Polymarket trading tools. Order tools are DRY-RUN by default: they "
    "validate and preview only. Live BUY is disabled; use preview_buy_depth "
    "to estimate up to 5 shares at $0.72 per share or less, before fees. "
    "Market SELL uses shares and requires confirm=true AND POLY_ALLOW_LIVE=1 "
    "to submit. The account is a legacy Proxy Wallet; the server refuses "
    "authenticated orders unless the wallet classifies as POLY_PROXY "
    "(signature_type=1). Show the user the DRY-RUN preview before requesting "
    "approval. Limit orders are not supported."
)

class _StaticBearerVerifier:
    """Bearer-token verifier for the HTTP transport.

    Checks the client-supplied token against ``POLY_HTTP_TOKEN`` (env).
    The token itself is never logged. ``AccessToken`` is required; the SDK's
    ``BearerAuthBackend`` wraps it into an ``AuthContext``. Scope is empty
    (single-tenant deployment); client_id is fixed to ``polymarket-mcp``.
    """

    def __init__(self, expected_token: str) -> None:
        self._expected = expected_token

    async def verify_token(self, token: str):
        from mcp.server.auth.provider import AccessToken

        if not self._expected or not hmac.compare_digest(token.encode(), self._expected.encode()):
            return None
        return AccessToken(
            token=token,
            client_id="polymarket-mcp",
            scopes=[],
        )


def _host_ip(host: str):
    try:
        return ipaddress.ip_address(host.removeprefix("[").removesuffix("]"))
    except ValueError:
        return None


def _require_http_token(host: str, token: str) -> None:
    address = _host_ip(host)
    if host != "localhost" and not (address and address.is_loopback) and not token:
        raise ValueError("POLY_HTTP_TOKEN is required for non-loopback HTTP/SSE binds")


def _transport_security(host: str, port: int = 8000):
    from mcp.server.transport_security import TransportSecuritySettings

    hosts = [entry.strip().lower() for entry in os.getenv("POLY_HTTP_ALLOWED_HOSTS", "").split(",") if entry.strip()]
    address = _host_ip(host)
    if address and address.is_unspecified:
        if not hosts:
            raise ValueError("POLY_HTTP_ALLOWED_HOSTS is required for wildcard binds")
    else:
        bind_host = f"[{address}]" if address and address.version == 6 else host.lower()
        hosts.extend((bind_host, f"{bind_host}:{port}"))
    if any(any(c in value for c in "*/?#@\\ ") for value in hosts):
        raise ValueError("POLY_HTTP_ALLOWED_HOSTS must contain exact Host values")

    origins = [entry.strip() for entry in os.getenv("POLY_HTTP_ALLOWED_ORIGINS", "").split(",") if entry.strip()]
    for origin in origins:
        parsed = urlsplit(origin)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username is not None
                or parsed.path or parsed.query or parsed.fragment or any(c in origin for c in "*\\ ?#")):
            raise ValueError("POLY_HTTP_ALLOWED_ORIGINS must contain exact HTTP(S) origins")
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=origins,
    )


def _make_server(host: str = "127.0.0.1", port: int = 8000) -> Any:
    token = os.getenv("POLY_HTTP_TOKEN", "").strip()
    _require_http_token(host, token)
    kwargs: dict[str, Any] = {"instructions": INSTRUCTIONS}
    if token:
        from mcp.server.auth.settings import AuthSettings

        kwargs["token_verifier"] = _StaticBearerVerifier(token)
        kwargs["auth"] = AuthSettings(
            issuer_url="https://polymarket.local",
            resource_server_url="https://polymarket.local/mcp",
            required_scopes=[],
            **({"validate_token_resource": False} if _MCP_V2 else {}),
        )
    if not _MCP_V2:
        kwargs.update(host=host, port=port, transport_security=_transport_security(host, port))
    return _Server("polymarket-trading", **kwargs)


_configured_http_token = os.getenv("POLY_HTTP_TOKEN", "").strip()
mcp = _make_server()

_AUTONOMOUS_SAFE_TOOLS = frozenset({
    "search_markets", "get_market", "get_order_book", "get_midpoint", "get_price",
    "get_last_trade_price", "estimate_market_price", "preview_buy_depth", "preview_order",
    "place_market_order", "place_batch_order", "guard_status", "get_btc5m_snapshot",
    "get_btc5m_resolution", "get_recent_btc5m_resolutions", "get_ptb", "ptb_status",
    "wait_for_ptb", "list_ptb", "subscribe_book", "get_live_book", "get_live_top_of_book",
    "subscribe_round", "round_status", "stream_health", "market_data_status",
    "market_data_snapshot", "market_data_last_points", "start_market_data", "start_ptb_feed",
})


_AUTONOMOUS_AT_START = os.getenv("POLY_AUTONOMOUS", "0").strip() != "0"


def _autonomous() -> bool:
    return _AUTONOMOUS_AT_START or os.getenv("POLY_AUTONOMOUS", "0").strip() != "0"


def _autonomous_tool_allowed(name: str) -> bool:
    configured = os.getenv("POLY_AUTONOMOUS_TOOLS")
    return name in _AUTONOMOUS_SAFE_TOOLS and (
        configured is None or name in {item.strip() for item in configured.split(",")}
    )


def _tool(func):
    if not _autonomous() or _autonomous_tool_allowed(func.__name__):
        mcp.tool()(func)
    return func


def _settings() -> Settings:
    """Read env fresh per call so tests/in-process callers can set vars."""
    return Settings.from_env()


def _to_jsonable(obj: Any) -> Any:
    """Convert an SDK pydantic/dataclass object to a JSON-safe structure."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    model_dump = getattr(obj, "model_dump", None)
    if callable(model_dump):
        try:
            return _to_jsonable(model_dump(mode="json"))
        except TypeError:
            return _to_jsonable(model_dump())
    try:
        import dataclasses

        if dataclasses.is_dataclass(obj):
            return _to_jsonable(dataclasses.asdict(obj))
    except Exception:
        pass
    return str(obj)


def _resolve_asset_id(market: Any, outcome: str) -> tuple[str, str]:
    """Return (asset_id, method) choosing position_id for v2 else token_id.

    Matches the official docs: asset_id = position_id for a v2 market,
    token_id for a CTF (v1) market. The field used is reported so a silent
    wrong-id trade is visible.
    """
    version = getattr(market, "version", None)
    version_str = getattr(version, "value", version)
    outcomes = getattr(market, "outcomes", None)
    key = outcome.lower()
    attr = "yes" if key in ("up", "yes") else "no" if key in ("down", "no") else key
    side_obj = getattr(outcomes, attr, None) if outcomes else None
    if side_obj is not None:
        if str(version_str) == "v2":
            pid = getattr(side_obj, "position_id", None)
            if pid is not None:
                return str(pid), "position_id (v2)"
        tid = getattr(side_obj, "token_id", None)
        if tid is not None:
            return str(tid), "token_id (CTF)"
    position_ids = getattr(market, "position_ids", None)
    if position_ids:
        key = outcome.lower()
        if key in ("up", "yes"):
            idx = 0
        elif key in ("down", "no"):
            idx = 1
        else:
            raise safety.ValidationError(f"Could not resolve asset id for outcome={outcome!r} on this market")
        if idx < len(position_ids):
            return str(position_ids[idx]), "position_ids[idx] (fallback)"
    raise safety.ValidationError(
        f"Could not resolve asset id for outcome={outcome!r} on this market"
    )


@_tool
async def search_markets(q: str, page_size: int = 10) -> dict:
    """Search Polymarket for markets/events. Read-only, no key required."""
    pub = clientmod.build_public_client()
    try:
        page = await pub.search(q=q, page_size=page_size).first_page()
        from polymarket_mcp.server import _to_jsonable

        return {"results": _to_jsonable(page.items if hasattr(page, "items") else page)}
    finally:
        await pub.close()


@_tool
async def get_market(slug: str = "", market_id: str = "", url: str = "") -> dict:
    """Fetch one market by slug, id, or URL and return its trading constraints."""
    pub = clientmod.build_public_client()
    try:
        from polymarket_mcp.server import _to_jsonable

        market = await pub.get_market(slug=slug or None, id=market_id or None, url=url or None)
        return _to_jsonable(market)
    finally:
        await pub.close()


@_tool
async def get_order_book(asset_id: str) -> dict:
    """Order book for one outcome asset id (token/position id). Read-only."""
    pub = clientmod.build_public_client()
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await pub.get_order_book(asset_id=asset_id))
    finally:
        await pub.close()


@_tool
async def get_midpoint(asset_id: str) -> dict:
    pub = clientmod.build_public_client()
    try:
        return {"asset_id": asset_id, "midpoint": str(await pub.get_midpoint(asset_id=asset_id))}
    finally:
        await pub.close()


@_tool
async def get_price(asset_id: str, side: str = "BUY") -> dict:
    pub = clientmod.build_public_client()
    try:
        return {"asset_id": asset_id, "side": side.upper(),
                "price": str(await pub.get_price(asset_id=asset_id, side=safety.validate_side(side)))}
    finally:
        await pub.close()


@_tool
async def get_last_trade_price(asset_id: str) -> dict:
    pub = clientmod.build_public_client()
    try:
        from polymarket_mcp.server import _to_jsonable

        ltp = await pub.get_last_trade_price(asset_id=asset_id)
        return _to_jsonable(ltp) if ltp is not None else {"asset_id": asset_id, "last_trade_price": None}
    finally:
        await pub.close()


@_tool
async def estimate_market_price(asset_id: str, side: str, amount: str = "", shares: str = "") -> dict:
    """Estimate the execution price of a market order at current depth. Read-only."""
    s = safety.validate_side(side)
    kw: dict = {"asset_id": asset_id, "side": s}
    if amount:
        kw["amount"] = safety.to_decimal(amount, "amount")
    if shares:
        kw["shares"] = safety.to_decimal(shares, "shares")
    pub = clientmod.build_public_client()
    try:
        est = None
        if hasattr(pub, "estimate_market_price"):
            est = await pub.estimate_market_price(**kw)
        return {"asset_id": asset_id, "side": s,
                "estimated_price": str(est) if est is not None else None}
    finally:
        await pub.close()


@_tool
async def account_preflight() -> dict:
    """Show config summary + Proxy-Wallet classification. Moves no funds."""
    import time
    st = _settings()
    summary: dict = {"config": st.public_summary(), "timestamp": int(time.time())}
    try:
        info = clientmod.classify_wallet(st)
        info["proxy_ok"] = bool(info["is_proxy"])
        summary["classification"] = info
    except Exception as exc:
        summary["classification_error"] = f"{type(exc).__name__}: {exc}"
    return summary


@_tool
async def get_balance_allowance(asset_type: str = "COLLATERAL", asset_id: str = "") -> dict:
    """Balance + allowance for the trading account. Read-only; requires keys."""
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await client.get_balance_allowance(
            asset_type=asset_type, asset_id=asset_id or None))
    finally:
        await client.close()


@_tool
async def list_positions() -> dict:
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        items = []
        async for page in client.list_positions():
            items.extend(_to_jsonable(page.items if hasattr(page, "items") else page))
        return {"positions": items}
    finally:
        await client.close()


@_tool
async def list_open_orders(asset_id: str = "", market: str = "") -> dict:
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        items = []
        async for page in client.list_open_orders(asset_id=asset_id or None, market=market or None):
            items.extend(_to_jsonable(page.items if hasattr(page, "items") else page))
        return {"open_orders": items, "count": len(items)}
    finally:
        await client.close()


def _buy_ledger():
    from .round_guard import BuyLedger

    return BuyLedger(observability._state_dir() / "buy-ledger.sqlite3")


@_tool
async def reconcile_round(window_start: int) -> dict:
    """Read exact-round account evidence without releasing a claim or resubmitting."""
    from .resolution import window_bounds

    if type(window_start) is not int or window_start <= 0 or window_start % 300:
        raise safety.ValidationError("window_start must be a positive UTC epoch multiple of 300")
    start, _, slug = window_bounds(window_start)
    public = clientmod.build_public_client()
    try:
        market = await public.get_market(slug=slug)
    finally:
        await public.close()
    condition_id = getattr(market, "condition_id", None)
    if getattr(market, "slug", None) != slug or not condition_id:
        raise safety.ValidationError("Could not verify the round's condition ID")

    claim = _buy_ledger().claim(start)
    client = await clientmod.build_readonly_account_client(_settings())
    try:
        orders, positions = [], []
        async for page in client.list_open_orders(market=condition_id):
            orders.extend(_to_jsonable(row) for row in page.items if row.condition_id == condition_id)
        async for page in client.list_positions(condition_id=condition_id, full_history=True):
            positions.extend(_to_jsonable(row) for row in page.items if row.condition_id == condition_id)
    finally:
        await client.close()
    return {
        "window_start": start, "slug": slug, "claim_status": claim,
        "open_orders": orders, "positions": positions,
        "note": "Account evidence only. An empty result does not prove no fill; no claim or budget was changed.",
    }


@_tool
async def get_trading_approvals_state() -> dict:
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await client.get_trading_approvals_state())
    finally:
        await client.close()


async def _resolve_market(slug_or_id: str, outcome: str):
    """Fetch market (public), resolve asset_id + constraints. No signing."""
    from polymarket_mcp.server import _resolve_asset_id, _to_jsonable

    pub = clientmod.build_public_client()
    try:
        if slug_or_id.strip().isdigit():
            market = await pub.get_market(id=slug_or_id)
        else:
            market = await pub.get_market(slug=slug_or_id)
        asset_id, how = _resolve_asset_id(market, outcome)
        trading = getattr(market, "trading", None)
        constraints = _to_jsonable(trading) if trading is not None else None
        return market, asset_id, how, constraints
    finally:
        await pub.close()


@_tool
@observability.audit_tool
async def place_market_order(
    market: str,
    outcome: str,
    side: str,
    amount: str = "",
    shares: str = "",
    max_spend: str = "",
    max_price: str = "",
    min_price: str = "",
    order_type: str = "FAK",
    confirm: bool = False,
) -> dict:
    """Preview a market BUY or SELL; live BUY is disabled.

    BUY previews use amount (USD notional); SELL uses shares. Live SELL
    requires POLY_ALLOW_LIVE=1 and confirm=true. Order type is FAK or FOK.
    """
    from decimal import Decimal

    st = _settings()
    o = safety.validate_outcome(outcome)
    s = safety.validate_side(side)
    ot = (order_type or "FAK").strip().upper()
    if ot not in ("FAK", "FOK"):
        raise safety.ValidationError("order_type must be 'FAK' or 'FOK'")
    amt = safety.to_decimal(amount, "amount") if amount else None
    shr = safety.to_decimal(shares, "shares") if shares else None
    mspend = safety.to_decimal(max_spend, "max_spend") if max_spend else None
    mxp = safety.to_decimal(max_price, "max_price") if max_price else None
    mnp = safety.to_decimal(min_price, "min_price") if min_price else None
    if s == "BUY" and amt is None:
        raise safety.ValidationError("BUY market order requires amount (USD notional)")
    if s == "SELL" and shr is None:
        raise safety.ValidationError("SELL market order requires shares")
    gate = safety.gate_result(st, confirm and not _autonomous())
    if gate["live"] and s == "BUY":
        observability.decision("live_buy_blocked")
        safety.reject_live_buy()
    cap = amt if amt is not None else (mspend if mspend is not None else Decimal(0))
    safety.check_notional(st, cap)
    _mkt, asset_id, how, constraints = await _resolve_market(market, o)

    if gate["mode"] == "DRY_RUN":
        if constraints:
            constraints_full = {**constraints, "asset_id": asset_id, "asset_id_via": how}
        else:
            constraints_full = {"asset_id": asset_id, "asset_id_via": how}
        return safety.order_preview(
            market_slug_or_id=market, outcome=o, side=s, kind="MARKET",
            price=None, size=None, amount=amt, shares=shr, max_spend=mspend,
            max_price=mxp, min_price=mnp, order_type=ot, expiration=None,
            post_only=False, gate=gate, market_constraints=constraints_full)

    observability.decision("allowed")
    client = await clientmod.build_secure_client(st)
    try:
        from polymarket_mcp.server import _to_jsonable

        resp = await observability.submit(
            client.place_market_order, asset_id=asset_id, side=s, amount=amt, shares=shr,
            max_spend=mspend, max_price=mxp, min_price=mnp,
            order_type=ot, builder_code=st.builder_code or None)
        return _to_jsonable(resp)
    finally:
        await client.close()


@_tool
async def guard_status() -> dict:
    """Show recent guard decisions and local audit/heartbeat health."""
    return observability.state.status()


@_tool
@observability.audit_tool
async def cancel_order(order_id: str, confirm: bool = False) -> dict:
    """Cancel one order. DRY-RUN unless POLY_ALLOW_LIVE=1 AND confirm=true."""
    gate = safety.gate_result(_settings(), confirm)
    if gate["mode"] == "DRY_RUN":
        return {"status": "DRY_RUN", "action": "cancel_order", "order_id": order_id,
                "gate": gate, "note": "Nothing cancelled. POLY_ALLOW_LIVE=1 AND confirm=true required."}
    observability.decision("allowed")
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await observability.submit(client.cancel_order, order_id=order_id))
    finally:
        await client.close()


@_tool
@observability.audit_tool
async def cancel_all_orders(confirm: bool = False) -> dict:
    gate = safety.gate_result(_settings(), confirm)
    if gate["mode"] == "DRY_RUN":
        return {"status": "DRY_RUN", "action": "cancel_all", "gate": gate,
                "note": "Nothing cancelled. POLY_ALLOW_LIVE=1 AND confirm=true required."}
    observability.decision("allowed")
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await observability.submit(client.cancel_all))
    finally:
        await client.close()


@_tool
@observability.audit_tool
async def setup_trading_approvals(confirm: bool = False) -> dict:
    """Request pUSD + conditional-token exchange allowances. State-changing.

    NOTE: this does not include the Neg-Risk Adapter. Markets with
    state.neg_risk=true need a separate adapter approval; the tool surfaces
    approvals state so that gap stays visible instead of silently failing.
    """
    gate = safety.gate_result(_settings(), confirm)
    if gate["mode"] == "DRY_RUN":
        return {"status": "DRY_RUN", "action": "setup_trading_approvals", "gate": gate,
                "note": "DRY-RUN; nothing submitted. Neg-Risk Adapter is NOT covered by "
                        "setup_trading_approvals; check neg_risk per market and approvals state."}
    observability.decision("allowed")
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        handle = await observability.submit(client.setup_trading_approvals)
        return {"status": "submitted", "handle": _to_jsonable(handle)}
    finally:
        await client.close()


# -------------------------------------------------------------- LIVE BOOK --
@_tool
async def subscribe_book(asset_id: str, slug: str = "") -> dict:
    """Start (or reuse) a live WS subscription to an outcome asset id."""
    from polymarket_mcp.live import get_manager

    m = get_manager()
    st = await m.subscribe(asset_id, slug=slug)
    return {"subscription": st.snapshot_dict(), "ok": True}


@_tool
async def unsubscribe_book(asset_id: str) -> dict:
    from polymarket_mcp.live import get_manager

    m = get_manager()
    ok = await m.unsubscribe(asset_id)
    return {"asset_id": asset_id, "ok": bool(ok)}


@_tool
async def get_live_book(asset_id: str) -> dict:
    """Current live book + freshness. If no live stream exists, resync by REST."""
    from polymarket_mcp.live import get_manager

    m = get_manager()
    st = m.get_state(asset_id)
    if st is None:
        # cold start: subscribe (which will also deliver a snapshot), then
        # if we still have no book, fall back to a REST snapshot.
        st = await m.subscribe(asset_id)
        snap = st.snapshot_dict()
        if snap.get("book_snapshot") is None:
            await m._rest_fallback(st)
            snap = st.snapshot_dict()
            snap["availability"] = "resync"
        return snap
    return st.snapshot_dict()


@_tool
async def get_live_top_of_book(asset_id: str) -> dict:
    """Return a compact, timestamped top-of-book snapshot for an asset."""
    from datetime import datetime

    from polymarket_mcp.live import get_manager

    manager = get_manager()
    st = manager.get_state(asset_id)
    if st is None:
        st = await manager.subscribe(asset_id)
    snap = st.snapshot_dict()
    book = snap.get("book_snapshot")
    if book is None or snap.get("tick_size") is None or not book.get("asks"):
        await manager._rest_fallback(st)
        snap = st.snapshot_dict()
    book = snap.get("book_snapshot") or {}
    bbo = snap.get("best_bid_ask") or {}
    def epoch_ms(value):
        if isinstance(value, datetime) and value.tzinfo is not None:
            return int(value.timestamp() * 1000)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return int(value)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
                if parsed.tzinfo is not None:
                    return int(parsed.timestamp() * 1000)
            except ValueError:
                pass
        return None

    # The book projection is updated by price_change deltas; a BBO event is
    # only authoritative when it is at least as new as that projection.
    book_timestamp_ms = epoch_ms(book.get("timestamp"))
    bbo_timestamp_ms = epoch_ms(bbo.get("timestamp"))
    use_bbo = (book_timestamp_ms is not None and bbo_timestamp_ms is not None
               and bbo_timestamp_ms >= book_timestamp_ms)
    bid, ask = (bbo.get("best_bid"), bbo.get("best_ask")) if use_bbo else (None, None)
    timestamp = bbo.get("timestamp") if use_bbo else None
    if bid is None or ask is None:
        bids, asks = book.get("bids") or [], book.get("asks") or []
        try:
            bid = max((decimal.Decimal(str(row["price"])) for row in bids), default=None)
            ask = min((decimal.Decimal(str(row["price"])) for row in asks), default=None)
            bid = str(bid) if bid is not None else None
            ask = str(ask) if ask is not None else None
        except (KeyError, TypeError, decimal.InvalidOperation, ValueError):
            bid = ask = None
        timestamp = book.get("timestamp")

    timestamp_ms = epoch_ms(timestamp)
    hash_timestamp_ms = book_timestamp_ms
    if not book:
        bid = ask = None
        timestamp_ms = None
    return {
        "asset_id": asset_id,
        "slug": snap.get("slug"),
        "availability": snap.get("availability"),
        "age_ms": snap.get("book_age_ms", snap.get("age_ms")),
        "received_at_ms": snap.get("book_received_at_ms"),
        "timestamp_ms": timestamp_ms,
        "bid": bid,
        "ask": ask,
        "book_hash": book.get("hash"),
        "hash_timestamp_ms": hash_timestamp_ms,
        "tick_size": snap.get("tick_size"),
        "source": snap.get("book_source"),
        "events_seen": snap.get("events_seen"),
        "reconnections": snap.get("reconnections"),
        "out_of_order_drops": snap.get("out_of_order_drops"),
    }


@_tool
async def stream_status() -> dict:
    from polymarket_mcp.live import get_manager

    return get_manager().status_snapshot()


@_tool
async def stream_events(asset_id: str, limit: int = 50) -> dict:
    from polymarket_mcp.live import get_manager

    return {"asset_id": asset_id, "events": get_manager().events_snapshot(asset_id, limit)}


@_tool
async def stream_health() -> dict:
    from polymarket_mcp.live import get_manager

    snap = get_manager().status_snapshot()
    total_reconnects = sum(
        a.get("reconnections", 0) for a in snap["assets"].values()
    )
    total_dropped = sum(a.get("dropped_events", 0) for a in snap["assets"].values())
    stale_count = sum(1 for a in snap["assets"].values() if a.get("stale"))
    return {
        "count": snap["count"],
        "total_reconnections": total_reconnects,
        "total_dropped_events": total_dropped,
        "stale_assets": stale_count,
        "timestamp_ms": snap["timestamp_ms"],
    }


@_tool
async def watch_asset(slug: str, outcome: str = "UP") -> dict:
    """Subscribe to a market by slug and return the freshest live book.

    Useful right before a BTC 5m boundary: it resolves the asset_id from the
    slug, subscribes (or reuses) and returns a live snapshot.
    """
    from polymarket_mcp.live import get_manager

    m = get_manager()
    pub = clientmod.build_public_client()
    try:
        market = await pub.get_market(slug=slug)
        asset_id, _how = _resolve_asset_id(market, safety.validate_outcome(outcome))
    finally:
        await pub.close()
    st = await m.subscribe(asset_id, slug=slug)
    snap = st.snapshot_dict()
    if snap.get("book_snapshot") is None:

        await m._rest_fallback(st)
        snap = st.snapshot_dict()
        snap["availability"] = "resync"
        snap["slug"] = slug
    return snap


# ---------------------------------------------------------- CREDENTIALS ---
@_tool
async def derive_api_credentials(nonce: int = 0) -> dict:
    """Derive-or-create CLOB L2 API credentials for the Proxy Wallet account.

    Runs the SDK's own create(POST)/derive(GET on 400) path; preflight
    (POLY_PROXY) gates it. Nonces: 0 unless you manage multiple key sets.
    Returns metadata only — never the secret/passphrase.
    """
    import polymarket_mcp.credentials as creds
    st = _settings()
    if nonce:
        st = Settings(**{**st.__dict__, "nonce": nonce})
    return await creds.derive_credentials(st)


@_tool
async def validate_api_credentials() -> dict:
    """Validate that the configured CLOB credentials are still active.

    Performs a real L2-signed GET /auth/api-keys probe (docs: get order list
    authentication). Returns active/invalid; 401 = invalid, other errors
    surface their endpoint status.
    """

    import polymarket_mcp.credentials as creds
    st = _settings()
    provided = clientmod.build_credentials(st)
    if provided is None:
        saved_path = creds.DEFAULT_PATH
        if saved_path.exists():
            provided = creds.load_credentials(saved_path)
        else:
            return {"status": "validate_failed", "error": "No credentials configured (set POLY_API_* or save a credential file)",
                    "classification": clientmod.classify_wallet(st) if st.private_key and st.wallet else "skipped"}
    return await creds.validate_credentials(st, provided)


@_tool
async def test_connection() -> dict:
    """End-to-end connectivity test (preflight -> client -> balance read).

    Read-only; no orders, no state changes. Safe to run repeatedly.
    """
    import polymarket_mcp.credentials as creds

    return await creds.test_connection(_settings())


@_tool
async def list_saved_credentials() -> dict:
    """Non-secret summary of the persisted credential file (if any)."""
    import polymarket_mcp.credentials as creds

    return creds.saved_summary()


@_tool
async def save_api_credentials(api_key: str, secret: str, passphrase: str) -> dict:
    """Persist supplied credentials to the 0600 file (atomic write).

    The values are written only to disk; the response contains no secret.
    Prefer populating POLY_API_* via env for ephemeral runs.
    """
    from polymarket.models.clob.api_key import ApiKeyCreds

    import polymarket_mcp.credentials as creds

    return creds.save_credentials(ApiKeyCreds(apiKey=api_key, secret=secret, passphrase=passphrase))


@_tool
async def revoke_api_credentials(rotate: bool = False, new_nonce: int = 0) -> dict:
    """Delete the API key currently in use (DELETE /auth/api-key).

    rotate=True derives a fresh credential first (new_nonce required) so the
    account is never left keyless. Otherwise all CLOB auth for this nonce
    stops immediately. State-changing: confirm your intent before calling.
    """
    import polymarket_mcp.credentials as creds

    st = _settings()
    provided = clientmod.build_credentials(st)
    if provided is None and creds.DEFAULT_PATH.exists():
        provided = creds.load_credentials(creds.DEFAULT_PATH)
    if provided is None:
        return {"status": "revoke_failed", "error": "No credentials configured"}
    return await creds.revoke_credentials(st, provided, rotate=rotate, new_nonce=new_nonce or None)


# -------------------------------------------------------------- ORDER OPS --
@_tool
@observability.audit_tool
async def preview_buy_depth(market: str, outcome: str) -> dict:
    """Estimate up to 5 shares at $0.72/share or less from public asks. No order."""
    o = safety.validate_outcome(outcome)
    _, asset_id, _, _ = await _resolve_market(market, o)
    pub = clientmod.build_public_client()
    try:
        book = await pub.get_order_book(asset_id=asset_id)
    finally:
        await pub.close()
    if book.asset_id != asset_id:
        raise safety.ValidationError("Order book asset does not match the requested outcome")
    if any(
        not level.price.is_finite() or not level.size.is_finite()
        or not 0 < level.price < 1 or level.size <= 0
        for level in book.asks
    ):
        raise safety.ValidationError("Order book contains an invalid ask price or size")

    remaining = safety.MAX_BUY_SHARES
    notional = decimal.Decimal(0)
    worst_price = None
    for level in sorted(book.asks, key=lambda level: level.price):
        if remaining == 0 or level.price > safety.MAX_BUY_PRICE:
            break
        quantity = min(remaining, level.size)
        notional += quantity * level.price
        remaining -= quantity
        worst_price = level.price

    return {
        "status": "DRY_RUN",
        "market_ref": market,
        "outcome": o,
        "asset_id": asset_id,
        "book_timestamp": book.timestamp.isoformat() if book.timestamp is not None else None,
        "max_shares": str(safety.MAX_BUY_SHARES),
        "max_price": str(safety.MAX_BUY_PRICE),
        "max_spend_including_fees": str(safety.MAX_BUY_SPEND),
        "estimated_shares": str(safety.MAX_BUY_SHARES - remaining),
        "estimated_notional_before_fees": str(notional),
        "worst_price": str(worst_price) if worst_price is not None else None,
        "all_in_spend_verified": False,
        "live_buy_allowed": False,
        "note": "Book-depth estimate only; does not verify fees or order eligibility and is not an execution quote.",
    }


@_tool
@observability.audit_tool
async def preview_order(
    market: str,
    outcome: str,
    side: str,
    amount: str = "",
    shares: str = "",
    max_spend: str = "",
    max_price: str = "",
    min_price: str = "",
) -> dict:
    """Full DRY-RUN preview of a MARKET order only with preflight.

    Read-only: resolves market constraints (accepting_orders, neg_risk, tick,
    min size, fee schedule), runs the order preflight checklist, and returns
    the step-by-step plan plus the failure-matrix — without signing or posting.
    Limit orders are intentionally not supported in this deployment (market
    orders only, per operator).
    """
    from polymarket_mcp import orders

    st = _settings()
    o = safety.validate_outcome(outcome)
    s = safety.validate_side(side)
    _mkt, asset_id, how, constraints = await _resolve_market(market, o)
    if constraints:
        constraints_full = {**constraints, "asset_id": asset_id, "asset_id_via": how}
    else:
        constraints_full = {"asset_id": asset_id, "asset_id_via": how}
    pre = orders.preflight_for_order(st, market_constraints=constraints_full, side=s, outcome=o)
    preview = safety.order_preview(
        market_slug_or_id=market, outcome=o, side=s, kind="MARKET",
        price=None,
        size=None,
        amount=safety.to_decimal(amount, "amount") if amount else None,
        shares=safety.to_decimal(shares, "shares") if shares else None,
        max_spend=safety.to_decimal(max_spend, "max_spend") if max_spend else None,
        max_price=safety.to_decimal(max_price, "max_price") if max_price else None,
        min_price=safety.to_decimal(min_price, "min_price") if min_price else None,
        order_type="FAK",
        expiration=None, post_only=False,
        gate=safety.gate_result(st, confirm=False),
        market_constraints=constraints_full,
    )
    preview["preflight"] = pre
    preview["failure_matrix"] = orders.FAILURE_MATRIX
    preview["note"] = (
        "Preview is informational. Two-factor gate still applies to placing: "
        "confirm=true AND POLY_ALLOW_LIVE=1. Market orders only — limit orders "
        "are not supported in this deployment."
    )
    return preview


@_tool
async def track_order(order_id: str, timeout_s: float = 10.0) -> dict:
    """Poll get_order until terminal or timeout; never reposts.

    'delayed' is explicitly NOT a fill — the response surfaces the note.
    """
    from polymarket_mcp import orders

    client = await clientmod.build_secure_client(_settings())
    try:
        return await orders.wait_for_settlement(client, order_id, timeout_s=timeout_s)
    finally:
        await client.close()


@_tool
@observability.audit_tool
async def place_batch_order(
    orders_json: str,
    confirm: bool = False,
) -> dict:
    """Place a batch of MARKET orders atomically.

    orders_json is a JSON array of objects with the same field names as
    place_market_order (market/outcome/side/amount/shares/max_spend/
    max_price/min_price/order_type). Limit orders are not supported in this
    deployment. The tool resolves each market, validates, runs the preflight,
    and only then — if POLY_ALLOW_LIVE=1 AND confirm=true — posts them via
    the SDK's ``post_orders``. In DRY-RUN it returns one preview per order.
    """
    import json
    from decimal import Decimal

    from polymarket_mcp import orders

    st = _settings()
    try:
        reqs = json.loads(orders_json)
    except (json.JSONDecodeError, TypeError) as exc:
        raise safety.ValidationError(f"orders_json must be a JSON array: {exc}") from exc
    if not isinstance(reqs, list) or not reqs:
        raise safety.ValidationError("orders_json must be a non-empty JSON array")
    gate = safety.gate_result(st, confirm and not _autonomous())
    if gate["live"]:
        for req in reqs:
            if not isinstance(req, dict):
                raise safety.ValidationError("each order must be a JSON object")
            if safety.validate_side(req.get("side", "")) == "BUY":
                observability.decision("live_buy_blocked")
                safety.reject_live_buy()
    batch = []
    for req in reqs:
        if not isinstance(req, dict):
            raise safety.ValidationError("each order must be a JSON object")
        _mkt, asset_id, how, constraints = await _resolve_market(req["market"], safety.validate_outcome(req["outcome"]))
        if constraints:
            constraints_full = {**constraints, "asset_id": asset_id, "asset_id_via": how}
        else:
            constraints_full = {"asset_id": asset_id, "asset_id_via": how}
        safety.check_notional(st, Decimal(req.get("amount") or req.get("shares") or "0"))
        batch.append(
            {
                "req": req,
                "side": safety.validate_side(req["side"]),
                "outcome": safety.validate_outcome(req["outcome"]),
                "asset_id": asset_id,
                "constraints": constraints_full,
            }
        )
    previews = []
    for b in batch:
        pre = orders.preflight_for_order(st, market_constraints=b["constraints"], side=b["side"], outcome=b["outcome"])
        previews.append(
            safety.order_preview(
                market_slug_or_id=b["req"]["market"], outcome=b["outcome"], side=b["side"],
                kind="MARKET",
                price=None,
                size=None,
                amount=safety.to_decimal(b["req"]["amount"], "amount") if b["req"].get("amount") else None,
                shares=safety.to_decimal(b["req"]["shares"], "shares") if b["req"].get("shares") else None,
                max_spend=safety.to_decimal(b["req"]["max_spend"], "max_spend") if b["req"].get("max_spend") else None,
                max_price=safety.to_decimal(b["req"]["max_price"], "max_price") if b["req"].get("max_price") else None,
                min_price=safety.to_decimal(b["req"]["min_price"], "min_price") if b["req"].get("min_price") else None,
                order_type=b["req"].get("order_type", "FAK"),
                expiration=None,
                post_only=False,
                gate=gate,
                market_constraints=b["constraints"],
            )
        )
        previews[-1]["preflight"] = pre
    if gate["mode"] == "DRY_RUN":
        return {"status": "DRY_RUN", "gate": gate, "orders": previews}

    observability.decision("allowed")
    client = await clientmod.build_secure_client(st)
    try:
        from polymarket_mcp.server import _to_jsonable

        signed = []
        for b in batch:
            req = b["req"]
            signed.append(await client.create_market_order(
                    asset_id=b["asset_id"], side=b["side"],
                    amount=safety.to_decimal(req["amount"], "amount") if req.get("amount") else None,
                    shares=safety.to_decimal(req["shares"], "shares") if req.get("shares") else None,
                    max_spend=safety.to_decimal(req["max_spend"], "max_spend") if req.get("max_spend") else None,
                    max_price=safety.to_decimal(req["max_price"], "max_price") if req.get("max_price") else None,
                    min_price=safety.to_decimal(req["min_price"], "min_price") if req.get("min_price") else None,
                    order_type=req.get("order_type", "FAK"),
                    builder_code=st.builder_code or None,
                ))
        resp = await observability.submit(client.post_orders, signed)
        return {"status": "LIVE", "gate": gate, "response": _to_jsonable(resp)}
    finally:
        await client.close()


# Heartbeat is part of the GTC resting-order lifecycle (docs: get_closed_only_mode/
# order heartbeats). This deployment is MARKET-ONLY, so there are no resting GTC
# orders to keep alive; the tool is intentionally not exposed.



@_tool
async def check_closed_only_mode() -> dict:
    """Return the account's closed-only mode (exit-only market). Read-only."""
    client = await clientmod.build_secure_client(_settings())
    try:
        return {"closed_only_mode": await client.get_closed_only_mode()}
    finally:
        await client.close()


@_tool
@observability.audit_tool
async def check_buy_readiness() -> dict:
    """Check the current BTC 5m BUY prerequisites without authorizing an order."""
    from . import live, ptb
    from .resolution import window_bounds

    start, _, slug = window_bounds(int(time.time()) // 300 * 300)
    reasons: list[str] = []
    try:
        max_age = float(os.getenv("POLY_MAX_PTB_AGE_S", "10"))
        if not math.isfinite(max_age) or max_age <= 0:
            raise ValueError
    except ValueError:
        reasons.append("invalid_POLY_MAX_PTB_AGE_S")
        max_age = None

    assets: dict[str, str] = {}
    try:
        pub = clientmod.build_public_client()
        try:
            market = await pub.get_market(slug=slug)
        finally:
            await pub.close()
        if market.slug != slug:
            reasons.append("market_round_mismatch")
        else:
            if market.state.accepting_orders is not True:
                reasons.append("market_not_accepting_orders")
            assets = {side: _resolve_asset_id(market, side)[0] for side in ("UP", "DOWN")}
            if assets["UP"] == assets["DOWN"]:
                reasons.append("market_assets_not_distinct")
    except Exception:
        reasons.append("market_unverified")

    try:
        account = await clientmod.build_readonly_account_client(_settings())
        try:
            closed_only = await account.get_closed_only_mode()
        finally:
            await account.close()
        if closed_only is True:
            reasons.append("closed_only")
        elif closed_only is not False:
            reasons.append("closed_only_unverified")
    except Exception:
        reasons.append("closed_only_unverified")

    now = time.time()
    if int(now) // 300 * 300 != start:
        observability.decision("readiness_blocked")
        return {"window_start": start, "prerequisites_met": False,
                "reasons": ["round_changed"], "live_buy_allowed": False}
    feed = ptb._FEED
    record = feed.captured(start) if feed is not None else None
    if record is None or record.status != "captured" or record.ptb is None or not record.ptb.is_finite() or record.ptb <= 0:
        reasons.append("ptb_not_captured")
    elif max_age is not None and (
        not feed._running or feed.auth_failed or feed.last_twap_at < start
        or not 0 <= now - feed.last_twap_at <= max_age
    ):
        reasons.append("ptb_stale")

    manager = live.get_manager()
    now_ms = int(now * 1000)
    for side, asset_id in assets.items():
        stream = manager.get_state(asset_id)
        if stream is None or not stream.book or not stream.book.get("asks"):
            reasons.append(f"book_{side}_missing")
        elif not stream.book_received_at_ms or not 0 <= stream.book_age_ms(now_ms) <= manager.cfg["fresh_ms"]:
            reasons.append(f"book_{side}_stale")

    observability.decision("readiness_blocked" if reasons else "readiness_met_live_buy_disabled")
    return {"window_start": start, "prerequisites_met": not reasons,
            "reasons": reasons, "live_buy_allowed": False}


# ----------------------------------------------------------------- PTB FEED --
@_tool
async def start_ptb_feed() -> dict:
    """Start the Price-to-Beat feed (PolyBolt price.crypto.twap).

    Requires POLY_API_KEY/SECRET/PASSPHRASE in env. One connection, one
    subscription (btcusd). Returns immediately; the feed runs in the server
    process until ``stop_ptb_feed``. Idempotent. If the feed already exists,
    it is revalidated against current env at start; a dead singleton (failed
    task) is rebuilt. Without this, ``start_ptb_feed`` could return ``started``
    while the background task was already dead (auth_invalid loop masking).
    """
    from polymarket_mcp.ptb import start_ptb_feed as _start

    feed = await _start()
    # Revalidate: the task must be alive and receiving. The websocket task
    # may still be in its first connect/auth phase when the tool returns, so
    # give it a brief window before judging "running". If it never receives
    # a message, surface the real cause instead of lying about "started".
    import asyncio
    import time
    for _ in range(10):
        if feed._last_msg_at:
            break
        if feed._task is None or feed._task.done():
            break
        await asyncio.sleep(0.25)
    alive = bool(feed._running and feed._task and not feed._task.done())
    receiving = bool(feed._last_msg_at)
    age = time.time() - feed._last_msg_at if feed._last_msg_at else None
    if not (alive and receiving):
        return {
            "status": "failed",
            "channel": "price.crypto.twap",
            "symbol": "btcusd",
            "running": alive,
            "receiving": receiving,
            "last_message_age_s": age,
            "note": "feed task is not receiving data; check that POLY_API_* in the server process env are the derived CLOB creds and that log does not show auth_invalid",
        }
    return {
        "status": "started",
        "channel": "price.crypto.twap",
        "symbol": "btcusd",
        "running": True,
        "last_message_age_s": age,
    }


@_tool
async def stop_ptb_feed() -> dict:
    from polymarket_mcp.ptb import stop_ptb_feed

    await stop_ptb_feed()
    return {"status": "stopped"}


@_tool
async def ptb_status() -> dict:
    from polymarket_mcp.ptb import get_feed

    feed = get_feed()
    recs = feed.store.list()
    return {
        "captured_count": len([r for r in recs if r.status == "captured"]),
        "unavailable_count": len([r for r in recs if r.status == "UNAVAILABLE"]),
        "dropped": feed.dropped_count(),
        "seq_gaps": feed.seq_gaps(),
        "auth_failed": feed.auth_failed,
        "last_error": feed.last_error,
        "last_points": feed.last_points(10),
        "records": [r.__dict__ for r in recs],
    }


@_tool
async def get_ptb(t_start: int) -> dict:
    """Return the PTB record for a 5-minute window, or None if not captured."""
    from polymarket_mcp.ptb import get_feed

    feed = get_feed()
    rec = feed.store.get(t_start)
    if rec is None:
        return {"t_start": t_start, "status": "not_yet_captured"}
    from polymarket_mcp.ptb import settlement_outcome

    return {
        "t_start": rec.t_start,
        "slug": rec.slug,
        "ptb": str(rec.ptb) if rec.ptb is not None else None,
        "close": str(rec.close_ptb) if rec.close_ptb is not None else None,
        "close_ts": rec.close_ts,
        "outcome": settlement_outcome(rec),
        "point_timestamp": rec.point_timestamp.isoformat() if rec.point_timestamp else None,
        "source": rec.source,
        "received_at": rec.received_at,
        "reconstructed_from_snapshot": rec.reconstructed_from_snapshot,
        "status": rec.status,
    }


@_tool
async def wait_for_ptb(t_start: int, timeout: float = 30.0) -> dict:
    """Wait up to `timeout` seconds for the PTB of a window."""
    import asyncio

    from polymarket_mcp.ptb import get_feed

    feed = get_feed()
    ev = feed.store.event_for(t_start)
    try:
        await asyncio.wait_for(ev.wait(), timeout=timeout)
    except TimeoutError:
        return {"t_start": t_start, "status": "timeout", "timeout": timeout}
    rec = feed.store.get(t_start)
    if rec is None:
        return {"t_start": t_start, "status": "not_yet_captured"}
    return {
        "t_start": rec.t_start,
        "slug": rec.slug,
        "ptb": str(rec.ptb) if rec.ptb is not None else None,
        "status": rec.status,
    }


@_tool
async def list_ptb() -> dict:
    from polymarket_mcp.ptb import get_feed

    feed = get_feed()
    recs = feed.store.list()
    return {
        "count": len(recs),
        "records": [
            {
                "t_start": r.t_start,
                "slug": r.slug,
                "ptb": str(r.ptb) if r.ptb is not None else None,
                "point_timestamp": r.point_timestamp.isoformat() if r.point_timestamp else None,
                "status": r.status,
                "reconstructed_from_snapshot": r.reconstructed_from_snapshot,
            }
            for r in recs
        ],
    }


# ------------------------------------------------------ LIVE MARKET DATA --
@_tool
async def start_market_data(binance_symbol: str, coinbase_symbol: str) -> dict:
    """Start the Live Market Data Engine for the requested Spot products.

    Read-only, public feeds. Idempotent. Validation flags (timestamp,
    staleness, feed gaps, outliers) are computed per point in the engine.
    """
    from polymarket_mcp.market_data import start_engine

    engine = start_engine(binance_symbol, coinbase_symbol)
    return {"status": "started", "feeds": [
        f"binance:{engine.binance_symbol}@trade",
        f"coinbase:{engine.coinbase_symbol}@ticker",
    ]}


@_tool
async def stop_market_data() -> dict:
    from polymarket_mcp.market_data import stop_engine

    await stop_engine()
    return {"status": "stopped"}


@_tool
async def market_data_status() -> dict:
    from polymarket_mcp.market_data import get_engine

    return get_engine().snapshot()


@_tool
async def market_data_snapshot(source: str) -> dict:
    """Most recent snapshot for one source ('binance' or 'coinbase')."""
    from polymarket_mcp.market_data import get_engine

    snap = get_engine().snapshot()
    if source == "binance":
        return snap["binance"]
    if source == "coinbase":
        return snap["coinbase"]
    return {"error": "source must be 'binance' or 'coinbase'"}


@_tool
async def market_data_last_points(source: str, limit: int = 20) -> dict:
    """Last N validated points for a source (with flags)."""
    from polymarket_mcp.market_data import get_engine

    if source not in ("binance", "coinbase"):
        return {"error": "source must be 'binance' or 'coinbase'"}
    return {"source": source, "points": get_engine().last_points(source, limit)}


@_tool
async def subscribe_round(slug: str = "", outcome: str = "UP") -> dict:
    """Subscribe to a BTC 5m round by slug (default current) with auto-rollover.

    Uses the official CLOB market WebSocket via the SDK and prefetches the next
    round before the boundary. Read-only: no order is placed.
    """
    from polymarket_mcp.round_streams import subscribe_round as _subscribe

    return await _subscribe(slug, outcome)


@_tool
async def stop_round_watch() -> dict:
    """Stop automatic round rollover watching."""
    from polymarket_mcp.round_streams import stop_round_watch as _stop

    return await _stop()


@_tool
async def round_status() -> dict:
    """Current/next 5m round boundaries and rollover watcher status."""
    from polymarket_mcp.round_streams import round_status as _status

    return await _status()


@_tool
async def start_user_stream(markets: list[str] | None = None) -> dict:
    """Subscribe to the authenticated 'user' stream (order + trade events).

    Requires configured CLOB credentials. Read-only: subscribing never places
    or cancels an order. Optional market filter uses condition IDs.
    """
    from polymarket_mcp.round_streams import start_user_stream as _start

    return await _start(markets)


@_tool
async def stop_user_stream() -> dict:
    """Stop the authenticated user stream and close its client."""
    from polymarket_mcp.round_streams import stop_user_stream as _stop

    return await _stop()


@_tool
async def user_stream_status() -> dict:
    """User stream running state, buffered event count and last error."""
    from polymarket_mcp.round_streams import user_stream_status as _status

    return await _status()


@_tool
async def user_stream_events(limit: int = 50) -> dict:
    """Last buffered user order/trade events (newest last)."""
    from polymarket_mcp.round_streams import user_stream_events as _events

    return await _events(limit)


@_tool
async def get_btc5m_snapshot(tape_limit: int = 10) -> dict:
    """One-call BTC 5m snapshot: round, PTB, both live books, tape and 5-sh fills.

    Read-only. Reduces an agent's multi-call chain to a single MCP call so the
    verdict is formed before the book is swept. Missing values stay explicit.
    """
    import time

    from polymarket_mcp.round_streams import _round_boundaries

    limit = max(1, min(int(tape_limit), 50))
    info = await _round_boundaries()
    start = int(info["current_window_start"])
    result: dict[str, Any] = {
        **info,
        "as_of": int(time.time()),
        "assets": {},
        "books": {},
        "fills": {},
        "tape": {},
        "market_constraints": None,
        "errors": [],
    }
    market = None
    pub = clientmod.build_public_client()
    try:
        market = await pub.get_market(slug=info["current_slug"])
    except Exception as exc:
        result["errors"].append(f"market:{type(exc).__name__}")
    finally:
        await pub.close()
    if market is not None:
        try:
            trading = getattr(market, "trading", None)
            result["market_constraints"] = _to_jsonable(trading) if trading is not None else None
        except Exception as exc:
            result["errors"].append(f"constraints:{type(exc).__name__}")
        for side in ("UP", "DOWN"):
            try:
                asset_id, how = _resolve_asset_id(market, side)
                result["assets"][side] = {"asset_id": asset_id, "via": how}
            except Exception as exc:
                result["errors"].append(f"{side}:{type(exc).__name__}")
    try:
        result["ptb"] = await get_ptb(start)
    except Exception as exc:
        result["errors"].append(f"ptb:{type(exc).__name__}")
        result["ptb"] = {"t_start": start, "status": "error"}
    for side, data in result["assets"].items():
        asset_id = data["asset_id"]
        try:
            result["books"][side] = await get_live_top_of_book(asset_id)
        except Exception as exc:
            result["errors"].append(f"book.{side}:{type(exc).__name__}")
            result["books"][side] = {"asset_id": asset_id, "status": "error"}
        try:
            ask = result["books"][side].get("ask")
            if ask is None:
                result["errors"].append(f"fill.{side}:NoAsk")
                result["fills"][side] = {"asset_id": asset_id, "estimated_price": None}
                continue
            best_ask = decimal.Decimal(str(ask))
            tick = decimal.Decimal(str(result["books"][side].get("tick_size")))
            if tick <= 0:
                raise decimal.InvalidOperation
            amount = ((decimal.Decimal(5) * best_ask / tick).to_integral_value(
                rounding=decimal.ROUND_CEILING
            ) * tick)
            result["fills"][side] = await estimate_market_price(
                asset_id, "BUY", amount=str(amount),
            )
        except Exception as exc:
            result["errors"].append(f"fill.{side}:{type(exc).__name__}")
            result["fills"][side] = {"asset_id": asset_id, "estimated_price": None}
    for source in ("binance", "coinbase"):
        try:
            payload = await market_data_last_points(source, limit)
            result["tape"][source] = payload.get("points", [])
        except Exception as exc:
            result["errors"].append(f"tape.{source}:{type(exc).__name__}")
            result["tape"][source] = []
    return result


@_tool
async def get_btc5m_resolution(window_start: int | None = None, wait_seconds: int = 0) -> dict:
    """Official BTC 5m settlement from Gamma + Data API v2 (read-only, no keys)."""
    from polymarket_mcp.resolution import get_btc5m_resolution as _get

    return await _get(window_start, wait_seconds)


@_tool
async def get_recent_btc5m_resolutions(count: int = 3) -> dict:
    """Official settlement rows for the last N completed BTC 5m windows (1-12)."""
    from polymarket_mcp.resolution import get_recent_btc5m_resolutions as _get

    return await _get(count)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="polymarket-mcp", description="Polymarket trading MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="Transport to serve (default: stdio)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="HTTP/SSE bind host (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="HTTP/SSE bind port (default 8000)")
    parser.add_argument(
        "--path",
        default="/mcp",
        help="HTTP endpoint path (default /mcp)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    kwargs: dict[str, Any] = {}
    if args.transport != "stdio":
        try:
            _require_http_token(args.host, _configured_http_token)
            security = _transport_security(args.host, args.port)
        except ValueError as exc:
            raise SystemExit(str(exc)) from None
        if _MCP_V2:
            kwargs.update(host=args.host, port=args.port, transport_security=security)
            if args.transport == "streamable-http":
                kwargs["streamable_http_path"] = args.path
        else:
            mcp.settings.host = args.host
            mcp.settings.port = args.port
            mcp.settings.streamable_http_path = args.path
            mcp.settings.transport_security = security
    asyncio.run(observability.run_with_heartbeat(_transport_runner(args.transport), **kwargs))


def _transport_runner(transport: str):
    if transport == "streamable-http":
        return mcp.run_streamable_http_async
    if transport == "sse":
        return mcp.run_sse_async
    return mcp.run_stdio_async


if __name__ == "__main__":
    main()
