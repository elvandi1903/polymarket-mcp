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
import decimal
from typing import Any

try:
    # RESMI: mcp >= 2.x
    from mcp.server.mcpserver import MCPServer as _Server
    _MCP_V2 = True
except ImportError:
    # RESMI: mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server
    _MCP_V2 = False

from . import client as clientmod
from . import safety
from .config import Settings

INSTRUCTIONS = (
    "Polymarket trading tools. Read tools are always safe. Order tools are "
    "DRY-RUN by default: they validate and preview only. To place a LIVE order "
    "the caller must pass confirm=true AND the server env POLY_ALLOW_LIVE must "
    "be 1. The account is a legacy Proxy Wallet; every order call first runs a "
    "preflight that refuses unless the wallet classifies as POLY_PROXY "
    "(signature_type=1). For BUY market orders provide amount (USD notional); "
    "for SELL market orders provide shares. Limit orders need price and size "
    "(shares). Always show the user the order fields and the DRY-RUN preview "
    "before asking for confirm=true."
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

        if not self._expected or token != self._expected:
            return None
        return AccessToken(
            token=token,
            client_id="polymarket-mcp",
            scopes=[],
        )


def _transport_security(host: str, allowed_hosts: list[str] | None = None):
    from mcp.server.transport_security import TransportSecuritySettings

    if allowed_hosts is None:
        if host == "0.0.0.0":
            return TransportSecuritySettings(enable_dns_rebinding_protection=False)
        if host in ("127.0.0.1", "localhost"):
            return TransportSecuritySettings(
                enable_dns_rebinding_protection=True,
                allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*"],
                allowed_origins=["*"],
            )
        return TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[f"{host}:*", "127.0.0.1:*", "localhost:*"],
            allowed_origins=["*"],
        )
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=["*"],
    )


def _make_server(host: str = "0.0.0.0", port: int = 8000,
                 allowed_hosts: list[str] | None = None) -> Any:
    import os

    token = os.environ.get("POLY_HTTP_TOKEN", "").strip()
    if _MCP_V2:
        kwargs: dict[str, Any] = {"instructions": INSTRUCTIONS}
        if token:
            from mcp.server.auth.settings import AuthSettings

            kwargs["token_verifier"] = _StaticBearerVerifier(token)
            kwargs["auth"] = AuthSettings(
                issuer_url="https://polymarket.local",
                resource_server_url="https://polymarket.local/mcp",
                required_scopes=[],
            )
        return _Server("polymarket-trading", **kwargs)

    transport_security = _transport_security(host, allowed_hosts)
    if token:
        from mcp.server.auth.settings import AuthSettings

        return _Server(
            "polymarket-trading",
            instructions=INSTRUCTIONS,
            host=host,
            port=port,
            token_verifier=_StaticBearerVerifier(token),
            auth=AuthSettings(
                issuer_url="https://polymarket.local",
                resource_server_url="https://polymarket.local/mcp",
                required_scopes=[],
            ),
            transport_security=transport_security,
        )
    return _Server(
        "polymarket-trading",
        instructions=INSTRUCTIONS,
        host=host,
        port=port,
        transport_security=transport_security,
    )


mcp = _make_server()


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


@mcp.tool()
async def search_markets(q: str, page_size: int = 10) -> dict:
    """Search Polymarket for markets/events. Read-only, no key required."""
    pub = clientmod.build_public_client()
    try:
        page = await pub.search(q=q, page_size=page_size).first_page()
        from polymarket_mcp.server import _to_jsonable

        return {"results": _to_jsonable(page.items if hasattr(page, "items") else page)}
    finally:
        await pub.close()


@mcp.tool()
async def get_market(slug: str = "", market_id: str = "", url: str = "") -> dict:
    """Fetch one market by slug, id, or URL and return its trading constraints."""
    pub = clientmod.build_public_client()
    try:
        from polymarket_mcp.server import _to_jsonable

        market = await pub.get_market(slug=slug or None, id=market_id or None, url=url or None)
        return _to_jsonable(market)
    finally:
        await pub.close()


@mcp.tool()
async def get_order_book(asset_id: str) -> dict:
    """Order book for one outcome asset id (token/position id). Read-only."""
    pub = clientmod.build_public_client()
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await pub.get_order_book(asset_id=asset_id))
    finally:
        await pub.close()


@mcp.tool()
async def get_midpoint(asset_id: str) -> dict:
    pub = clientmod.build_public_client()
    try:
        return {"asset_id": asset_id, "midpoint": str(await pub.get_midpoint(asset_id=asset_id))}
    finally:
        await pub.close()


@mcp.tool()
async def get_price(asset_id: str, side: str = "BUY") -> dict:
    pub = clientmod.build_public_client()
    try:
        return {"asset_id": asset_id, "side": side.upper(),
                "price": str(await pub.get_price(asset_id=asset_id, side=safety.validate_side(side)))}
    finally:
        await pub.close()


@mcp.tool()
async def get_last_trade_price(asset_id: str) -> dict:
    pub = clientmod.build_public_client()
    try:
        from polymarket_mcp.server import _to_jsonable

        ltp = await pub.get_last_trade_price(asset_id=asset_id)
        return _to_jsonable(ltp) if ltp is not None else {"asset_id": asset_id, "last_trade_price": None}
    finally:
        await pub.close()


@mcp.tool()
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


@mcp.tool()
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


@mcp.tool()
async def get_balance_allowance(asset_type: str = "COLLATERAL", asset_id: str = "") -> dict:
    """Balance + allowance for the trading account. Read-only; requires keys."""
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await client.get_balance_allowance(
            asset_type=asset_type, asset_id=asset_id or None))
    finally:
        await client.close()


@mcp.tool()
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


@mcp.tool()
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


@mcp.tool()
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


@mcp.tool()
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
    """Place a market order. DRY-RUN unless POLY_ALLOW_LIVE=1 AND confirm=true.

    BUY uses amount (USD notional); SELL uses shares. max_spend caps total
    USD incl. fees; max_price/min_price bound the execution price.
    order_type is FAK (default) or FOK.
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
    cap = amt if amt is not None else (mspend if mspend is not None else Decimal("0"))
    safety.check_notional(st, cap)
    gate = safety.gate_result(st, confirm)
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

    client = await clientmod.build_secure_client(st)
    try:
        from polymarket_mcp.server import _to_jsonable

        resp = await client.place_market_order(
            asset_id=asset_id, side=s, amount=amt, shares=shr,
            max_spend=mspend, max_price=mxp, min_price=mnp,
            order_type=ot, builder_code=st.builder_code or None)
        return _to_jsonable(resp)
    finally:
        await client.close()


@mcp.tool()
async def cancel_order(order_id: str, confirm: bool = False) -> dict:
    """Cancel one order. DRY-RUN unless POLY_ALLOW_LIVE=1 AND confirm=true."""
    gate = safety.gate_result(_settings(), confirm)
    if gate["mode"] == "DRY_RUN":
        return {"status": "DRY_RUN", "action": "cancel_order", "order_id": order_id,
                "gate": gate, "note": "Nothing cancelled. POLY_ALLOW_LIVE=1 AND confirm=true required."}
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await client.cancel_order(order_id=order_id))
    finally:
        await client.close()


@mcp.tool()
async def cancel_all_orders(confirm: bool = False) -> dict:
    gate = safety.gate_result(_settings(), confirm)
    if gate["mode"] == "DRY_RUN":
        return {"status": "DRY_RUN", "action": "cancel_all", "gate": gate,
                "note": "Nothing cancelled. POLY_ALLOW_LIVE=1 AND confirm=true required."}
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        return _to_jsonable(await client.cancel_all())
    finally:
        await client.close()


@mcp.tool()
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
    client = await clientmod.build_secure_client(_settings())
    try:
        from polymarket_mcp.server import _to_jsonable

        handle = await client.setup_trading_approvals()
        return {"status": "submitted", "handle": _to_jsonable(handle)}
    finally:
        await client.close()


# -------------------------------------------------------------- LIVE BOOK --
@mcp.tool()
async def subscribe_book(asset_id: str, slug: str = "") -> dict:
    """Start (or reuse) a live WS subscription to an outcome asset id."""
    from polymarket_mcp.live import get_manager

    m = get_manager()
    st = await m.subscribe(asset_id, slug=slug)
    return {"subscription": st.snapshot_dict(), "ok": True}


@mcp.tool()
async def unsubscribe_book(asset_id: str) -> dict:
    from polymarket_mcp.live import get_manager

    m = get_manager()
    ok = await m.unsubscribe(asset_id)
    return {"asset_id": asset_id, "ok": bool(ok)}


@mcp.tool()
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


@mcp.tool()
async def get_live_top_of_book(asset_id: str) -> dict:
    """Return a compact, timestamped top-of-book snapshot for an asset."""
    from datetime import datetime
    from polymarket_mcp.live import get_manager

    manager = get_manager()
    st = manager.get_state(asset_id)
    if st is None:
        st = await manager.subscribe(asset_id)
    snap = st.snapshot_dict()
    book = snap.get("book_snapshot") or {}
    bbo = snap.get("best_bid_ask") or {}
    bid, ask = bbo.get("best_bid"), bbo.get("best_ask")
    timestamp = bbo.get("timestamp")
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
    def epoch_ms(value):
        if isinstance(value, datetime) and value.tzinfo is not None:
            return int(value.timestamp() * 1000)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    return int(parsed.timestamp() * 1000)
            except ValueError:
                pass
        return None

    timestamp_ms = epoch_ms(timestamp)
    hash_timestamp_ms = epoch_ms(book.get("timestamp"))
    if not book:
        bid = ask = None
        timestamp_ms = None
    return {
        "asset_id": asset_id,
        "slug": snap.get("slug"),
        "availability": snap.get("availability"),
        "age_ms": snap.get("age_ms"),
        "received_at_ms": snap.get("last_event_at_ms"),
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


@mcp.tool()
async def stream_status() -> dict:
    from polymarket_mcp.live import get_manager

    return get_manager().status_snapshot()


@mcp.tool()
async def stream_events(asset_id: str, limit: int = 50) -> dict:
    from polymarket_mcp.live import get_manager

    return {"asset_id": asset_id, "events": get_manager().events_snapshot(asset_id, limit)}


@mcp.tool()
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


@mcp.tool()
async def watch_asset(slug: str, outcome: str = "UP") -> dict:
    """Subscribe to a market by slug and return the freshest live book.

    Useful right before a BTC 5m boundary: it resolves the asset_id from the
    slug, subscribes (or reuses) and returns a live snapshot.
    """
    from polymarket_mcp.live import boundaries, get_manager

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
        import time

        await m._rest_fallback(st)
        snap = st.snapshot_dict()
        snap["availability"] = "resync"
        snap["slug"] = slug
    return snap


# ---------------------------------------------------------- CREDENTIALS ---
@mcp.tool()
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


@mcp.tool()
async def validate_api_credentials() -> dict:
    """Validate that the configured CLOB credentials are still active.

    Performs a real L2-signed GET /auth/api-keys probe (docs: get order list
    authentication). Returns active/invalid; 401 = invalid, other errors
    surface their endpoint status.
    """
    from polymarket.models.clob.api_key import ApiKeyCreds

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


@mcp.tool()
async def test_connection() -> dict:
    """End-to-end connectivity test (preflight -> client -> balance read).

    Read-only; no orders, no state changes. Safe to run repeatedly.
    """
    import polymarket_mcp.credentials as creds

    return await creds.test_connection(_settings())


@mcp.tool()
async def list_saved_credentials() -> dict:
    """Non-secret summary of the persisted credential file (if any)."""
    import polymarket_mcp.credentials as creds

    return creds.saved_summary()


@mcp.tool()
async def save_api_credentials(api_key: str, secret: str, passphrase: str) -> dict:
    """Persist supplied credentials to the 0600 file (atomic write).

    The values are written only to disk; the response contains no secret.
    Prefer populating POLY_API_* via env for ephemeral runs.
    """
    import polymarket_mcp.credentials as creds
    from polymarket.models.clob.api_key import ApiKeyCreds

    return creds.save_credentials(ApiKeyCreds(apiKey=api_key, secret=secret, passphrase=passphrase))


@mcp.tool()
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
@mcp.tool()
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


@mcp.tool()
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


@mcp.tool()
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
    gate = safety.gate_result(st, confirm)
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
        resp = await client.post_orders(signed)
        return {"status": "LIVE", "gate": gate, "response": _to_jsonable(resp)}
    finally:
        await client.close()


# Heartbeat is part of the GTC resting-order lifecycle (docs: get_closed_only_mode/
# order heartbeats). This deployment is MARKET-ONLY, so there are no resting GTC
# orders to keep alive; the tool is intentionally not exposed.



@mcp.tool()
async def check_closed_only_mode() -> dict:
    """Return the account's closed-only mode (exit-only market). Read-only."""
    client = await clientmod.build_secure_client(_settings())
    try:
        return {"closed_only_mode": await client.get_closed_only_mode()}
    finally:
        await client.close()


# ----------------------------------------------------------------- PTB FEED --
@mcp.tool()
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
    try:
        for _ in range(10):
            if feed._last_msg_at:
                break
            if feed._task is None or feed._task.done():
                break
            await asyncio.sleep(0.25)
    except asyncio.CancelledError:
        raise
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


@mcp.tool()
async def stop_ptb_feed() -> dict:
    from polymarket_mcp.ptb import stop_ptb_feed

    await stop_ptb_feed()
    return {"status": "stopped"}


@mcp.tool()
async def ptb_status() -> dict:
    from polymarket_mcp.ptb import get_feed

    feed = get_feed()
    recs = feed.store.list()
    return {
        "captured_count": len([r for r in recs if r.status == "captured"]),
        "unavailable_count": len([r for r in recs if r.status == "UNAVAILABLE"]),
        "dropped": feed.dropped_count(),
        "seq_gaps": feed.seq_gaps(),
        "last_points": feed.last_points(10),
        "records": [r.__dict__ for r in recs],
    }


@mcp.tool()
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


@mcp.tool()
async def wait_for_ptb(t_start: int, timeout: float = 30.0) -> dict:
    """Wait up to `timeout` seconds for the PTB of a window."""
    import asyncio

    from polymarket_mcp.ptb import get_feed

    feed = get_feed()
    ev = feed.store.event_for(t_start)
    try:
        await asyncio.wait_for(ev.wait(), timeout=timeout)
    except asyncio.TimeoutError:
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


@mcp.tool()
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
@mcp.tool()
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


@mcp.tool()
async def stop_market_data() -> dict:
    from polymarket_mcp.market_data import stop_engine

    await stop_engine()
    return {"status": "stopped"}


@mcp.tool()
async def market_data_status() -> dict:
    from polymarket_mcp.market_data import get_engine

    return get_engine().snapshot()


@mcp.tool()
async def market_data_snapshot(source: str) -> dict:
    """Most recent snapshot for one source ('binance' or 'coinbase')."""
    from polymarket_mcp.market_data import get_engine

    snap = get_engine().snapshot()
    if source == "binance":
        return snap["binance"]
    if source == "coinbase":
        return snap["coinbase"]
    return {"error": "source must be 'binance' or 'coinbase'"}


@mcp.tool()
async def market_data_last_points(source: str, limit: int = 20) -> dict:
    """Last N validated points for a source (with flags)."""
    from polymarket_mcp.market_data import get_engine

    if source not in ("binance", "coinbase"):
        return {"error": "source must be 'binance' or 'coinbase'"}
    return {"source": source, "points": get_engine().last_points(source, limit)}


@mcp.tool()
async def subscribe_round(slug: str = "", outcome: str = "UP") -> dict:
    """Subscribe to a BTC 5m round by slug (default current) with auto-rollover.

    Uses the official CLOB market WebSocket via the SDK and prefetches the next
    round before the boundary. Read-only: no order is placed.
    """
    from polymarket_mcp.round_streams import subscribe_round as _subscribe

    return await _subscribe(slug, outcome)


@mcp.tool()
async def stop_round_watch() -> dict:
    """Stop automatic round rollover watching."""
    from polymarket_mcp.round_streams import stop_round_watch as _stop

    return await _stop()


@mcp.tool()
async def round_status() -> dict:
    """Current/next 5m round boundaries and rollover watcher status."""
    from polymarket_mcp.round_streams import round_status as _status

    return await _status()


@mcp.tool()
async def start_user_stream(markets: list[str] | None = None) -> dict:
    """Subscribe to the authenticated 'user' stream (order + trade events).

    Requires configured CLOB credentials. Read-only: subscribing never places
    or cancels an order. Optional market filter uses condition IDs.
    """
    from polymarket_mcp.round_streams import start_user_stream as _start

    return await _start(markets)


@mcp.tool()
async def stop_user_stream() -> dict:
    """Stop the authenticated user stream and close its client."""
    from polymarket_mcp.round_streams import stop_user_stream as _stop

    return await _stop()


@mcp.tool()
async def user_stream_status() -> dict:
    """User stream running state, buffered event count and last error."""
    from polymarket_mcp.round_streams import user_stream_status as _status

    return await _status()


@mcp.tool()
async def user_stream_events(limit: int = 50) -> dict:
    """Last buffered user order/trade events (newest last)."""
    from polymarket_mcp.round_streams import user_stream_events as _events

    return await _events(limit)


@mcp.tool()
async def get_btc5m_resolution(window_start: int | None = None, wait_seconds: int = 0) -> dict:
    """Official BTC 5m settlement from Gamma + Data API v2 (read-only, no keys)."""
    from polymarket_mcp.resolution import get_btc5m_resolution as _get

    return await _get(window_start, wait_seconds)


@mcp.tool()
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
    if _MCP_V2:
        if args.transport in ("streamable-http", "sse"):
            kwargs: dict[str, Any] = {
                "host": args.host,
                "port": args.port,
                "transport_security": _transport_security(args.host),
            }
            if args.transport == "streamable-http":
                kwargs["streamable_http_path"] = args.path
            mcp.run(transport=args.transport, **kwargs)
        else:
            mcp.run(transport="stdio")
        return

    if args.transport in ("streamable-http", "sse"):
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        mcp.settings.streamable_http_path = args.path
        from mcp.server.transport_security import TransportSecuritySettings

        if args.host == "0.0.0.0":
            mcp.settings.transport_security = TransportSecuritySettings(
                enable_dns_rebinding_protection=False,
            )
        elif args.host in ("127.0.0.1", "localhost"):
            mcp.settings.transport_security = TransportSecuritySettings(
                enable_dns_rebinding_protection=True,
                allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*"],
                allowed_origins=["*"],
            )
    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
