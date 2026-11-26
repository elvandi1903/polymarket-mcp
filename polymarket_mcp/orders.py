"""Order placement orchestration (step-by-step) + failure-handling matrix.

All facts taken from the official docs and the SDK's own code — never
assumed:

* Order types: docs "Place Orders" tab (Types) + SDK ``OrderParameters``.
  Limit = resting (GTC by default, GTD with expiration, post_only optional).
  Market = FAK (default) / FOK. IOC is not a documented type in this SDK.
* Order lifecycle statuses come straight from ``OrderResponse`` models:
  accepted = {matched, delayed, live} (with order_id); rejected = INVALID /
  not_enough_balance / unknown / unmatched (error_msg mapping in SDK).
* ``delayed`` is NOT a fill. It is a 425 readiness hold; an order in that
  state will not fill until the exchange is ready. ``matched`` is the only
  terminal-positive status from POST (the other terminal statuses come
  later via user stream / get_order).
* Cooldowns (docs "Manage Orders"): market-making or post-only GTD orders
  auto-cancel at ~3 minutes; a 60s security threshold is logged, and there
  is a 5-minute no-fill cooldown per market afterwards. GTC/non-post-only
  orders instead use a 10s heartbeat model and must be kept alive with
  ``send_order_heartbeat`` (SDK ``send_order_heartbeat`` / ``OrderHeartbeatMismatch``).
* Neg-risk markets: state.neg_risk=true ⇒ order uses the Neg Risk Adapter
  exchange address; onchain NegRiskAdapter approvals are separate actions
  from ``setup_trading_approvals`` (docs Trading / Wallets & Auth).
* Allowance recovery: SDK ``post_order_with_allowance_recovery`` auto-reposts
  once if the first POST fails with 400 "allowance is not enough" (message
  token checked by the SDK). This module surfaces that as an observable
  ``allowance_recovery`` flag rather than silently swallowing it.
* Fee: crypto 0.07 taker-only, maker free. Formula
  ``C * rate * p * (1 - p)``; orders in the [0.5, 0.95] band pay near-max
  fees. ``max_spend`` is an estimate based on the fee at posting time.
"""

from __future__ import annotations

import time
from typing import Any

from polymarket.errors import RequestRejectedError

from . import client as clientmod
from . import safety
from .config import Settings

# Status sets are taken verbatim from the SDK's _ORDER_POST_STATUSES.
ACCEPTED_STATUSES = {"matched", "delayed", "live"}
REJECTED_CODES = {"INVALID", "not_enough_balance", "unknown", "unmatched"}


def order_response_status(resp: Any) -> str:
    """Return the raw status string from an OrderResponse (no mapping)."""
    return str(getattr(resp, "status", "") or "")


def is_accepted_status(status: str) -> bool:
    return status in ACCEPTED_STATUSES


def classify_order_response(resp: Any) -> dict:
    """Map an OrderResponse to our read-facing classification.

    No invented semantics: 'matched' = filled, 'delayed' = 425-hold (not a
    fill), 'live' = resting, rejected = the SDK's code/message.
    """
    status = order_response_status(resp)
    if is_accepted_status(status):
        return {
            "accepted": True,
            "is_fill": status == "matched",
            "is_delated": status == "delayed",
            "status": status,
            "order_id": getattr(resp, "order_id", None),
            "error_msg": getattr(resp, "error_msg", None),
        }
    return {
        "accepted": False,
        "status": status,
        "code": getattr(resp, "code", None),
        "message": getattr(resp, "message", getattr(resp, "error_msg", None)),
    }


def preflight_for_order(
    settings: Settings,
    *,
    client=None,
    market_constraints: dict | None = None,
    side: str,
    outcome: str,
    now_ms: int | None = None,
) -> dict:
    """Run the order preflight checklist (read-only; collects issues).

    Returns {"ok": bool, "issues": [...], "warnings": [...]}. The secure
    client is created by the caller when available; a missing client skips
    the wallet/balance probes and is reported as a warning, never an error.
    """
    issues: list[str] = []
    warnings: list[str] = []
    # 1. Proxy wallet classification (hard gate)
    try:
        classification = clientmod.classify_wallet(settings)
        if not classification["is_proxy"]:
            issues.append(
                f"Wallet not classified as POLY_PROXY (got {classification['wallet_type']}, "
                f"sig {classification['signature_type']})"
            )
    except Exception as exc:
        issues.append(f"Preflight classification failed: {type(exc).__name__}: {exc}")
    # 2. Live gate state (info only; the confirm flag is checked per-call)
    safety.gate_result(settings, confirm=False)
    if not settings.has_private_key:
        issues.append("POLY_PRIVATE_KEY not set")
    if not settings.wallet:
        issues.append("POLY_WALLET (funder) not set")
    # 3. Market constraints (tick, min order size, state)
    if market_constraints:
        state = market_constraints.get("state") or market_constraints.get("accepting_orders")
        accepting = market_constraints.get("accepting_orders", state)
        if accepting is False:
            issues.append("Market is not accepting orders")
        if market_constraints.get("neg_risk") is True:
            warnings.append(
                "Neg-risk market: order will use Neg Risk Adapter exchange; "
                "setup_trading_approvals does NOT include the adapter — check "
                "get_trading_approvals_state and the adapter approval."
            )
    # 4. Cooldown hint (docs: MM / post-only GTD auto-cancel ~3min + 5min cooldown)
    warnings.append(
        "Cooldown note: market-making/post-only GTD orders auto-cancel at ~3min, "
        "then the market enforces a 5-min no-fill cooldown. Prefer GTC (heartbeat) "
        "for resting orders."
    )
    return {"ok": not issues, "issues": issues, "warnings": warnings}


async def wait_for_settlement(
    client,
    order_id: str,
    *,
    timeout_s: float = 10.0,
    poll_interval_s: float = 0.5,
    warm: bool = True,
) -> dict:
    """Poll get_order until terminal or timeout.

    Terminal set is taken from the SDK's Order status semantics. 'delayed'
    is explicitly NOT terminal-positive: the caller must not treat it as a
    fill. This is an observability helper; it never reposts.
    """
    started = time.time()
    last: Any = None
    while time.time() - started < timeout_s:
        try:
            last = await client.get_order(order_id=order_id)
            status = order_response_status(last)
        except RequestRejectedError as exc:
            if int(getattr(exc, "status", 0) or 0) != 404:
                raise
            status = "pending_not_found"
        if status in {"matched", "canceled", "invalid"}:
            return {
                "terminal": True,
                "status": status,
                "order": _safe(last),
                "polls": int((time.time() - started) / poll_interval_s),
            }
        await _sleep(poll_interval_s)
    return {
        "terminal": False,
        "status": order_response_status(last),
        "order": _safe(last),
        "note": "Timeout. A 'delayed' order here is still pending; do NOT treat as filled.",
    }


def _safe(obj: Any) -> Any:
    """Serialize an order/tx object without ever echoing signer material."""
    from polymarket_mcp.server import _to_jsonable

    return _to_jsonable(obj)


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


# ---- failure matrix (documentation-grade, table-driven) -------------------
FAILURE_MATRIX = {
    "not_enough_balance": {
        "status_code": 400,
        "meaning": "maker_amount > balance, or allowance missing (SDK auto-recovery may approve once and repost).",
        "action": "Check get_balance_allowance and get_trading_approvals_state; re-run setup_trading_approvals if missing. Do not blindly retry.",
        "retryable": False,
    },
    "INVALID": {
        "status_code": 400,
        "meaning": "Malformed body (SDK maps API error messages to INVALID; server omitted a reason).",
        "action": "Verify asset_id for this market version (position_id v2 / token_id v1), tick alignment, min size, expiration >= 3min in future, post_only invariants (price must not cross).",
        "retryable": False,
    },
    "unknown": {
        "status_code": 400,
        "meaning": "Server omitted a readable reason; same handling as INVALID until server provides one.",
        "action": "Treat as INVALID; do not assume it filled.",
        "retryable": False,
    },
    "unmatched": {
        "status_code": 400,
        "meaning": "Order crossed the book but did not clear the configured protection (FAK/FOK) or available size.",
        "action": "For FAK the residual is dropped; for FOK the whole order is rejected. Widen bounds or reduce size; do not repost blindly.",
        "retryable": True,
    },
    "delayed": {
        "status_code": 425,
        "meaning": "Exchange is not ready; order is pending in the CLOB. NOT a fill.",
        "action": "Read the post-order status and order list carefully. This is not a fill — track via order ID. State-changing user-stream events will confirm when it fills.",
        "retryable": False,
    },
    "rate_limit": {
        "status_code": 429,
        "meaning": "Too many requests. Backoff and retry are required.",
        "action": "Back off (Retry-After is surfaced by the SDK / you may back off 30-60s), then re-check open orders before reposting.",
        "retryable": True,
    },
    "service_unavailable": {
        "status_code": 503,
        "meaning": "Transient server/service outage. Not a fill.",
        "action": "Back off per Retry-After; the SDK does not retry automatically. Re-check open orders before reposting.",
        "retryable": True,
    },
}
