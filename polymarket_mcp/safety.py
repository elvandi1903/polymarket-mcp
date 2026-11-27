"""Input validation + two-factor live-trade gate.

A live order requires BOTH:
  1. server env flag POLY_ALLOW_LIVE=1, AND
  2. explicit confirm=true on the individual tool call,
plus passing all validation. If either is missing the call is dry-run only:
it validates, resolves market constraints, and returns a full preview
without signing or posting anything.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Literal

from .config import Settings

MAX_BUY_SHARES = Decimal(5)
MAX_BUY_PRICE = Decimal("0.72")
MAX_BUY_SPEND = Decimal(5)
# At this floor a 1.00 USD entry cannot exceed MAX_BUY_SHARES shares.
MIN_BUY_ASK = Decimal("0.20")


class ValidationError(RuntimeError):
    pass


def reject_live_buy() -> None:
    raise ValidationError(
        "Live BUY is disabled: the market-order SDK cannot guarantee at most "
        f"{MAX_BUY_SHARES} shares, {MAX_BUY_SPEND} USD including fees, and "
        f"{MAX_BUY_PRICE} USD per share. Use preview_buy_depth for DRY-RUN."
    )


def to_decimal(value: str | float, field: str) -> Decimal:
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValidationError(f"{field} must be a number, got {value!r}") from exc
    if d <= 0:
        raise ValidationError(f"{field} must be > 0, got {value!r}")
    return d


def validate_outcome(outcome: str) -> Literal["yes", "no"]:
    o = (outcome or "").strip().lower()
    if o in ("up", "yes"):
        return "yes"
    if o in ("down", "no"):
        return "no"
    raise ValidationError("outcome must be 'UP' or 'DOWN'")


def validate_side(side: str) -> Literal["BUY", "SELL"]:
    s = (side or "").strip().upper()
    if s not in ("BUY", "SELL"):
        raise ValidationError("side must be 'BUY' or 'SELL'")
    return s  # type: ignore[return-value]


def check_notional(settings: Settings, amount: Decimal) -> None:
    if settings.max_notional > 0 and amount > settings.max_notional:
        raise ValidationError(
            f"Amount {amount} exceeds POLY_MAX_NOTIONAL cap {settings.max_notional}"
        )


def gate_result(settings: Settings, confirm: bool) -> dict:
    """Two-factor gate. Returns a summary; does not itself place anything."""
    both = bool(settings.allow_live) and bool(confirm)
    return {
        "live": both,
        "confirm": bool(confirm),
        "POLY_ALLOW_LIVE": settings.allow_live,
        "mode": "LIVE" if both else "DRY_RUN",
    }


def order_preview(
    *,
    market_slug_or_id: str,
    outcome: str,
    side: str,
    kind: str,
    price: Decimal | None,
    size: Decimal | None,
    amount: Decimal | None,
    shares: Decimal | None,
    max_spend: Decimal | None,
    max_price: Decimal | None,
    min_price: Decimal | None,
    order_type: str | None,
    expiration: int | None,
    post_only: bool,
    gate: dict,
    market_constraints: dict | None = None,
) -> dict:
    return {
        "status": gate["mode"],
        "market_ref": market_slug_or_id,
        "outcome": outcome,
        "side": side,
        "order_type": kind,
        "price": str(price) if price is not None else None,
        "size_shares": str(size) if size is not None else None,
        "amount_notional": str(amount) if amount is not None else None,
        "shares": str(shares) if shares is not None else None,
        "max_spend": str(max_spend) if max_spend is not None else None,
        "max_price": str(max_price) if max_price is not None else None,
        "min_price": str(min_price) if min_price is not None else None,
        "fa_k_or_fok": order_type,
        "expiration": expiration,
        "post_only": post_only,
        "gate": gate,
        "market_constraints": market_constraints,
        "note": (
            "DRY-RUN preview only. Nothing was signed or posted. "
            "Set POLY_ALLOW_LIVE=1 AND pass confirm=true to execute."
            if gate["mode"] == "DRY_RUN"
            else "LIVE order submitted."
        ),
    }
