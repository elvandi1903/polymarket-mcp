"""Fail-closed configuration for the autonomous BTC 5m BUY tool.

Reads the process environment only. Deliberately does not fall back to the
operator's ``.env`` file: a required risk limit must be exported explicitly,
so a missing variable can never be satisfied by a stale local file.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from . import safety

_CENT = Decimal("0.01")


def _raw(name: str) -> str:
    return os.environ.get(name, "").strip()


def _decimal(name: str, raw: str, *, minimum: Decimal | None = None,
             maximum: Decimal | None = None) -> Decimal:
    if not raw:
        raise safety.ValidationError(f"Missing required BUY env: {name}")
    try:
        value = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise safety.ValidationError(f"{name} must be a decimal") from exc
    if not value.is_finite():
        raise safety.ValidationError(f"{name} must be finite")
    if minimum is not None and value < minimum:
        raise safety.ValidationError(f"{name} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise safety.ValidationError(f"{name} must be <= {maximum}")
    return value


def _integer(name: str, raw: str, default: int, *, minimum: int) -> int:
    text = raw or str(default)
    try:
        value = int(text)
    except ValueError as exc:
        raise safety.ValidationError(f"{name} must be an integer") from exc
    if value < minimum:
        raise safety.ValidationError(f"{name} must be >= {minimum}")
    return value


@dataclass(frozen=True)
class BuyConfig:
    usd: Decimal
    fee_buffer: Decimal
    max_notional: Decimal
    max_daily_notional: Decimal
    max_daily_loss: Decimal
    max_consecutive_losses: int
    price_cap: Decimal
    max_slippage_ticks: int
    entry_min_s: int
    entry_max_s: int
    max_ptb_age_s: float

    @property
    def budget_cost(self) -> Decimal:
        """Amount charged against the budget; fees ride above ``usd``."""
        return self.usd * (1 + self.fee_buffer)

    def public_summary(self) -> dict:
        return {
            "usd": str(self.usd), "fee_buffer": str(self.fee_buffer),
            "budget_cost": str(self.budget_cost), "price_cap": str(self.price_cap),
            "max_notional": str(self.max_notional),
            "max_daily_notional": str(self.max_daily_notional),
            "max_daily_loss": str(self.max_daily_loss),
            "max_consecutive_losses": self.max_consecutive_losses,
            "max_slippage_ticks": self.max_slippage_ticks,
            "entry_window_s": [self.entry_min_s, self.entry_max_s],
            "max_ptb_age_s": self.max_ptb_age_s,
        }


def load() -> BuyConfig:
    usd = _decimal("POLY_BUY_USD", _raw("POLY_BUY_USD") or "1.00",
                   minimum=Decimal("1.00"), maximum=safety.MAX_BUY_SPEND)
    if usd != usd.quantize(_CENT):
        raise safety.ValidationError("POLY_BUY_USD must be a whole number of cents")

    fee_buffer = _decimal("POLY_FEE_BUFFER", _raw("POLY_FEE_BUFFER") or "0.10",
                          minimum=Decimal(0), maximum=Decimal(1))

    max_notional = _decimal("POLY_MAX_NOTIONAL", _raw("POLY_MAX_NOTIONAL"),
                            minimum=_CENT, maximum=safety.MAX_BUY_SPEND)
    max_daily_notional = _decimal("POLY_MAX_DAILY_NOTIONAL", _raw("POLY_MAX_DAILY_NOTIONAL"),
                                  minimum=_CENT)
    max_daily_loss = _decimal("POLY_MAX_DAILY_LOSS", _raw("POLY_MAX_DAILY_LOSS"),
                              minimum=_CENT)

    cost = usd * (1 + fee_buffer)
    if cost > max_notional:
        raise safety.ValidationError("POLY_BUY_USD x (1 + POLY_FEE_BUFFER) exceeds POLY_MAX_NOTIONAL")

    cap = safety.MAX_BUY_PRICE
    raw_cap = _raw("POLY_MAX_BUY_PRICE")
    if raw_cap:
        configured = _decimal("POLY_MAX_BUY_PRICE", raw_cap, minimum=Decimal(0))
        # Fail closed instead of silently clamping: a higher value means the
        # operator believes a looser cap is in force, which is not true.
        if configured > safety.MAX_BUY_PRICE:
            raise safety.ValidationError(
                f"POLY_MAX_BUY_PRICE {configured} exceeds the hard ceiling {safety.MAX_BUY_PRICE}")
        cap = min(cap, configured)

    entry_min = _integer("POLY_ENTRY_MIN_S", _raw("POLY_ENTRY_MIN_S"), 15, minimum=0)
    entry_max = _integer("POLY_ENTRY_MAX_S", _raw("POLY_ENTRY_MAX_S"), 90, minimum=1)
    if entry_min >= entry_max:
        raise safety.ValidationError("POLY_ENTRY_MIN_S must be < POLY_ENTRY_MAX_S")

    try:
        max_ptb_age = float(_raw("POLY_MAX_PTB_AGE_S") or "10")
    except ValueError as exc:
        raise safety.ValidationError("POLY_MAX_PTB_AGE_S must be a number") from exc
    if not math.isfinite(max_ptb_age) or max_ptb_age <= 0:
        raise safety.ValidationError("POLY_MAX_PTB_AGE_S must be > 0")

    return BuyConfig(
        usd=usd, fee_buffer=fee_buffer, max_notional=max_notional,
        max_daily_notional=max_daily_notional, max_daily_loss=max_daily_loss,
        max_consecutive_losses=_integer("POLY_MAX_CONSECUTIVE_LOSSES",
                                        _raw("POLY_MAX_CONSECUTIVE_LOSSES"), 3, minimum=1),
        price_cap=cap,
        max_slippage_ticks=_integer("POLY_MAX_SLIPPAGE_TICKS",
                                    _raw("POLY_MAX_SLIPPAGE_TICKS"), 1, minimum=0),
        entry_min_s=entry_min, entry_max_s=entry_max, max_ptb_age_s=max_ptb_age,
    )
