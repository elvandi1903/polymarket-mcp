"""Two-factor live gate + input validation."""
import pytest

from polymarket_mcp import safety
from polymarket_mcp.config import Settings


def _s(allow_live):
    return Settings(allow_live=allow_live)


def test_gate_requires_both_factors():
    assert safety.gate_result(_s(False), True)["mode"] == "DRY_RUN"
    assert safety.gate_result(_s(True), False)["mode"] == "DRY_RUN"
    assert safety.gate_result(_s(False), False)["mode"] == "DRY_RUN"
    g = safety.gate_result(_s(True), True)
    assert g["live"] is True and g["mode"] == "LIVE"


def test_validation_rejects_bad_inputs():
    with pytest.raises(safety.ValidationError):
        safety.to_decimal("0", "price")
    with pytest.raises(safety.ValidationError):
        safety.to_decimal("-5", "size")
    with pytest.raises(safety.ValidationError):
        safety.validate_side("HOLD")
    with pytest.raises(safety.ValidationError):
        safety.validate_outcome("maybe")


def test_notional_cap():
    s = Settings(max_notional=__import__("decimal").Decimal("50"))
    safety.check_notional(s, __import__("decimal").Decimal("49.99"))
    with pytest.raises(safety.ValidationError):
        safety.check_notional(s, __import__("decimal").Decimal("50.01"))


def test_order_preview_marks_dry_run():
    p = safety.order_preview(
        market_slug_or_id="x", outcome="yes", side="BUY", kind="LIMIT",
        price=__import__("decimal").Decimal("0.6"), size=__import__("decimal").Decimal("10"),
        amount=None, shares=None, max_spend=None, max_price=None, min_price=None,
        order_type=None, expiration=None, post_only=False,
        gate=safety.gate_result(_s(False), False))
    assert p["status"] == "DRY_RUN"
