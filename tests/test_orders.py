"""Order placement failure-handling (offline, no network/keys).

All expectations are taken from the SDK models and docs, not invented:
accepted = {matched, delayed, live}; 'delayed' is NOT a fill; matrix codes
and status codes are the ones in _internal/actions/orders.
"""
from types import SimpleNamespace as NS

from polymarket_mcp import orders
from polymarket_mcp.config import Settings


def _R(status, oid="", code=None, msg=None):
    return NS(status=status, order_id=oid, code=code, error_msg=msg)


def test_accepted_classification():
    for st_, is_fill, is_delated in [
        ("matched", True, False),
        ("delayed", False, True),
        ("live", False, False),
    ]:
        c = orders.classify_order_response(_R(st_, "0xabc"))
        assert c["accepted"] is True and c["is_fill"] is is_fill and c["is_delated"] is is_delated


def test_rejected_classification():
    for st_, code, msg in [
        ("unmatched", None, "unmatched"),
        ("invalid", "INVALID", "bad tick"),
        ("", None, None),
    ]:
        c = orders.classify_order_response(_R(st_, "", code=code, msg=msg))
        assert c["accepted"] is False


def test_delayed_is_not_a_fill():
    c = orders.classify_order_response(_R("delayed", "0xok"))
    assert c["is_fill"] is False and c["is_delated"] is True


def test_failure_matrix_covers_all_codes():
    expected = {"not_enough_balance", "INVALID", "unknown", "unmatched",
                "delayed", "rate_limit", "service_unavailable"}
    assert expected <= set(orders.FAILURE_MATRIX)
    for v in orders.FAILURE_MATRIX.values():
        assert "meaning" in v and "action" in v and "status_code" in v


def test_status_code_mapping():
    assert orders.FAILURE_MATRIX["delayed"]["status_code"] == 425
    assert orders.FAILURE_MATRIX["rate_limit"]["status_code"] == 429
    assert orders.FAILURE_MATRIX["service_unavailable"]["status_code"] == 503
    for k in ("not_enough_balance", "INVALID", "unknown", "unmatched"):
        assert orders.FAILURE_MATRIX[k]["status_code"] == 400


def test_preflight_rejects_non_proxy_wallet():
    s = Settings(private_key="0x" + "11" * 32, wallet="0x" + "22" * 20)
    pre = orders.preflight_for_order(s, side="BUY", outcome="yes")
    assert pre["ok"] is False and pre["issues"]
    assert any("POLY_PROXY" in i for i in pre["issues"])


def test_preflight_missing_keys_rejected():
    s = Settings(private_key="", wallet="")
    pre = orders.preflight_for_order(s, side="BUY", outcome="yes")
    assert pre["ok"] is False


def test_preflight_warns_on_negrisk():
    s = Settings(private_key="0x" + "11" * 32, wallet="0x6Cd089333445c89b55B7693c6009DCc229613738")
    pre = orders.preflight_for_order(
        s, side="BUY", outcome="yes",
        market_constraints={"neg_risk": True, "accepting_orders": True},
    )
    assert any("Neg-risk" in w or "neg" in w.lower() for w in pre["warnings"])


class _FakeClient:
    def __init__(self, seq):
        self.seq = list(seq)

    async def get_order(self, order_id: str):
        return self.seq.pop(0)


def _R2(status, oid="0x1", code=None):
    return NS(status=status, order_id=oid, code=code, error_msg=None)


def test_wait_for_settlement_terminal_on_matched():
    import asyncio

    async def go():
        out = await orders.wait_for_settlement(
            _FakeClient([_R2("live"), _R2("matched")]), "0x1", timeout_s=2.0, poll_interval_s=0.01
        )
        assert out["terminal"] is True and out["status"] == "matched"
    asyncio.new_event_loop().run_until_complete(go())


def test_wait_for_settlement_tolerates_not_found_then_settles():
    import asyncio

    from polymarket.errors import RequestRejectedError

    class Client:
        def __init__(self):
            self.calls = 0

        async def get_order(self, *, order_id):
            self.calls += 1
            if self.calls == 1:
                raise RequestRejectedError("not found", status=404)
            return _R("matched", "0x1")

    async def go():
        fake = Client()
        out = await orders.wait_for_settlement(
            fake, "0x1", timeout_s=1.0, poll_interval_s=0.01
        )
        assert out["terminal"] is True and out["status"] == "matched"
        assert fake.calls == 2

    asyncio.new_event_loop().run_until_complete(go())


def test_wait_for_settlement_timeout_keeps_delayed_as_pending():
    import asyncio

    async def go():
        # enough canned responses to fill the timeout; last is what persists
        out = await orders.wait_for_settlement(
            _FakeClient([_R2("delayed")] * 20), "0x1", timeout_s=0.05, poll_interval_s=0.01
        )
        assert out["terminal"] is False and out["status"] == "delayed"
        assert "delayed" in out["note"].lower()
    asyncio.new_event_loop().run_until_complete(go())
