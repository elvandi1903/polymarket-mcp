"""Asset resolution must follow official v1/v2 rules, never guess."""
from types import SimpleNamespace as NS

from polymarket_mcp.server import _resolve_asset_id


def _market(version, yes_tid="yes-token", yes_pid="yes-position"):
    return NS(
        version=version,
        outcomes=NS(
            yes=NS(label="Up", token_id=yes_tid, position_id=yes_pid),
            no=NS(label="Down", token_id="no-token", position_id="no-position"),
        ),
        position_ids=("fallback-up", "fallback-down"),
    )


def test_v2_uses_position_id_not_token_id():
    for version in ("v2", NS(value="v2")):
        asset, how = _resolve_asset_id(_market(version), "UP")
        assert asset == "yes-position"
        assert "position_id" in how


def test_v1_uses_ctf_token_id():
    asset, how = _resolve_asset_id(_market("v1"), "DOWN")
    assert asset == "no-token"
    assert "token_id" in how


def test_fallback_index_up_down():
    market = NS(
        version="v1",
        outcomes=None,
        position_ids=("up-position", "down-position"),
    )
    assert _resolve_asset_id(market, "UP")[0] == "up-position"
    assert _resolve_asset_id(market, "DOWN")[0] == "down-position"
