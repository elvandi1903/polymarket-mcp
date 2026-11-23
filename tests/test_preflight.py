"""Proxy-Wallet preflight: the silent-misclassification guard."""
import pytest

from polymarket_mcp import client as clientmod
from polymarket_mcp.config import Settings

# proxy derived from signer 0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A
DERIVED_PROXY = "0x6Cd089333445c89b55B7693c6009DCc229613738"
HARDCODED_SIGNER = "0x" + "11" * 32


def test_derived_proxy_classifies_as_proxy():
    s = Settings(private_key=HARDCODED_SIGNER, wallet=DERIVED_PROXY)
    info = clientmod.classify_wallet(s)
    assert info["wallet_type"] == "POLY_PROXY"
    assert info["signature_type"] == 1
    assert info["is_proxy"] is True


def test_random_wallet_is_not_proxy():
    s = Settings(private_key=HARDCODED_SIGNER, wallet="0x" + "22" * 20)
    info = clientmod.classify_wallet(s)
    assert info["is_proxy"] is False


def test_assert_proxy_wallet_raises_on_misclassify():
    s = Settings(private_key=HARDCODED_SIGNER, wallet="0x" + "22" * 20)
    with pytest.raises(clientmod.PreflightError):
        clientmod.assert_proxy_wallet(s)


def test_assert_proxy_wallet_ok_on_derived():
    s = Settings(private_key=HARDCODED_SIGNER, wallet=DERIVED_PROXY)
    info = clientmod.assert_proxy_wallet(s)
    assert info["wallet_type"] == "POLY_PROXY"


def test_missing_wallet_raises():
    s = Settings(private_key=HARDCODED_SIGNER, wallet="")
    with pytest.raises(clientmod.PreflightError):
        clientmod.assert_proxy_wallet(s)


def test_relayer_fields_parsed():
    import os
    from unittest.mock import patch
    from polymarket_mcp.config import Settings
    env = {
        "RELAYER_API_KEY": "fake-relayer-key",
        "RELAYER_API_KEY_ADDRESS": "0xE5919322C7d0D52cA4a876Ef02459F0b39E2cCC3",
    }
    with patch.dict(os.environ, env):
        cfg = Settings.from_env()
    assert cfg.relayer_api_key == "fake-relayer-key"
    assert cfg.relayer_address == env["RELAYER_API_KEY_ADDRESS"]
