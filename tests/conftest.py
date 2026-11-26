import os

import pytest

os.environ.setdefault("POLY_PRIVATE_KEY", "0x" + "11" * 32)
os.environ.setdefault("POLY_WALLET", "0x6Cd089333445c89b55B7693c6009DCc229613738")
os.environ.setdefault("POLY_ALLOW_LIVE", "0")


@pytest.fixture(autouse=True)
def isolated_guard_state(monkeypatch, tmp_path):
    monkeypatch.setenv("POLY_GUARD_STATE_DIR", str(tmp_path / "guard-state"))
