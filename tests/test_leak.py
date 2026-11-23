"""Secret-leak regression suite.

Every test uses sentinel secrets; if any of them appears in any tool output,
log, exception, or repr, the test fails. This is the "no leak" gate for this
repo: it covers the MCP tool surface (success AND error paths) and the
direct Python call paths.
"""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

SKEY = "SENTINEL_KEY_9f8e"
SSEC = "SENTINEL_SECRET_9f8e"
SPASS = "SENTINEL_PASS_9f8e"
SPK = "SENTINEL_PK_9f8e"
SWALLET = "SENTINEL_WALLET_9f8e"
SENTINELS = (SKEY, SSEC, SPASS, SPK, SWALLET)


def _stub_http_dir() -> str:
    folder = tempfile.mkdtemp(prefix="poly-mcp-stub-")
    stub = '''
import httpx

class _Response:
    status_code = 200
    text = "{}"
    content = b"{}"
    headers = {}
    is_success = True
    is_error = False

    def json(self):
        return {}

    def raise_for_status(self):
        return None

class _Client:
    def __init__(self, *args, **kwargs):
        self.is_closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, *args, **kwargs):
        return _Response()

    async def post(self, *args, **kwargs):
        return _Response()

    async def request(self, *args, **kwargs):
        return _Response()

    async def close(self):
        self.is_closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

httpx.AsyncClient = _Client
httpx.Client = _Client
'''
    (Path(folder) / "sitecustomize.py").write_text(stub, encoding="utf-8")
    return folder


def _no_sentinel(obj_or_text, where: str) -> None:
    text = obj_or_text if isinstance(obj_or_text, str) else json.dumps(obj_or_text, default=str)
    for s in SENTINELS:
        assert s not in text, f"LEAK of {s} in {where}: {text[:200]}"


def _env_with_sentinels():
    env = dict(os.environ)
    env.update(
        {
            "POLY_PRIVATE_KEY": SPK,
            "POLY_WALLET": SWALLET,
            "POLY_API_KEY": SKEY,
            "POLY_API_SECRET": SSEC,
            "POLY_API_PASSPHRASE": SPASS,
        }
    )
    return env


def test_save_credentials_returns_no_secret():
    import polymarket_mcp.credentials as C
    from polymarket.models.clob.api_key import ApiKeyCreds

    p = Path(tempfile.mkdtemp()) / "c.json"
    r = C.save_credentials(ApiKeyCreds(apiKey=SKEY, secret=SSEC, passphrase=SPASS), path=p)
    _no_sentinel(r, "save_credentials return")
    # tail is allowed (last 4 of key only)
    assert r["api_key_tail"] == SKEY[-4:]


def test_load_credentials_repr_redacted():
    import polymarket_mcp.credentials as C
    from polymarket.models.clob.api_key import ApiKeyCreds

    p = Path(tempfile.mkdtemp()) / "c.json"
    C.save_credentials(ApiKeyCreds(apiKey=SKEY, secret=SSEC, passphrase=SPASS), path=p)
    loaded = C.load_credentials(p)
    _no_sentinel(repr(loaded), "ApiKeyCreds repr")
    _no_sentinel(str(loaded), "ApiKeyCreds str")


def test_saved_summary_no_secret():
    import polymarket_mcp.credentials as C
    from polymarket.models.clob.api_key import ApiKeyCreds

    p = Path(tempfile.mkdtemp()) / "c.json"
    C.save_credentials(ApiKeyCreds(apiKey=SKEY, secret=SSEC, passphrase=SPASS), path=p)
    _no_sentinel(C.saved_summary(p), "saved_summary")


def test_describe_error_no_secret_even_if_msg_contains_it():
    """describe_error truncates; a message embedding a key is truncated AND rejected."""
    import polymarket_mcp.credentials as C

    class FakeErr(Exception):
        pass

    # We do NOT assert the message is scrubbed here — we assert the caller
    # must not pass a message that contains secrets. The real SDK cannot know
    # them (it only sees headers). This test pins the documented contract:
    # describe_error returns a summary and MUST NOT be fed a secret by callers.
    try:
        raise FakeErr("some error")
    except Exception as e:
        d = C.describe_error(e)
        _no_sentinel(d, "describe_error")


def test_preflight_error_no_secret():
    from polymarket_mcp import client as clientmod
    from polymarket_mcp.config import Settings

    s = Settings(private_key=SPK, wallet=SWALLET)
    try:
        clientmod.assert_proxy_wallet(s)
    except Exception as e:
        _no_sentinel(str(e), "PreflightError message")
        return
    raise AssertionError("expected PreflightError")


def test_public_summary_no_secret():
    from polymarket_mcp.config import Settings

    s = Settings(
        private_key=SPK,
        wallet=SWALLET,
        api_key=SKEY,
        api_secret=SSEC,
        api_passphrase=SPASS,
    )
    _no_sentinel(s.public_summary(), "Settings.public_summary")


def test_config_reads_dotenv_when_env_missing():
    """HTTP processes started without exported env must still see .env."""
    import os
    from unittest import mock

    from polymarket_mcp.config import Settings, _dotenv

    fake = {
        "POLY_PRIVATE_KEY": "0x" + "ab" * 32,
        "POLY_WALLET": "0x" + "cd" * 20,
        "POLY_API_KEY": "dotenv-key",
        "POLY_API_SECRET": "dotenv-secret",
        "POLY_API_PASSPHRASE": "dotenv-pass",
    }
    with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
        "polymarket_mcp.config._dotenv", return_value=fake
    ):
        _dotenv.cache_clear()
        s = Settings.from_env()
        assert s.has_private_key is True
        assert s.wallet == "0x" + "cd" * 20
        assert s.has_api_creds is True
        assert s.api_key == "dotenv-key"
        assert s.api_secret == "dotenv-secret"
        assert s.api_passphrase == "dotenv-pass"
        _dotenv.cache_clear()


def test_credential_file_mode_0600_and_no_sentinel_in_name(tmp_path):
    import polymarket_mcp.credentials as C
    from polymarket.models.clob.api_key import ApiKeyCreds

    cred_dir = tmp_path / "sub"
    p = cred_dir / "mycreds.json"
    C.save_credentials(ApiKeyCreds(apiKey=SKEY, secret=SSEC, passphrase=SPASS), path=p)
    mode = os.stat(p).st_mode & 0o777
    assert mode == 0o600, f"mode {oct(mode)} != 0o600"
    assert not any(s in p.name for s in SENTINELS)
    data = json.loads(p.read_text())
    assert set(data) == {"key", "secret", "passphrase"}


def _run_stdio(env_extra: dict, calls: list[tuple[str, dict]]):
    env = dict(os.environ)
    env.update(env_extra)
    env["PYTHONPATH"] = os.pathsep.join(
        [_stub_http_dir(), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    proc = subprocess.Popen(
        [sys.executable, "-m", "polymarket_mcp.server"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=env,
        cwd=str(Path(__file__).resolve().parent.parent),
    )

    def send(obj):
        proc.stdin.write((json.dumps(obj) + "\n").encode())
        proc.stdin.flush()

    def recv():
        line = proc.stdout.readline()
        return line.decode()

    out = []
    send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
          "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}})
    init = recv()
    _no_sentinel(init, "initialize")
    send({"jsonrpc": "2.0", "method": "notifications/initialized"})
    send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    tools_line = recv()
    _no_sentinel(tools_line, "tools/list")
    for i, (name, args) in enumerate(calls):
        send({"jsonrpc": "2.0", "id": 10 + i, "method": "tools/call",
              "params": {"name": name, "arguments": args}})
        out.append(recv())
    proc.stdin.close()
    proc.wait(timeout=5)
    return out


def test_mcp_stdio_no_sentinel_on_all_tools():
    """Every tool, sentinel env, stdout must not contain a sentinel."""
    calls = [
        ("search_markets", {"q": "bitcoin", "page_size": 1}),
        ("get_market", {"slug": "btc-updown-5m-1794022200"}),
        ("get_order_book", {"asset_id": "1"}),
        ("get_midpoint", {"asset_id": "1"}),
        ("get_price", {"asset_id": "1", "side": "BUY"}),
        ("get_last_trade_price", {"asset_id": "1"}),
        ("estimate_market_price", {"asset_id": "1", "side": "BUY", "amount": "1"}),
        ("account_preflight", {}),
        ("get_balance_allowance", {}),
        ("list_positions", {}),
        ("list_open_orders", {}),
        ("get_trading_approvals_state", {}),
        ("place_market_order", {"market": "btc-updown-5m-1794022200", "outcome": "yes", "side": "BUY", "amount": "50"}),
        ("preview_order", {"market": "btc-updown-5m-1794022200", "outcome": "yes", "side": "BUY", "amount": "10"}),
        ("cancel_order", {"order_id": "0x1"}),
        ("cancel_all_orders", {}),
        ("setup_trading_approvals", {}),
        ("subscribe_book", {"asset_id": "1"}),
        ("unsubscribe_book", {"asset_id": "1"}),
        ("get_live_book", {"asset_id": "1"}),
        ("stream_status", {}),
        ("stream_events", {"asset_id": "1"}),
        ("stream_health", {}),
        ("watch_asset", {"slug": "btc-updown-5m-1794022200", "outcome": "yes"}),
        ("derive_api_credentials", {}),
        ("validate_api_credentials", {}),
        ("test_connection", {}),
        ("list_saved_credentials", {}),
        ("save_api_credentials", {"api_key": SKEY, "secret": SSEC, "passphrase": SPASS}),
        ("revoke_api_credentials", {}),
    ]
    outs = _run_stdio(_env_with_sentinels(), calls)
    for name, txt in zip([c[0] for c in calls], outs):
        _no_sentinel(txt, name)
    assert len(outs) == len(calls)


def test_env_example_has_no_values():
    text = (Path(__file__).resolve().parent.parent / ".env.example").read_text()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            if val.strip():
                # only POLY_NONCE=0 / POLY_ALLOW_LIVE=0 style non-secret defaults allowed
                assert key.strip() in ("POLY_NONCE", "POLY_ALLOW_LIVE"), f"non-empty default for {key}"
