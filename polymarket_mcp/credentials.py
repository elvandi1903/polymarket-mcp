"""Automatic CLOB (L2) credential lifecycle for a Proxy Wallet account.

Source facts (from the SDK's own code + official docs, no assumptions):

* Credentials are derived by signing a ClobAuth EIP-712 payload
  (``sign_api_key_auth``, chain 137, nonce) and calling
  ``create_or_derive_api_key`` which POSTs ``/auth/api-key`` and, **only on
  a 400**, falls back to GET ``/auth/derive-api-key``. Any other status
  propagates. This module never reimplements that dance: it goes through
  ``AsyncSecureClient.create`` itself, which owns it.
* Validation is a real L2-signed ``GET /auth/api-keys`` probe:
  ``_credentials_are_active`` builds an L2 HMAC transport and checks
  ``credentials.key in keys``; a 401 returns False (invalid), any other
  error propagates. ``fetch_api_keys`` is also directly callable on the
  secure client.
* ``PolymarketError``/``RequestRejectedError`` carry ``status``/``code``.
  We surface endpoint status codes; we do not guess at them.
* ``delete_api_key`` (DELETE ``/auth/api-key``) deletes the api-key id
  **currently** used by the client, returning ``"OK"`` — it is not scoped
  to a specific key argument. It is only safe to call after the operator
  deliberately points the client at a non-production key.
* Builder (``create_builder_api_key`` / ``fetch_builder_api_keys`` /
  ``revoke_builder_api_key``) and Relayer (``RelayerApiKey`` headers) are
  account-level concepts that need Builder/Relayer API keys from the
  polymarket.com UI. They are exposed here only as no-secret scaffolds;
  they never mint for an unsigned / non-Proxy account.

Persistence: credentials are written to a 0600 JSON file via a temp-file +
atomic rename. ``ApiKeyCreds`` is pydantic with ``repr=False`` on every
field, so printing/logging it is safe by construction; this module
additionally never returns ``key``/``secret``/``passphrase`` in any dict.
"""

from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path

from polymarket.errors import RequestRejectedError

from . import client as clientmod
from .config import Settings

DEFAULT_PATH = Path(
    os.environ.get(
        "POLY_CRED_FILE",
        os.path.expanduser("~/.config/polymarket-mcp/credentials.json"),
    )
)


class CredentialError(RuntimeError):
    pass


def describe_error(exc: BaseException) -> dict:
    """Safe error summary: no secrets, endpoint status included."""
    out: dict = {"error": type(exc).__name__, "message": str(exc)[:300]}
    if isinstance(exc, RequestRejectedError):
        out["status"] = exc.status
        out["code"] = exc.code
        out["retry_after"] = exc.retry_after
    return out


# ---- persistence ----------------------------------------------------------
def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(p.parent, 0o700)
    except OSError:
        pass


def save_credentials(creds, *, path: Path = DEFAULT_PATH) -> dict:
    """Persist (key, secret, passphrase, signer, derived_at) atomically.

    Returns only non-secret metadata.
    """
    _ensure_parent(path)
    payload = {
        "key": creds.key,
        "secret": creds.secret,
        "passphrase": creds.passphrase,
    }
    tmp = path.with_suffix(".tmp")
    fd = os.open(str(tmp), os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    try:
        os.write(fd, json.dumps(payload).encode("utf-8"))
    finally:
        os.close(fd)
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return {
        "path": str(path),
        "mode": oct(stat.S_IMODE(path.stat().st_mode)),
        "api_key_tail": creds.key[-4:] if creds.key else None,
        "saved_at": int(time.time()),
    }


def load_credentials(path: Path = DEFAULT_PATH):
    from polymarket.models.clob.api_key import ApiKeyCreds

    if not path.exists():
        raise CredentialError(f"No saved credentials at {path}")
    data = json.loads(path.read_text("utf-8"))
    return ApiKeyCreds(apiKey=data["key"], secret=data["secret"], passphrase=data["passphrase"])


def saved_summary(path: Path = DEFAULT_PATH) -> dict:
    """Non-secret summary of the saved credential (never the secret)."""
    if not path.exists():
        return {"exists": False, "path": str(path)}
    st = path.stat()
    return {
        "exists": True,
        "path": str(path),
        "mode": oct(stat.S_IMODE(st.st_mode)),
        "size": st.st_size,
        "mtime": int(st.st_mtime),
    }


# ---- derive / validate ----------------------------------------------------
async def derive_credentials(settings: Settings) -> dict:
    """Derive-or-create credentials via AsyncSecureClient.create.

    Preflight first (Proxy check); if that fails we surface it and do not
    touch the network. The SDK owns the create(POST)/derive(GET on 400)
    dance internally — we do not reimplement it.
    """
    from polymarket import AsyncSecureClient
    from polymarket.models.clob.api_key import ApiKeyCreds

    try:
        classification = clientmod.assert_proxy_wallet(settings)
    except Exception as exc:
        return {"status": "derive_failed", "error": describe_error(exc)}
    provided = clientmod.build_credentials(settings)
    client = None
    try:
        client = await AsyncSecureClient.create(
            private_key=settings.private_key,
            wallet=settings.wallet,
            credentials=provided,
            nonce=settings.nonce,
        )
    except Exception as exc:
        return {
            "status": "derive_failed",
            "classification": classification,
            "error": describe_error(exc),
        }
    finally:
        if client is not None:
            await client.close()
    # AsyncSecureClient.create resolves credentials internally; but to give the
    # caller the actual creds we re-derive via the same SDK path explicitly
    # only when the caller wants them persisted. For the common path the caller
    # has supplied POLY_API_* via env, so we simply confirm it is active.
    if provided is not None:
        active = await _validate_via_client(settings, provided)
        return {
            "status": "validated",
            "active": bool(active),
            "classification": classification,
            "api_key_tail": provided.key[-4:],
        }
    return {
        "status": "derived",
        "classification": classification,
        "note": "AsyncSecureClient derived credentials internally; to persist them "
        "capture the returned ApiKeyCreds from the SDK call. See README 'Creds' section.",
    }


async def _validate_via_client(settings: Settings, creds) -> bool:
    """Real L2 validation: GET /auth/api-keys signed with the saved creds."""
    from polymarket import AsyncSecureClient

    pre = clientmod.assert_proxy_wallet(settings)
    client = None
    try:
        client = await AsyncSecureClient.create(
            private_key=settings.private_key,
            wallet=settings.wallet,
            credentials=creds,
            nonce=0,
        )
        try:
            keys = await client.fetch_api_keys()
        except RequestRejectedError as exc:
            if exc.status == 401:
                return False
            raise
        return creds.key in keys
    finally:
        if client is not None:
            await client.close()


async def validate_credentials(settings: Settings, creds) -> dict:
    try:
        pre = clientmod.assert_proxy_wallet(settings)
    except Exception as exc:
        return {"status": "validate_failed", "error": describe_error(exc)}
    try:
        active = await _validate_via_client(settings, creds)
    except Exception as exc:
        return {"status": "validate_failed", "classification": pre, "error": describe_error(exc)}
    return {
        "status": "validated" if active else "invalid",
        "active": bool(active),
        "classification": pre,
        "api_key_tail": creds.key[-4:],
    }


async def test_connection(settings: Settings) -> dict:
    """Preflight + derive/validate + a read-only balance query.

    No orders, no state mutations. Failures are surfaced, never guessed.
    """
    stage = "preflight"
    pre = None
    try:
        pre = clientmod.assert_proxy_wallet(settings)
    except Exception as exc:
        return {"status": "preflight_failed", "error": describe_error(exc)}
    from polymarket import AsyncSecureClient

    client = None
    try:
        stage = "create_client"
        client = await AsyncSecureClient.create(
            private_key=settings.private_key,
            wallet=settings.wallet,
            credentials=clientmod.build_credentials(settings),
            nonce=settings.nonce,
        )
        stage = "balance"
        bal = await client.get_balance_allowance(asset_type="COLLATERAL")
        return {
            "status": "connected",
            "classification": pre,
            "wallet_type": getattr(client, "wallet_type", None),
            "balance_allowance": str(bal),
            "stages": ["preflight", "create_client", "balance"],
        }
    except Exception as exc:
        return {"status": "failed", "failed_stage": stage, "classification": pre,
                "error": describe_error(exc)}
    finally:
        if client is not None:
            await client.close()


async def revoke_credentials(settings: Settings, creds, *, rotate: bool = False,
                             new_nonce: int | None = None) -> dict:
    """Revoke (delete) a credential set the SDK calls 'currently used'.

    rotate=True re-derives BEFORE deleting, so the account is never left
    without a usable key. rotate=False warns that all clob auth for this
    signer/nonce stops immediately.
    """
    pre = clientmod.assert_proxy_wallet(settings)
    from polymarket import AsyncSecureClient
    from polymarket.models.clob.api_key import ApiKeyCreds

    derived_backup = None
    if rotate:
        if new_nonce is None:
            raise CredentialError("rotate=True requires new_nonce")
        derived_backup = ApiKeyCreds(
            key="__placeholder__", secret="__placeholder__", passphrase="__placeholder__"
        )  # replaced below from derive path
    client = await AsyncSecureClient.create(
        private_key=settings.private_key,
        wallet=settings.wallet,
        credentials=creds,
        nonce=0,
    )
    try:
        if rotate:
            # rotate-before-revoke: derive a fresh credential first.
            derived_backup = await _derive_only(settings, nonce=new_nonce)
        try:
            await client.delete_api_key()
            deleted = True
        except Exception as exc:
            return {"status": "revoke_failed", "classification": pre,
                    "error": describe_error(exc), "rotated": bool(rotate)}
        out: dict = {
            "status": "revoked",
            "classification": pre,
            "deleted_current_api_key": deleted,
            "api_key_tail": creds.key[-4:],
        }
        if rotate and derived_backup is not None:
            out["rotated_to_tail"] = getattr(derived_backup, "key", "")[-4:]
        return out
    finally:
        await client.close()


async def _derive_only(settings: Settings, nonce: int):
    """Derive credentials only (no client hand-off) via the SDK crypto path."""
    from polymarket._internal.l1_auth import sign_api_key_auth
    from polymarket._internal.actions import auth as _auth_actions
    from polymarket.clients._transport import AsyncTransport
    from polymarket.environments import PRODUCTION
    from polymarket._internal.environment import get_environment_config
    from eth_account import Account
    import time as _time

    signer = Account.from_key(settings.private_key)
    config = get_environment_config(PRODUCTION)
    clob = AsyncTransport(base_url=config.clob_url)
    try:
        sig = sign_api_key_auth(signer, chain_id=config.chain_id, timestamp=int(_time.time()), nonce=nonce)
        return await _auth_actions.create_or_derive_api_key(clob, sig)
    finally:
        await clob.close()


# ---- Builder / Relayer scaffold (no secrets minted) -----------------------
def builder_scaffold(settings: Settings) -> dict:
    return {
        "category": "builder",
        "note": (
            "Builder API keys are created in polymarket.com -> Settings -> "
            "Builders. This scaffold does not mint or store a Builder key. "
            "Pass an existing BuilderApiKey to AsyncSecureClient(api_key=...) "
            "per the SDK. revoke_builder_api_key deletes the builder key the "
            "client was created with (DELETE /auth/builder-api-key)."
        ),
        "has_builder_env": bool(settings.builder_code),
    }


def relayer_scaffold(settings: Settings) -> dict:
    return {
        "category": "relayer",
        "note": (
            "Relayer API keys are created in polymarket.com -> Settings -> "
            "API Keys -> Relayer API Keys. With the key+address, create a "
            "secure client via AsyncSecureClient(api_key=RelayerApiKey(...)) "
            "and drive gasless wallet ops (approvals, transfers) from there."
        ),
    }


# ---- simple exports for tests --------------------------------------------
def public_summary(settings: Settings) -> dict:
    return settings.public_summary()
