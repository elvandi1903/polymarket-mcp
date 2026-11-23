"""Client construction + read-only Proxy-Wallet preflight.

The unified SDK does NOT take signature_type as input. It infers the wallet
type by re-deriving CREATE2 addresses from (signer, wallet). If the wallet
does not match any derived address, classify_account falls back to
SESSION_KEY/DEPOSIT_WALLET (signature_type 3), which would sign orders for
the wrong maker. That silent-misclassification is the single most dangerous
failure mode for a legacy Magic/email Proxy Wallet, so it is checked here
before any authenticated call.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from polymarket import AsyncPublicClient, AsyncSecureClient
from polymarket.environments import PRODUCTION
from polymarket.models.clob.api_key import ApiKeyCreds

from .config import Settings

if TYPE_CHECKING:
    pass


class PreflightError(RuntimeError):
    pass


def build_credentials(settings: Settings) -> ApiKeyCreds | None:
    if settings.has_api_creds:
        return ApiKeyCreds(
            key=settings.api_key,
            secret=settings.api_secret,
            passphrase=settings.api_passphrase,
        )
    return None


def signer_address_of(settings: Settings) -> str | None:
    if not settings.private_key:
        return None
    try:
        from eth_account import Account

        return Account.from_key(settings.private_key).address
    except Exception as exc:  # keep generic: never echo the key
        raise PreflightError(f"Invalid POLY_PRIVATE_KEY ({type(exc).__name__})") from exc


def classify_wallet(settings: Settings) -> dict:
    """Offline CREATE2 classification. No network, no funds touched.

    Returns dict with signer, derived/funder, wallet_type, signer_type,
    signature_type. The SDK performs the identical classification at client
    creation; this mirrors it so we surface the result to the user.
    """
    from polymarket._internal.environment import get_environment_config
    from polymarket._internal.wallet import classify_account, signature_type_for

    signer = signer_address_of(settings)
    if signer is None:
        raise PreflightError("POLY_PRIVATE_KEY is not set")
    if not settings.wallet:
        raise PreflightError("POLY_WALLET (funder/proxy address) is not set")

    config = get_environment_config(PRODUCTION).wallet_derivation
    account = classify_account(signer=signer, wallet=settings.wallet, config=config)
    return {
        "signer": signer,
        "funder_wallet": settings.wallet,
        "wallet_type": account.wallet_type,
        "signer_type": account.signer_type,
        "signature_type": signature_type_for(account.wallet_type),
        "is_proxy": account.wallet_type == "POLY_PROXY",
    }


def assert_proxy_wallet(settings: Settings) -> dict:
    """Abort loudly unless the funder is classified as a Proxy Wallet.

    Raises PreflightError if classification != POLY_PROXY. This is the guard
    that prevents signature_type=3 mis-signing for a legacy Proxy account.
    """
    info = classify_wallet(settings)
    if not info["is_proxy"]:
        raise PreflightError(
            "POLY_WALLET is not classified as POLY_PROXY "
            f"(got wallet_type={info['wallet_type']}, signature_type={info['signature_type']}). "
            "Refusing to proceed: using this wallet would sign orders for the wrong maker. "
            "Check that POLY_WALLET is the proxy/funder address (polymarket.com > Profile > "
            "Deposit address) and that POLY_PRIVATE_KEY matches that account's signer."
        )
    return info


def build_public_client() -> AsyncPublicClient:
    return AsyncPublicClient(PRODUCTION)


async def build_secure_client(settings: Settings) -> AsyncSecureClient:
    """Create a secure client and verify it settled on POLY_PROXY.

    Raises PreflightError if the live client is not a Proxy wallet.
    """
    info = assert_proxy_wallet(settings)
    creds = build_credentials(settings)
    api_key = None
    if settings.relayer_api_key and settings.relayer_address:
        from polymarket.auth import RelayerApiKey

        api_key = RelayerApiKey(key=settings.relayer_api_key,
                                address=settings.relayer_address)
    client = await AsyncSecureClient.create(
        private_key=settings.private_key,
        wallet=settings.wallet,
        credentials=creds,
        api_key=api_key,
        nonce=settings.nonce,
    )
    try:
        live_type = getattr(client, "wallet_type", None)
        if live_type != info["wallet_type"]:
            raise PreflightError(
                f"SDK resolved wallet_type={live_type!r} but preflight expected "
                f"{info['wallet_type']!r}; refusing to hand this client to trading."
            )
    except BaseException:
        await client.close()
        raise
    return client
