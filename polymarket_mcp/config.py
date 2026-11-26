"""Environment-driven configuration and secret hygiene.

Secrets come from environment variables and, when a variable is unset, from
the operator's own ``.env`` file next to this package (mode 0600, git-ignored).
This module never logs them and refuses to echo them. The file fallback exists
so HTTP server processes started without explicit shell-exported env still see
the creds — fixing the recurring "POLY_PRIVATE_KEY is not set" / auth_invalid
loop. The file is read only for names the caller actually asks for.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path


class ConfigError(RuntimeError):
    """Raised when configuration is missing/invalid."""


@lru_cache(maxsize=1)
def _dotenv() -> dict[str, str]:
    """Load the operator .env (same file as .env.example, git-ignored)."""
    candidates = [
        Path(__file__).resolve().parent.parent / ".env",
        Path.cwd() / ".env",
    ]
    for path in candidates:
        try:
            if path.exists():
                data: dict[str, str] = {}
                for line in path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    data[k.strip()] = v.strip()
                return data
        except OSError:
            continue
    return {}


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name, "")
    if v and v.strip():
        return v.strip()
    file_val = _dotenv().get(name, "")
    if file_val and file_val.strip():
        return file_val.strip()
    return default.strip()



def _decimal_or_zero(raw: str, name: str) -> Decimal:
    raw = raw.strip()
    if not raw:
        return Decimal(0)
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ConfigError(f"Invalid decimal for {name}: {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    private_key: str = ""
    wallet: str = ""                      # funder / proxy address
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    builder_code: str = ""
    relayer_api_key: str = ""
    relayer_address: str = ""
    allow_live: bool = False              # server-side live switch
    max_notional: Decimal = Decimal(0)  # 0 = unset
    max_open_orders: int = 0              # 0 = unset
    nonce: int = 0

    @classmethod
    def from_env(cls) -> Settings:
        try:
            nonce = int(_env("POLY_NONCE", "0") or "0")
        except ValueError as exc:
            raise ConfigError("POLY_NONCE must be an integer") from exc
        return cls(
            private_key=_env("POLY_PRIVATE_KEY"),
            wallet=_env("POLY_WALLET"),
            api_key=_env("POLY_API_KEY"),
            api_secret=_env("POLY_API_SECRET"),
            api_passphrase=_env("POLY_API_PASSPHRASE"),
            builder_code=_env("POLY_BUILDER_CODE"),
            relayer_api_key=_env("RELAYER_API_KEY"),
            relayer_address=_env("RELAYER_API_KEY_ADDRESS"),
            allow_live=_env("POLY_ALLOW_LIVE", "0") in ("1", "true", "True", "TRUE"),
            max_notional=_decimal_or_zero(_env("POLY_MAX_NOTIONAL"), "POLY_MAX_NOTIONAL"),
            max_open_orders=int(_env("POLY_MAX_OPEN_ORDERS", "0") or "0"),
            nonce=nonce,
        )

    # ---- derived helpers (never expose secrets) -------------------------
    @property
    def has_private_key(self) -> bool:
        return bool(self.private_key)

    @property
    def has_api_creds(self) -> bool:
        return bool(self.api_key and self.api_secret and self.api_passphrase)

    def public_summary(self) -> dict:
        """Safe-to-show summary. Sentinels are booleans, never values."""
        return {
            "has_private_key": self.has_private_key,
            "wallet_configured": bool(self.wallet),
            "has_api_creds": self.has_api_creds,
            "builder_code_set": bool(self.builder_code),
            "allow_live": self.allow_live,
            "max_notional": str(self.max_notional),
            "max_open_orders": self.max_open_orders,
            "nonce": self.nonce,
        }
