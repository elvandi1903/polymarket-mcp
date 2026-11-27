#!/usr/bin/env python3
"""Read-only account check: wallet classification, creds, USDC balance, positions.

Tidak menempatkan order. Tidak memamerkan secret (hanya hasil, tidak kunci).
"""
import asyncio
import json
import sys

from polymarket_mcp.config import Settings


def load_settings():
    return Settings.from_env()
from polymarket_mcp import client as clientmod


def redact(x, keep=8):
    if not x:
        return None
    s = str(x)
    return s[:4] + "…" + s[-4:] if len(s) > keep + 8 else "***"


async def main() -> int:
    s = load_settings()
    print("=== config ===")
    print("private_key set:", bool(s.private_key))
    print("wallet (funder):", redact(s.wallet, 4) if s.wallet else "(tidak ada)")
    print("has_api_creds:", s.has_api_creds)
    print("allow_live:", s.allow_live)
    print("max_notional:", s.max_notional)

    # 1. Klasifikasi wallet offline (tanpa network)
    try:
        info = clientmod.classify_wallet(s)
        print("=== wallet classification (offline) ===")
        print(json.dumps({k: (redact(v, 4) if "wallet" in k or "signer" in k else v) for k, v in info.items()},
                          default=str, indent=2))
    except Exception as exc:
        print("classification gagal:", type(exc).__name__, str(exc)[:200])
        return 1

    # 2. Balance (read-only)
    pub = clientmod.build_public_client()
    try:
        print("=== balance / positions (read-only) ===")
        addr = s.wallet
        found = False
        for name in ("get_balances", "get_balance", "user_balance", "get_user_balance"):
            fn = getattr(pub, name, None)
            if fn is None:
                continue
            try:
                kw = {"user_address": addr} if "user" in name or "address" in str(fn.__doc__ or "") else {}
                resp = await fn(addr) if not kw else await fn(**kw)
                print(f"{name} ->", json.dumps(_safe(resp), default=str)[:400])
                found = True
                break
            except Exception as exc:
                print(f"{name} gagal ({type(exc).__name__}):", str(exc)[:150])
        if not found:
            # coba method apa saja yang mengandung 'balance'
            cands = [m for m in dir(pub) if "balance" in m.lower() or "position" in m.lower() or "asset" in m.lower()]
            print("method kandidat di public client:", cands)
    finally:
        try:
            await pub.close()
        except Exception:
            pass
    print("OK")
    return 0


def _safe(obj):
    try:
        return json.loads(json.dumps(obj, default=str))
    except Exception:
        return str(obj)[:400]


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
