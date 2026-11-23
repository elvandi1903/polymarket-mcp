#!/usr/bin/env python3
"""Daemon PTB feed standalone: tulis ~/.config/polymarket-mcp/ptb.jsonl terus-menerus.

Feed = PolyBolt price.crypto.twap (BTCUSD, window 60s) dari modul ptb.py proyek
MCP. Jalankan persisten (background) agar consumer (paper trader / f0) selalu
punya PTB resmi per t_start. Hanya writer file; tidak menyentuh trading.
"""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from polymarket_mcp.config import Settings  # noqa: E402
from polymarket_mcp.ptb import PtbFeed  # noqa: E402


async def main() -> int:
    feed = PtbFeed(Settings.from_env())
    feed.start()
    print("ptb feed daemon start", flush=True)
    while True:
        await asyncio.sleep(30)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    sys.exit(0)
