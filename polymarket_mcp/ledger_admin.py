"""Operator CLI for BUY claims. Never exposed as an MCP tool.

Usage:
    python -m polymarket_mcp.ledger_admin resolve <round> <matched|no_fill>
    python -m polymarket_mcp.ledger_admin show
"""
from __future__ import annotations

import argparse
import json

from . import safety


def _ledger():
    from .observability import _state_dir
    from .round_guard import BuyLedger

    return BuyLedger(_state_dir() / "buy-ledger.sqlite3")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="polymarket-ledger-admin")
    sub = parser.add_subparsers(dest="command", required=True)
    resolve = sub.add_parser("resolve", help="finalize one pending round")
    resolve.add_argument("round", type=int)
    resolve.add_argument("status", choices=["matched", "no_fill"])
    sub.add_parser("show", help="print every claim")
    args = parser.parse_args(argv)

    ledger = _ledger()
    if args.command == "resolve":
        try:
            ledger.resolve(args.round, args.status)
        except (safety.ValidationError, FileNotFoundError, PermissionError) as exc:
            print(f"rejected: {type(exc).__name__ if not str(exc) else exc}")
            return 1
        print(f"resolved {args.round} as {args.status}")
        return 0

    rows = ledger.all_rows()
    print(json.dumps(rows, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
