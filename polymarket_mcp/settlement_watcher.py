"""Autonomous BTC 5m settlement poller, independent of any MCP client."""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any

from . import resolution

DEFAULT_OUTPUT = Path.home() / ".config" / "polymarket-mcp" / "settlements.jsonl"


class SettlementWatcher:
    def __init__(self, *, interval: int = 15, lookback: int = 12,
                 output: Path = DEFAULT_OUTPUT) -> None:
        self.interval = max(1, int(interval))
        self.lookback = max(1, min(int(lookback), 48))
        self.output = Path(output)
        self.seen: set[int] = set()
        self.stop_event = asyncio.Event()
        self.cycles = 0
        self.writes = 0
        self.errors = 0
        self._load_seen()

    def _load_seen(self) -> None:
        try:
            if not self.output.exists():
                return
            for line in self.output.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                record = json.loads(line)
                start = record.get("window_start")
                if type(start) is int:
                    self.seen.add(start)
        except (OSError, ValueError, TypeError):
            return

    def _write(self, result: dict[str, Any]) -> None:
        self.output.parent.mkdir(parents=True, exist_ok=True)
        record = {"observed_at": int(time.time()), **result}
        with self.output.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            handle.flush()
        self.writes += 1

    def _windows(self) -> list[int]:
        current = (int(time.time()) // 300) * 300
        return [current - i * 300 for i in range(self.lookback)]

    async def cycle(self) -> list[dict[str, Any]]:
        self.cycles += 1
        written: list[dict[str, Any]] = []
        for start in self._windows():
            if start in self.seen:
                continue
            try:
                result = await resolution.get_btc5m_resolution(
                    start, wait_seconds=0,
                )
            except Exception as exc:
                self.errors += 1
                print(f"settlement watcher error {start}: {type(exc).__name__}",
                      file=sys.stderr, flush=True)
                continue
            if result.get("state") == "resolved":
                self._write(result)
                self.seen.add(start)
                written.append(result)
                print(f"settlement resolved {result.get('slug')} "
                      f"winner={result.get('winner')}", flush=True)
            elif result.get("state") == "error":
                self.errors += 1
                print(f"settlement read failed {start}: "
                      f"{result.get('error')}", file=sys.stderr, flush=True)
        return written

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except (NotImplementedError, RuntimeError):
                pass
        print(f"settlement watcher started interval={self.interval}s "
              f"lookback={self.lookback} output={self.output}", flush=True)
        while not self.stop_event.is_set():
            await self.cycle()
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=self.interval,
                )
            except asyncio.TimeoutError:
                continue
        print("settlement watcher stopped", flush=True)
        return 0

    def status(self) -> dict[str, Any]:
        return {
            "output": str(self.output),
            "interval": self.interval,
            "lookback": self.lookback,
            "cycles": self.cycles,
            "writes": self.writes,
            "errors": self.errors,
            "seen": len(self.seen),
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="polymarket-settlement-watcher")
    parser.add_argument("--interval", type=int, default=15,
                        help="seconds between full polling cycles (default 15)")
    parser.add_argument("--lookback", type=int, default=12,
                        help="recent 5m windows to keep polling (default 12)")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT),
                        help="JSONL destination for settled windows")
    parser.add_argument("--once", action="store_true",
                        help="run one cycle and exit")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    watcher = SettlementWatcher(
        interval=args.interval,
        lookback=args.lookback,
        output=Path(args.output),
    )
    if args.once:
        asyncio.run(watcher.cycle())
        return 0
    return asyncio.run(watcher.run())
