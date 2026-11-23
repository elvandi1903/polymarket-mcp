import asyncio
import json
import time

from polymarket_mcp import resolution, settlement_watcher


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _fake_reader(results):
    async def read(window_start, wait_seconds=0):
        value = results.get(window_start, results.get("default"))
        if isinstance(value, list):
            return value.pop(0) if value else results["default"]
        return value
    return read


def test_watcher_polls_until_resolved_and_writes_once(tmp_path, monkeypatch):
    out = tmp_path / "settlements.jsonl"
    current = (int(time.time()) // 300) * 300
    pending = {"state": "pending", "slug": f"btc-updown-5m-{current}",
               "window_start": current}
    resolved = {"state": "resolved", "slug": f"btc-updown-5m-{current}",
                "window_start": current, "winner": "Up",
                "outcomes": [{"outcome": "Up", "resolved": True,
                              "final_price": 1.0}]}
    monkeypatch.setattr(
        resolution, "get_btc5m_resolution",
        _fake_reader({current: [pending, resolved, resolved]}),
    )
    watcher = settlement_watcher.SettlementWatcher(
        interval=1, lookback=1, output=out,
    )

    assert _run(watcher.cycle()) == []
    assert not out.exists()
    written = _run(watcher.cycle())
    assert len(written) == 1
    lines = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(lines) == 1
    assert lines[0]["winner"] == "Up"
    assert lines[0]["observed_at"] >= current
    assert _run(watcher.cycle()) == []
    assert len(out.read_text().splitlines()) == 1
    assert watcher.status()["writes"] == 1


def test_watcher_is_idempotent_on_restart(tmp_path, monkeypatch):
    out = tmp_path / "settlements.jsonl"
    current = (int(time.time()) // 300) * 300
    existing = {"observed_at": current, "state": "resolved",
                "window_start": current, "winner": "Down"}
    out.write_text(json.dumps(existing) + "\n")
    monkeypatch.setattr(
        resolution, "get_btc5m_resolution",
        _fake_reader({"default": {"state": "resolved",
                                  "window_start": current, "winner": "Down"}}),
    )
    watcher = settlement_watcher.SettlementWatcher(
        interval=1, lookback=1, output=out,
    )
    assert watcher.status()["seen"] == 1
    assert _run(watcher.cycle()) == []
    assert len(out.read_text().splitlines()) == 1
