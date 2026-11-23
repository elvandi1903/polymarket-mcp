"""Offline validation of round rollover + user stream tools (no network/keys)."""
import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import patch

from polymarket_mcp import round_streams


class FakeStream:
    def snapshot_dict(self):
        return {"asset_id": "111", "book_snapshot": {"bids": [], "asks": []}}


class FakeManager:
    def __init__(self):
        self.subscribed = []
        self.calls = []
        self._streams = {}
        self._client = object()
        self.cfg = {"prefetch_lead_s": 30.0}

    async def subscribe(self, asset_id, slug="", window_start=0):
        self.calls.append("subscribe")
        self.subscribed.append((asset_id, slug, window_start))
        return FakeStream()

    async def _rest_fallback(self, st):
        self.calls.append("rest")

    def status_snapshot(self):
        return {"assets": {}, "count": len(self.subscribed)}


class FakePublic:
    async def get_market(self, *, slug):
        return NS(slug=slug)

    async def close(self):
        return None


class FakeHandle:
    def __init__(self, events):
        self.events = events
        self.closed = False

    def __aiter__(self):
        async def gen():
            for event in self.events:
                yield event
                await asyncio.sleep(0)
        return gen()

    async def close(self):
        self.closed = True


class FakeSecure:
    def __init__(self, handle):
        self.handle = handle
        self.closed = False

    async def subscribe(self, spec):
        self.spec = spec
        return self.handle

    async def close(self):
        self.closed = True


def _async_return(value):
    async def build(settings):
        return value
    return build


def test_subscribe_round_starts_rollover_and_status():
    manager = FakeManager()

    async def go():
        with patch.object(round_streams, "get_manager", lambda: manager), \
             patch.object(round_streams, "boundaries", lambda now: (
                 "btc-updown-5m-100", "btc-updown-5m-200", 100, 200)), \
             patch.object(round_streams.clientmod, "build_public_client", lambda: FakePublic()), \
             patch("polymarket_mcp.server._resolve_asset_id", lambda market, outcome: ("111" if outcome == "UP" else "222", "fake")):
            out = await round_streams.subscribe_round()
            assert out["status"] == "subscribed"
            assert out["slug"] == "btc-updown-5m-100"
            assert out["asset_ids"] == ["111", "222"]
            assert out["outcomes"] == ["UP", "DOWN"]
            assert out["auto_rollover"] is True
            assert out["rest_bootstrap_first"] is True
            assert out["boundaries"]["next_slug"] == "btc-updown-5m-200"
            assert manager.subscribed == [
                ("111", "btc-updown-5m-100", 100),
                ("222", "btc-updown-5m-100", 100),
            ]
            assert manager.calls == ["rest", "subscribe", "rest", "subscribe"]
            status = await round_streams.round_status()
            assert status["auto_rollover"] is True
            stopped = await round_streams.stop_round_watch()
            assert stopped["status"] == "stopped"
            status = await round_streams.round_status()
            assert status["auto_rollover"] is False
    asyncio.new_event_loop().run_until_complete(go())


def test_rollover_prefetches_next_round_with_rest_before_ws():
    from polymarket_mcp.live import RolloverWatcher

    manager = FakeManager()
    manager._client = FakePublic()

    async def go():
        watcher = RolloverWatcher(manager)
        with patch("polymarket_mcp.live.boundaries", lambda now: (
            "btc-updown-5m-100", "btc-updown-5m-200", 100, 200)), \
             patch("polymarket_mcp.live._resolve_asset_id", lambda market, outcome: ("222" if outcome == "UP" else "333", "fake")), \
             patch("polymarket_mcp.live.time.time", lambda: 380):
            result = await watcher.tick_once()
        assert result["prefetch_due"] is True
        assert result["prefetched"] is True
        assert result["rest_bootstrap_first"] is True
        assert result["next_asset_ids"] == ["222", "333"]
        assert manager.calls == ["rest", "subscribe", "rest", "subscribe"]
        assert manager.subscribed == [
            ("222", "btc-updown-5m-200", 200),
            ("333", "btc-updown-5m-200", 200),
        ]

    asyncio.new_event_loop().run_until_complete(go())


def test_user_stream_buffers_order_and_trade_events():
    events = [
        {"topic": "user", "type": "order", "payload": {"id": "0xa", "status": "LIVE"}},
        {"topic": "user", "type": "trade", "payload": {"id": "0xb", "size_matched": "5"}},
    ]
    handle = FakeHandle(events)
    secure = FakeSecure(handle)

    async def go():
        with patch.object(round_streams, "Settings", NS(from_env=lambda: object())), \
             patch.object(round_streams.clientmod, "build_secure_client", _async_return(secure)):
            started = await round_streams.start_user_stream()
            assert started["status"] == "started" and started["topic"] == "user"
            for _ in range(200):
                status = await round_streams.user_stream_status()
                if status["events"] >= 2:
                    break
                await asyncio.sleep(0.01)
            assert status["events"] >= 2
            out = await round_streams.user_stream_events(10)
            assert [e["type"] for e in out["events"]][-2:] == ["order", "trade"]
            assert "POLY_PRIVATE_KEY" not in json.dumps(out)
            stopped = await round_streams.stop_user_stream()
            assert stopped["status"] == "stopped"
            assert handle.closed is True and secure.closed is True
    asyncio.new_event_loop().run_until_complete(go())


def test_user_stream_error_is_reported_not_silently_dropped():
    class FailingHandle:
        def __aiter__(self):
            async def gen():
                raise RuntimeError("stream broke")
                yield
            return gen()

        def close(self):
            return None

    secure = FakeSecure(FailingHandle())

    async def go():
        with patch.object(round_streams, "Settings", NS(from_env=lambda: object())), \
             patch.object(round_streams.clientmod, "build_secure_client", _async_return(secure)):
            await round_streams.start_user_stream()
            for _ in range(200):
                status = await round_streams.user_stream_status()
                if status["error"]:
                    break
                await asyncio.sleep(0.01)
            assert status["error"] and "stream broke" in status["error"]
            await round_streams.stop_user_stream()
    asyncio.new_event_loop().run_until_complete(go())
