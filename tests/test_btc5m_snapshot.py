import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from polymarket_mcp import server


class SnapshotTests(unittest.TestCase):
    def test_snapshot_is_one_call_with_round_ptb_books_tape_and_fills(self):
        market = SimpleNamespace(
            version=None,
            outcomes=SimpleNamespace(
                yes=SimpleNamespace(token_id="111", position_id=None),
                no=SimpleNamespace(token_id="222", position_id=None),
            ),
            position_ids=None,
        )
        pub = SimpleNamespace(
            get_market=lambda **kw: asyncio.sleep(0, result=market),
            close=lambda: asyncio.sleep(0),
        )
        boundaries = {
            "current_slug": "btc-updown-5m-1000",
            "next_slug": "btc-updown-5m-1300",
            "current_window_start": 1000,
            "next_window_start": 1300,
            "seconds_to_boundary": 240,
        }
        with patch("polymarket_mcp.round_streams._round_boundaries",
                   lambda: asyncio.sleep(0, result=boundaries)), \
             patch.object(server.clientmod, "build_public_client", lambda: pub), \
             patch.object(server, "get_ptb",
                          lambda start: asyncio.sleep(0, result={"t_start": start, "ptb": "100"})), \
             patch.object(server, "get_live_top_of_book",
                          lambda aid: asyncio.sleep(0, result={"asset_id": aid, "bid": "0.4", "ask": "0.5"})), \
             patch.object(server, "estimate_market_price",
                          lambda aid, side, shares: asyncio.sleep(
                              0, result={"asset_id": aid, "estimated_price": "0.52"})), \
             patch.object(server, "market_data_last_points",
                          lambda src, limit: asyncio.sleep(0, result={"source": src, "points": [src]})):
            result = asyncio.run(server.get_btc5m_snapshot(tape_limit=3))

        self.assertEqual(result["current_slug"], "btc-updown-5m-1000")
        self.assertEqual(result["ptb"]["ptb"], "100")
        self.assertEqual(result["assets"]["UP"]["asset_id"], "111")
        self.assertEqual(result["assets"]["DOWN"]["asset_id"], "222")
        self.assertEqual(result["books"]["UP"]["bid"], "0.4")
        self.assertEqual(result["fills"]["DOWN"]["estimated_price"], "0.52")
        self.assertEqual(result["tape"]["binance"], ["binance"])
        self.assertEqual(result["errors"], [])


if __name__ == "__main__":
    unittest.main()
