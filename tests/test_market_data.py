"""Live Market Data Engine unit tests (offline, no network)."""
from decimal import Decimal

from polymarket_mcp.market_data import FeedState, LiveMarketDataEngine, SpotPoint


def test_parse_binance_trade():
    raw = '{"e":"trade","s":"BTCUSDT","T":1794049800341,"p":"76008.14000000","q":"0.1","U":123,"u":124}'
    pt = LiveMarketDataEngine.parse_binance_trade(raw)
    assert pt is not None
    assert pt.source == "binance"
    assert pt.price == Decimal("76008.14")
    assert pt.timestamp_ms == 1794049800341
    assert pt.sequence == 124


def test_parse_coinbase_ticker():
    raw = '{"type":"ticker","sequence":138962385025,"product_id":"BTC-USD","price":"75925.13","time":"2026-11-07T11:08:48.962783Z","trade_id":1116755995}'
    pt = LiveMarketDataEngine.parse_coinbase_ticker(raw)
    assert pt is not None
    assert pt.source == "coinbase"
    assert pt.price == Decimal("75925.13")
    assert pt.sequence == 138962385025
    assert pt.timestamp_ms > 0


def test_parse_rejects_garbage():
    assert LiveMarketDataEngine.parse_binance_trade('{"e":"depthUpdate"}') is None
    assert LiveMarketDataEngine.parse_binance_trade('not json') is None
    assert LiveMarketDataEngine.parse_coinbase_ticker('{"type":"subscriptions"}') is None


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


def test_ingest_marks_age_and_gap_and_outlier():
    now = _now_ms()
    feed = FeedState("binance", "BTC/USDT")
    p1 = feed.ingest(SpotPoint("binance", "BTC/USDT", Decimal(100), now))
    assert p1.age_ms >= 0 and p1.is_stale is False
    # gap: second point 5000ms later (event time jumps forward)
    p2 = feed.ingest(SpotPoint("binance", "BTC/USDT", Decimal(100), now + 5000))
    assert p2.is_gap is True
    # outlier: +2% move
    feed2 = FeedState("binance", "BTC/USDT")
    feed2.ingest(SpotPoint("binance", "BTC/USDT", Decimal(100), now))
    p3 = feed2.ingest(SpotPoint("binance", "BTC/USDT", Decimal(102), now + 1))
    assert p3.is_outlier is True
    # non-positive price
    p4 = feed2.ingest(SpotPoint("binance", "BTC/USDT", Decimal(0), now + 2))
    assert p4.is_outlier is True


def test_seq_regression_counts_as_gap():
    feed = FeedState("coinbase", "BTC/USD")
    feed.ingest(SpotPoint("coinbase", "BTC/USD", Decimal(100), 1000, sequence=10))
    p = feed.ingest(SpotPoint("coinbase", "BTC/USD", Decimal(100), 1001, sequence=9))
    assert p.is_gap is True


def test_snapshot_structure():
    feed = FeedState("binance", "BTC/USDT")
    feed.ingest(SpotPoint("binance", "BTC/USDT", Decimal(100), 1000))
    snap = feed.snapshot()
    assert snap["connected"] is True
    assert snap["last_price"] == "100"
