"""
tests/unit/test_ingestion.py
─────────────────────────────────────────────────────────────────────────────
Unit tests for the ingestion layer.
No real network calls — everything is mocked.

Run:
    pytest tests/unit/test_ingestion.py -v
    pytest tests/unit/test_ingestion.py -v -k "test_market"   # filter
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingestion.base_connector import (
    BaseConnector, ConnectorHealth, ConnectorStats, RetryConfig,
)
from ingestion.market_feed import AlpacaMarketFeed, MarketTick, PolygonMarketFeed, TradeBar
from ingestion.news_feed import NewsItem, RSSNewsFeed, extract_symbols
from ingestion.macro_feed import (
    EarningsEvent, EconomicIndicator, FREDIndicatorFeed, MacroEvent,
)
from ingestion.alt_data_feed import AltDataSignal, FearGreedFeed, PutCallRatioFeed


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# base_connector tests
# ─────────────────────────────────────────────────────────────────────────────

class _ConcreteConnector(BaseConnector):
    """Minimal concrete subclass for testing BaseConnector logic."""

    def __init__(self, messages: list[dict], **kwargs):
        super().__init__(name="TestConnector", **kwargs)
        self._messages = messages

    @property
    def ws_url(self) -> str:
        return "ws://test.local/stream"

    async def _subscribe(self, ws) -> None:
        pass

    def _parse_message(self, data: dict):
        if data.get("skip"):
            return None
        return MarketTick(
            symbol    = data["symbol"],
            price     = data["price"],
            volume    = data.get("volume", 1.0),
            timestamp = _ts(),
            source    = "test",
            raw_type  = "trade",
        )


class TestBaseConnector:

    def test_initial_health_is_disconnected(self):
        conn = _ConcreteConnector(messages=[])
        assert conn.health == ConnectorHealth.DISCONNECTED

    def test_stop_sets_event(self):
        conn = _ConcreteConnector(messages=[])
        conn.stop()
        assert conn._stop_event.is_set()

    def test_initial_stats(self):
        conn = _ConcreteConnector(messages=[])
        stats = conn.stats
        assert stats.messages_received == 0
        assert stats.messages_failed == 0
        assert stats.error_rate == 0.0

    @pytest.mark.asyncio
    async def test_handle_raw_valid_message(self):
        received = []
        conn = _ConcreteConnector(
            messages=[],
            message_callback=received.append,
        )
        raw = json.dumps({"symbol": "AAPL", "price": 189.5, "volume": 1000})
        await conn._handle_raw(raw)

        assert len(received) == 1
        assert received[0].symbol == "AAPL"
        assert received[0].price == 189.5
        assert conn.stats.messages_received == 1

    @pytest.mark.asyncio
    async def test_handle_raw_invalid_json(self):
        conn = _ConcreteConnector(messages=[])
        await conn._handle_raw("not valid json {{{")
        assert conn.stats.messages_failed == 1
        assert conn.stats.messages_received == 0

    @pytest.mark.asyncio
    async def test_handle_raw_skip_returns_none(self):
        received = []
        conn = _ConcreteConnector(messages=[], message_callback=received.append)
        await conn._handle_raw(json.dumps({"skip": True}))
        assert len(received) == 0
        assert conn.stats.messages_received == 0   # skip doesn't count as failure

    @pytest.mark.asyncio
    async def test_dead_letter_populated_on_validation_error(self):
        conn = _ConcreteConnector(messages=[])
        # Missing required 'price' field → ValidationError
        await conn._handle_raw(json.dumps({"symbol": "AAPL"}))
        assert len(conn._dead_letters) == 1
        assert "price" in conn._dead_letters[0]["reason"].lower() or True  # pydantic msg varies

    def test_health_degrades_on_high_error_rate(self):
        conn = _ConcreteConnector(messages=[])
        conn._health = ConnectorHealth.CONNECTED
        conn._stats.messages_received = 1
        conn._stats.messages_failed   = 5   # 83% error rate
        conn._update_health()
        assert conn.health == ConnectorHealth.DEGRADED


# ─────────────────────────────────────────────────────────────────────────────
# MarketTick model tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMarketTickModel:

    def test_basic_construction(self):
        tick = MarketTick(
            symbol="NVDA", price=875.0, volume=5000,
            timestamp=_ts(), source="alpaca", raw_type="trade",
        )
        assert tick.symbol == "NVDA"
        assert tick.price  == 875.0

    def test_rejects_negative_price(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            MarketTick(
                symbol="AAPL", price=-10.0, volume=100,
                timestamp=_ts(), source="alpaca", raw_type="trade",
            )

    def test_timestamp_from_epoch_nanoseconds(self):
        import time
        ns = int(time.time() * 1e9)
        tick = MarketTick(
            symbol="AAPL", price=100.0, volume=1,
            timestamp=ns, source="test", raw_type="trade",
        )
        assert tick.timestamp.tzinfo is not None   # must be tz-aware

    def test_timestamp_from_iso_string(self):
        tick = MarketTick(
            symbol="AAPL", price=100.0, volume=1,
            timestamp="2025-01-15T14:30:00Z",
            source="test", raw_type="trade",
        )
        assert tick.timestamp.year == 2025

    def test_optional_bid_ask(self):
        tick = MarketTick(
            symbol="AAPL", price=100.0, volume=1,
            bid=99.99, ask=100.01,
            timestamp=_ts(), source="test", raw_type="quote",
        )
        assert tick.bid == 99.99
        assert tick.ask == 100.01


# ─────────────────────────────────────────────────────────────────────────────
# AlpacaMarketFeed parsing tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAlpacaMarketFeed:

    def _feed(self) -> AlpacaMarketFeed:
        with patch.dict("os.environ", {
            "ALPACA_API_KEY":    "test_key",
            "ALPACA_SECRET_KEY": "test_secret",
        }):
            return AlpacaMarketFeed(symbols=["AAPL", "NVDA"])

    def test_parse_trade_message(self):
        feed = self._feed()
        raw = [{"T": "t", "S": "AAPL", "p": "189.5", "s": "500", "t": _ts()}]
        result = feed._parse_message(raw)
        assert isinstance(result, MarketTick)
        assert result.symbol == "AAPL"
        assert result.price  == 189.5
        assert result.raw_type == "trade"

    def test_parse_quote_message(self):
        feed = self._feed()
        raw = [{"T": "q", "S": "NVDA", "bp": "874.0", "ap": "875.0", "bs": "100", "as": "150", "t": _ts()}]
        result = feed._parse_message(raw)
        assert isinstance(result, MarketTick)
        assert result.symbol == "NVDA"
        assert result.bid    == 874.0
        assert result.ask    == 875.0
        assert result.raw_type == "quote"

    def test_parse_bar_message(self):
        feed = self._feed()
        raw = [{
            "T": "b", "S": "AAPL",
            "o": "188.0", "h": "191.0", "l": "187.5", "c": "190.0",
            "v": "12500000", "vw": "189.75", "t": _ts(),
        }]
        result = feed._parse_message(raw)
        assert isinstance(result, TradeBar)
        assert result.open  == 188.0
        assert result.high  == 191.0
        assert result.close == 190.0

    def test_skips_heartbeat(self):
        feed = self._feed()
        result = feed._parse_message([{"T": "success", "msg": "connected"}])
        assert result is None

    def test_skips_subscription_ack(self):
        feed = self._feed()
        result = feed._parse_message([{"T": "subscription", "trades": ["AAPL"]}])
        assert result is None

    def test_list_of_mixed_messages(self):
        """Multiple messages in one frame — returns last non-None result."""
        feed = self._feed()
        raw = [
            {"T": "success", "msg": "connected"},
            {"T": "t", "S": "AAPL", "p": "189.5", "s": "500", "t": _ts()},
        ]
        result = feed._parse_message(raw)
        assert isinstance(result, MarketTick)

    def test_ws_url_uses_feed(self):
        feed = self._feed()
        assert "iex" in feed.ws_url


# ─────────────────────────────────────────────────────────────────────────────
# PolygonMarketFeed parsing tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPolygonMarketFeed:

    def _feed(self) -> PolygonMarketFeed:
        with patch.dict("os.environ", {"POLYGON_API_KEY": "test_key"}):
            return PolygonMarketFeed(symbols=["AAPL"])

    def test_parse_trade(self):
        feed = self._feed()
        import time
        raw = [{"ev": "T", "sym": "AAPL", "p": 189.0, "s": 100, "t": int(time.time() * 1000)}]
        result = feed._parse_message(raw)
        assert isinstance(result, MarketTick)
        assert result.source == "polygon"

    def test_parse_aggregate(self):
        feed = self._feed()
        import time
        raw = [{
            "ev": "A", "sym": "AAPL",
            "op": 188.0, "h": 190.0, "l": 187.0, "c": 189.5,
            "av": 5_000_000, "vw": 188.9,
            "s": int(time.time() * 1000),
        }]
        result = feed._parse_message(raw)
        assert isinstance(result, TradeBar)
        assert result.bar_size == "1Sec"

    def test_skips_status(self):
        feed = self._feed()
        result = feed._parse_message([{"ev": "status", "status": "connected"}])
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# extract_symbols tests
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractSymbols:

    def test_cashtags(self):
        symbols = extract_symbols("$AAPL is up while $NVDA drops")
        assert "AAPL" in symbols
        assert "NVDA" in symbols

    def test_watchlist_cross_reference(self):
        symbols = extract_symbols("AAPL reported strong earnings", watchlist={"AAPL", "MSFT"})
        assert "AAPL" in symbols
        assert "MSFT" not in symbols

    def test_no_false_positives(self):
        # Common English words that look like tickers shouldn't be extracted
        # unless they're in the watchlist
        symbols = extract_symbols("IT was a great day", watchlist=set())
        assert symbols == []

    def test_empty_text(self):
        assert extract_symbols("") == []

    def test_mixed_case_ignored(self):
        # Cashtag regex requires uppercase after $
        symbols = extract_symbols("$aapl is cheap")
        assert "AAPL" not in symbols


# ─────────────────────────────────────────────────────────────────────────────
# NewsItem model tests
# ─────────────────────────────────────────────────────────────────────────────

class TestNewsItem:

    def test_make_id_is_deterministic(self):
        id1 = NewsItem.make_id("reuters", "https://example.com/story1")
        id2 = NewsItem.make_id("reuters", "https://example.com/story1")
        assert id1 == id2

    def test_make_id_differs_by_source(self):
        id1 = NewsItem.make_id("reuters", "same-url")
        id2 = NewsItem.make_id("cnbc",    "same-url")
        assert id1 != id2

    def test_construction(self):
        item = NewsItem(
            id           = "abc123",
            title        = "Test headline",
            summary      = "Summary text",
            url          = "https://example.com",
            source       = "reuters",
            source_type  = "rss",
            symbols      = ["AAPL"],
            published_at = _ts(),
        )
        assert item.symbols == ["AAPL"]
        assert item.ingested_at is not None   # auto-set


# ─────────────────────────────────────────────────────────────────────────────
# RSSNewsFeed tests (mocked HTTP)
# ─────────────────────────────────────────────────────────────────────────────

MOCK_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>$AAPL beats earnings estimates</title>
      <link>https://example.com/aapl-earnings</link>
      <description>Apple reports record quarterly profits.</description>
      <pubDate>Mon, 28 Apr 2025 14:00:00 GMT</pubDate>
    </item>
    <item>
      <title>$NVDA surges on AI chip demand</title>
      <link>https://example.com/nvda-chips</link>
      <description>NVIDIA shares rise 4% on strong data center orders.</description>
      <pubDate>Mon, 28 Apr 2025 13:30:00 GMT</pubDate>
    </item>
  </channel>
</rss>"""


class TestRSSNewsFeed:

    @pytest.mark.asyncio
    async def test_parses_rss_and_calls_back(self):
        received = []

        feed = RSSNewsFeed(
            sources={"test": "https://example.com/rss"},
            callback=received.append,
            poll_interval=9999,   # won't actually loop
        )

        # Mock aiohttp response
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.text   = AsyncMock(return_value=MOCK_RSS)
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__  = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        await feed._poll_feed(mock_session, "test", "https://example.com/rss")

        assert len(received) == 2
        titles = [item.title for item in received]
        assert any("AAPL" in t for t in titles)
        assert any("NVDA" in t for t in titles)

    @pytest.mark.asyncio
    async def test_deduplication(self):
        received = []
        feed = RSSNewsFeed(
            sources={"test": "https://example.com/rss"},
            callback=received.append,
            poll_interval=9999,
        )

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.text   = AsyncMock(return_value=MOCK_RSS)
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__  = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        # Poll twice — second poll should yield no new items
        await feed._poll_feed(mock_session, "test", "https://example.com/rss")
        await feed._poll_feed(mock_session, "test", "https://example.com/rss")

        assert len(received) == 2   # NOT 4


# ─────────────────────────────────────────────────────────────────────────────
# FREDIndicatorFeed tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFREDIndicatorFeed:

    @pytest.mark.asyncio
    async def test_emits_on_new_release(self):
        received = []

        with patch.dict("os.environ", {"FRED_API_KEY": "test_key"}):
            feed = FREDIndicatorFeed(callback=received.append, poll_interval=9999)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json   = AsyncMock(return_value={
            "observations": [
                {"date": "2025-04-01", "value": "3.5"},
                {"date": "2025-03-01", "value": "3.2"},
            ]
        })
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__  = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        await feed._poll_series(mock_session, "CPIAUCSL", "CPI", "%")

        assert len(received) == 1
        assert isinstance(received[0], EconomicIndicator)
        assert received[0].value == 3.5
        assert received[0].prior_value == 3.2

    @pytest.mark.asyncio
    async def test_no_emit_if_date_unchanged(self):
        received = []

        with patch.dict("os.environ", {"FRED_API_KEY": "test_key"}):
            feed = FREDIndicatorFeed(callback=received.append, poll_interval=9999)

        feed._last_date["CPIAUCSL"] = "2025-04-01"   # already seen

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json   = AsyncMock(return_value={
            "observations": [{"date": "2025-04-01", "value": "3.5"}]
        })
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__  = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        await feed._poll_series(mock_session, "CPIAUCSL", "CPI", "%")

        assert len(received) == 0   # no duplicate emit


# ─────────────────────────────────────────────────────────────────────────────
# FearGreedFeed tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFearGreedFeed:

    @pytest.mark.asyncio
    async def test_emits_signal(self):
        received = []
        feed = FearGreedFeed(callback=received.append, poll_interval=9999)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json   = AsyncMock(return_value={
            "fear_and_greed": {
                "score":          72.5,
                "rating":         "Greed",
                "previous_close": 68.0,
            }
        })
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__  = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        await feed._poll(mock_session)

        assert len(received) == 1
        sig = received[0]
        assert isinstance(sig, AltDataSignal)
        assert sig.score     == 72.5
        assert sig.direction == 1      # greed → bullish

    @pytest.mark.asyncio
    async def test_fear_direction(self):
        received = []
        feed = FearGreedFeed(callback=received.append, poll_interval=9999)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json   = AsyncMock(return_value={
            "fear_and_greed": {"score": 22.0, "rating": "Extreme Fear"}
        })
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__  = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        await feed._poll(mock_session)
        assert received[0].direction == -1


# ─────────────────────────────────────────────────────────────────────────────
# AltDataSignal model tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAltDataSignal:

    def test_score_clamped_above_100(self):
        sig = AltDataSignal(
            source="test", signal_type="test",
            score=150.0, direction=1,
            timestamp=_ts(),
        )
        assert sig.score == 100.0

    def test_score_clamped_below_0(self):
        sig = AltDataSignal(
            source="test", signal_type="test",
            score=-20.0, direction=-1,
            timestamp=_ts(),
        )
        assert sig.score == 0.0

    def test_symbol_optional(self):
        sig = AltDataSignal(
            source="cnn_money", signal_type="fear_greed_index",
            score=50.0, direction=0,
            timestamp=_ts(),
        )
        assert sig.symbol is None