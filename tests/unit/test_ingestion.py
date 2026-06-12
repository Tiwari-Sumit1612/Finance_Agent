"""
tests/unit/test_ingestion_annotated.py
─────────────────────────────────────────────────────────────────────────────
Same tests as test_ingestion.py but with FULL ANNOTATIONS explaining:
  • Why each test exists
  • What it is testing
  • How to write similar tests yourself

Run:
    pytest tests/unit/test_ingestion_annotated.py -v

Prerequisites:
    pip install pydantic websockets aiohttp pytest pytest-asyncio
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS — we import from the real ingestion modules, not from test doubles
# ─────────────────────────────────────────────────────────────────────────────
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
    """Return a valid ISO timestamp string for the current moment."""
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Concrete subclass for testing BaseConnector in isolation
#
# BaseConnector is abstract — you can't instantiate it directly.
# We create a minimal concrete subclass that implements the three
# abstract methods with the simplest possible logic, so we can
# exercise the BaseConnector infrastructure.
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
        pass   # no-op for testing

    def _parse_message(self, data: dict):
        if data.get("skip"):
            return None   # intentional skip
        # This will raise KeyError if "price" is missing — tests that case
        return MarketTick(
            symbol    = data["symbol"],
            price     = data["price"],
            volume    = data.get("volume", 1.0),
            timestamp = _ts(),
            source    = "test",
            raw_type  = "trade",
        )


# ─────────────────────────────────────────────────────────────────────────────
# ❶ BaseConnector tests
#
# What we're testing:
#   The infrastructure of BaseConnector — initial state, stop(), stats,
#   and the message-handling logic in _handle_raw().
#
# Why these tests matter:
#   Every ingestion connector inherits from BaseConnector.
#   If the base class is broken, every connector is broken.
# ─────────────────────────────────────────────────────────────────────────────

class TestBaseConnector:

    # ── Health state ──────────────────────────────────────────────────────────

    def test_initial_health_is_disconnected(self):
        """
        Before connecting to anything, health must be DISCONNECTED.
        This matters because other code might check health before using
        the connector.
        """
        conn = _ConcreteConnector(messages=[])
        assert conn.health == ConnectorHealth.DISCONNECTED

    def test_stop_sets_event(self):
        """
        stop() sets the internal asyncio.Event that the run loop checks.
        We test this synchronously — no need to start a real run loop.
        """
        conn = _ConcreteConnector(messages=[])
        conn.stop()
        assert conn._stop_event.is_set()

    # ── Stats ─────────────────────────────────────────────────────────────────

    def test_initial_stats(self):
        """
        Fresh connector should have zero counts and 0% error rate.
        """
        conn  = _ConcreteConnector(messages=[])
        stats = conn.stats
        assert stats.messages_received == 0
        assert stats.messages_failed   == 0
        assert stats.error_rate        == 0.0

    # ── _handle_raw — the message processing pipeline ─────────────────────────
    # Each test below covers ONE branch of the if/except tree in _handle_raw.
    # Testing branches individually makes it easy to see exactly which case
    # is broken when a test fails.

    @pytest.mark.asyncio   # ← required for async test functions
    async def test_handle_raw_valid_message(self):
        """
        Happy path: valid JSON → _parse_message succeeds → callback fires.

        How the mock works:
          message_callback=received.append  makes every received message
          get appended to the 'received' list, so we can inspect them.
        """
        received = []
        conn = _ConcreteConnector(
            messages=[],
            message_callback=received.append,
        )
        raw = json.dumps({"symbol": "AAPL", "price": 189.5, "volume": 1000})
        await conn._handle_raw(raw)

        assert len(received) == 1
        assert received[0].symbol == "AAPL"
        assert received[0].price  == 189.5
        assert conn.stats.messages_received == 1   # counter incremented

    @pytest.mark.asyncio
    async def test_handle_raw_invalid_json(self):
        """
        Invalid JSON should increment messages_failed.
        The connector should NOT crash — bad messages must be swallowed.
        """
        conn = _ConcreteConnector(messages=[])
        await conn._handle_raw("not valid json {{{")
        assert conn.stats.messages_failed   == 1
        assert conn.stats.messages_received == 0   # failed, not received

    @pytest.mark.asyncio
    async def test_handle_raw_skip_returns_none(self):
        """
        When _parse_message returns None (heartbeat, subscription ack, etc.),
        the message is silently dropped.
        It must NOT increment messages_received OR messages_failed.
        """
        received = []
        conn = _ConcreteConnector(messages=[], message_callback=received.append)
        await conn._handle_raw(json.dumps({"skip": True}))
        assert len(received)                == 0   # not delivered
        assert conn.stats.messages_received == 0   # not counted as success
        assert conn.stats.messages_failed   == 0   # not counted as failure

    @pytest.mark.asyncio
    async def test_dead_letter_populated_on_validation_error(self):
        """
        When a message fails parsing (missing required field → KeyError, or
        bad type → ValidationError), it lands in the dead-letter queue.

        This test sends {"symbol": "AAPL"} — missing "price".
        _parse_message will raise KeyError (or ValidationError depending on
        how the connector is implemented).

        FIX REQUIRED: base_connector.py must catch broad Exception, not just
        ValidationError, for this test to pass.
        """
        conn = _ConcreteConnector(messages=[])
        await conn._handle_raw(json.dumps({"symbol": "AAPL"}))   # no price!
        assert len(conn._dead_letters) == 1   # bad message captured, not lost
        assert conn.stats.messages_failed == 1

    def test_health_degrades_on_high_error_rate(self):
        """
        When more than 10% of messages fail, health drops to DEGRADED.
        This lets operators detect a connector that's receiving garbage.
        """
        conn = _ConcreteConnector(messages=[])
        conn._health = ConnectorHealth.CONNECTED
        conn._stats.messages_received = 1
        conn._stats.messages_failed   = 5   # 5/6 = 83% error rate
        conn._update_health()
        assert conn.health == ConnectorHealth.DEGRADED


# ─────────────────────────────────────────────────────────────────────────────
# ❷ MarketTick model tests
#
# What we're testing:
#   The Pydantic model itself — validation rules, type coercion, validators.
#
# Why test the model?
#   The model is the contract between the ingestion layer and your agents.
#   If the model lets bad data through (negative price) or fails to parse
#   valid data (ISO timestamps), agents receive garbage.
# ─────────────────────────────────────────────────────────────────────────────

class TestMarketTickModel:

    def test_basic_construction(self):
        """Normal case — just make sure nothing explodes."""
        tick = MarketTick(
            symbol="NVDA", price=875.0, volume=5000,
            timestamp=_ts(), source="alpaca", raw_type="trade",
        )
        assert tick.symbol == "NVDA"
        assert tick.price  == 875.0

    def test_rejects_negative_price(self):
        """
        A negative price is physically impossible for a stock.
        Pydantic should reject it via the @field_validator.

        pytest.raises(ValidationError) is a context manager.
        If the code inside does NOT raise that exception, the test fails.
        """
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            MarketTick(
                symbol="AAPL", price=-10.0, volume=100,
                timestamp=_ts(), source="alpaca", raw_type="trade",
            )

    def test_timestamp_from_epoch_nanoseconds(self):
        """
        Alpaca sends timestamps as nanoseconds since epoch (a big integer).
        The @field_validator must convert this to a proper tz-aware datetime.
        """
        import time
        ns = int(time.time() * 1e9)
        tick = MarketTick(
            symbol="AAPL", price=100.0, volume=1,
            timestamp=ns, source="test", raw_type="trade",
        )
        assert tick.timestamp.tzinfo is not None   # must be timezone-aware

    def test_timestamp_from_iso_string(self):
        """
        JSON often carries timestamps as ISO strings like "2025-01-15T14:30:00Z".
        The validator must handle the "Z" suffix (not standard isoformat).
        """
        tick = MarketTick(
            symbol="AAPL", price=100.0, volume=1,
            timestamp="2025-01-15T14:30:00Z",
            source="test", raw_type="trade",
        )
        assert tick.timestamp.year == 2025

    def test_optional_bid_ask(self):
        """
        bid/ask are optional fields (quotes have them, trades don't).
        They should be set when provided.
        """
        tick = MarketTick(
            symbol="AAPL", price=100.0, volume=1,
            bid=99.99, ask=100.01,
            timestamp=_ts(), source="test", raw_type="quote",
        )
        assert tick.bid == 99.99
        assert tick.ask == 100.01


# ─────────────────────────────────────────────────────────────────────────────
# ❸ AlpacaMarketFeed parsing tests
#
# What we're testing:
#   _parse_message() — the connector's internal parser.
#   This is a pure function (dict → model), so we test it directly.
#
# How the env patching works:
#   AlpacaMarketFeed.__init__ reads os.environ["ALPACA_API_KEY"].
#   patch.dict("os.environ", {...}) temporarily sets those keys.
#   When the `with` block exits, the original environment is restored.
# ─────────────────────────────────────────────────────────────────────────────

class TestAlpacaMarketFeed:

    def _feed(self) -> AlpacaMarketFeed:
        """Helper: construct a feed with fake credentials."""
        with patch.dict("os.environ", {
            "ALPACA_API_KEY":    "test_key",
            "ALPACA_SECRET_KEY": "test_secret",
        }):
            return AlpacaMarketFeed(symbols=["AAPL", "NVDA"])

    def test_parse_trade_message(self):
        """
        Alpaca trade message format: {"T": "t", "S": symbol, "p": price, ...}
        Must produce a MarketTick with raw_type="trade".
        """
        feed = self._feed()
        raw  = [{"T": "t", "S": "AAPL", "p": "189.5", "s": "500", "t": _ts()}]
        result = feed._parse_message(raw)
        assert isinstance(result, MarketTick)
        assert result.symbol   == "AAPL"
        assert result.price    == 189.5     # was a string, now a float
        assert result.raw_type == "trade"

    def test_parse_quote_message(self):
        """
        Alpaca quote message: {"T": "q", "bp": bid_price, "ap": ask_price, ...}
        """
        feed = self._feed()
        raw  = [{"T": "q", "S": "NVDA", "bp": "874.0", "ap": "875.0",
                 "bs": "100", "as": "150", "t": _ts()}]
        result = feed._parse_message(raw)
        assert isinstance(result, MarketTick)
        assert result.bid      == 874.0
        assert result.ask      == 875.0
        assert result.raw_type == "quote"

    def test_parse_bar_message(self):
        """
        Alpaca bar message: {"T": "b", "o": open, "h": high, "l": low, "c": close, ...}
        Must produce a TradeBar, not a MarketTick.
        """
        feed = self._feed()
        raw  = [{
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
        """
        Alpaca sends {"T": "success"} after successful auth.
        These are control messages and must be silently skipped (return None).
        """
        feed   = self._feed()
        result = feed._parse_message([{"T": "success", "msg": "connected"}])
        assert result is None

    def test_skips_subscription_ack(self):
        """Subscription confirmations must also be skipped."""
        feed   = self._feed()
        result = feed._parse_message([{"T": "subscription", "trades": ["AAPL"]}])
        assert result is None

    def test_list_of_mixed_messages(self):
        """
        Alpaca can send multiple events in one WebSocket frame.
        _parse_message should return the last meaningful one.
        """
        feed = self._feed()
        raw  = [
            {"T": "success", "msg": "connected"},             # skip
            {"T": "t", "S": "AAPL", "p": "189.5", "s": "500", "t": _ts()},  # use this
        ]
        result = feed._parse_message(raw)
        assert isinstance(result, MarketTick)

    def test_ws_url_uses_feed(self):
        """The WebSocket URL should include the feed name (iex = free tier)."""
        feed = self._feed()
        assert "iex" in feed.ws_url


# ─────────────────────────────────────────────────────────────────────────────
# ❹ extract_symbols tests
#
# What we're testing:
#   A pure utility function — no mocking needed.
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractSymbols:

    def test_cashtags(self):
        """$AAPL and $NVDA are unambiguous cashtags."""
        symbols = extract_symbols("$AAPL is up while $NVDA drops")
        assert "AAPL" in symbols
        assert "NVDA" in symbols

    def test_watchlist_cross_reference(self):
        """
        Plain uppercase words are only extracted if they're in the watchlist.
        Without a watchlist, "AAPL" by itself could be any word.
        """
        symbols = extract_symbols("AAPL reported strong earnings", watchlist={"AAPL", "MSFT"})
        assert "AAPL" in symbols    # in text AND in watchlist → extracted
        assert "MSFT" not in symbols  # in watchlist but NOT in text → not extracted

    def test_no_false_positives(self):
        """
        Uppercase abbreviations like "IT" should not be extracted
        unless they're in the watchlist.
        """
        symbols = extract_symbols("IT was a great day", watchlist=set())
        assert symbols == []

    def test_empty_text(self):
        assert extract_symbols("") == []

    def test_mixed_case_ignored(self):
        """Cashtag regex requires uppercase: $aapl is NOT a cashtag."""
        symbols = extract_symbols("$aapl is cheap")
        assert "AAPL" not in symbols


# ─────────────────────────────────────────────────────────────────────────────
# ❺ NewsItem model tests
# ─────────────────────────────────────────────────────────────────────────────

class TestNewsItem:

    def test_make_id_is_deterministic(self):
        """
        The same source+URL must always produce the same ID.
        This is used for deduplication across polls.
        """
        id1 = NewsItem.make_id("reuters", "https://example.com/story1")
        id2 = NewsItem.make_id("reuters", "https://example.com/story1")
        assert id1 == id2

    def test_make_id_differs_by_source(self):
        """Reuters and CNBC might publish the same URL — IDs must differ."""
        id1 = NewsItem.make_id("reuters", "same-url")
        id2 = NewsItem.make_id("cnbc",    "same-url")
        assert id1 != id2

    def test_ingested_at_is_auto_set(self):
        """
        ingested_at is set automatically by the model's default_factory.
        We don't want to have to pass it manually every time.
        """
        item = NewsItem(
            id="abc123", title="Test", summary="Summary",
            url="https://example.com", source="reuters",
            source_type="rss", symbols=["AAPL"],
            published_at=_ts(),
        )
        assert item.ingested_at is not None


# ─────────────────────────────────────────────────────────────────────────────
# ❻ RSSNewsFeed tests (mocked HTTP)
#
# What we're testing:
#   The _poll_feed method, which fetches and parses an RSS XML response.
#
# Mocking strategy:
#   aiohttp.ClientSession.get() returns a context manager (async with).
#   We simulate this with:
#     mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
#     mock_resp.__aexit__  = AsyncMock(return_value=False)
#   This makes `async with session.get(url) as resp:` work in tests.
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


def _mock_session(xml_body: str):
    """
    Helper: build a fake aiohttp.ClientSession that returns xml_body.
    Extracted into a helper so test methods don't repeat 6 lines of boilerplate.
    """
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.text   = AsyncMock(return_value=xml_body)
    mock_resp.raise_for_status = MagicMock()
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__  = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_resp)
    return mock_session


class TestRSSNewsFeed:

    @pytest.mark.asyncio
    async def test_parses_rss_and_calls_back(self):
        """
        Given a valid 2-item RSS feed, exactly 2 NewsItems should be emitted.
        """
        received = []
        feed = RSSNewsFeed(
            sources={"test": "https://example.com/rss"},
            callback=received.append,
            poll_interval=9999,   # large → won't loop
        )
        await feed._poll_feed(_mock_session(MOCK_RSS), "test", "https://example.com/rss")

        assert len(received) == 2
        titles = [item.title for item in received]
        assert any("AAPL" in t for t in titles)
        assert any("NVDA" in t for t in titles)

    @pytest.mark.asyncio
    async def test_deduplication(self):
        """
        Polling the same feed twice must NOT produce 4 items — only 2.
        The feed should remember the item IDs from the first poll.
        """
        received = []
        feed = RSSNewsFeed(
            sources={"test": "https://example.com/rss"},
            callback=received.append,
            poll_interval=9999,
        )
        session = _mock_session(MOCK_RSS)
        await feed._poll_feed(session, "test", "https://example.com/rss")
        await feed._poll_feed(session, "test", "https://example.com/rss")

        assert len(received) == 2   # second poll sees already-known IDs, skips


# ─────────────────────────────────────────────────────────────────────────────
# ❼ FREDIndicatorFeed tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFREDIndicatorFeed:

    @pytest.mark.asyncio
    async def test_emits_on_new_release(self):
        """
        When FRED returns a fresh observation date, an EconomicIndicator
        event should be emitted with the correct value and prior_value.
        """
        received = []
        with patch.dict("os.environ", {"FRED_API_KEY": "test_key"}):
            feed = FREDIndicatorFeed(callback=received.append, poll_interval=9999)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json   = AsyncMock(return_value={
            "observations": [
                {"date": "2025-04-01", "value": "3.5"},   # latest
                {"date": "2025-03-01", "value": "3.2"},   # prior
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
        assert received[0].value       == 3.5
        assert received[0].prior_value == 3.2

    @pytest.mark.asyncio
    async def test_no_emit_if_date_unchanged(self):
        """
        If we already saw "2025-04-01" for CPIAUCSL, don't emit again.
        This prevents duplicate events when FRED is polled every 5 minutes.
        """
        received = []
        with patch.dict("os.environ", {"FRED_API_KEY": "test_key"}):
            feed = FREDIndicatorFeed(callback=received.append, poll_interval=9999)

        feed._last_date["CPIAUCSL"] = "2025-04-01"   # simulate already-seen

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

        assert len(received) == 0   # no duplicate


# ─────────────────────────────────────────────────────────────────────────────
# ❽ FearGreedFeed tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFearGreedFeed:

    @pytest.mark.asyncio
    async def test_emits_signal(self):
        """
        CNN Fear & Greed score of 72.5 is in the "Greed" zone → direction +1.
        """
        received = []
        feed = FearGreedFeed(callback=received.append, poll_interval=9999)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json   = AsyncMock(return_value={
            "fear_and_greed": {
                "score": 72.5, "rating": "Greed", "previous_close": 68.0,
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
        assert sig.direction == 1        # greed → bullish

    @pytest.mark.asyncio
    async def test_fear_direction(self):
        """Score of 22 (Extreme Fear) → direction -1."""
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
# ❾ AltDataSignal clamping tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAltDataSignal:

    def test_score_clamped_above_100(self):
        """
        score must be in [0, 100]. A raw value of 150 should become 100.
        This is enforced by the @field_validator("score") clamp_score method.
        """
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
        """
        Market-wide signals (Fear & Greed) don't have a symbol.
        The model must allow symbol=None.
        """
        sig = AltDataSignal(
            source="cnn_money", signal_type="fear_greed_index",
            score=50.0, direction=0,
            timestamp=_ts(),
        )
        assert sig.symbol is None


# ─────────────────────────────────────────────────────────────────────────────
# HOW TO WRITE YOUR OWN TESTS — quick reference
# ─────────────────────────────────────────────────────────────────────────────
#
# 1. Pure function (no I/O):
#      def test_my_func():
#          assert my_func(input) == expected_output
#
# 2. Pydantic validation:
#      def test_rejects_bad_input():
#          with pytest.raises(ValidationError):
#              MyModel(bad_field=bad_value)
#
# 3. Class that reads os.environ:
#      def test_my_class():
#          with patch.dict("os.environ", {"KEY": "value"}):
#              obj = MyClass()
#          assert obj.some_property == expected
#
# 4. Async function:
#      @pytest.mark.asyncio
#      async def test_my_async_func():
#          result = await my_async_func()
#          assert result == expected
#
# 5. HTTP call (aiohttp context manager pattern):
#      @pytest.mark.asyncio
#      async def test_http_fetch():
#          mock_resp = AsyncMock()
#          mock_resp.status = 200
#          mock_resp.json   = AsyncMock(return_value={"key": "value"})
#          mock_resp.raise_for_status = MagicMock()
#          mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
#          mock_resp.__aexit__  = AsyncMock(return_value=False)
#          mock_session = AsyncMock()
#          mock_session.get = MagicMock(return_value=mock_resp)
#
#          result = await my_feed._poll(mock_session)
#          assert result == expected
#
# 6. Callback collection:
#      received = []
#      feed = MyFeed(callback=received.append)
#      await feed._poll(mock_session)
#      assert len(received) == 1
#      assert received[0].field == expected
# ─────────────────────────────────────────────────────────────────────────────
