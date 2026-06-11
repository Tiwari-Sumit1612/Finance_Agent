"""
market_feed.py
─────────────────────────────────────────────────────────────────────────────
Real-time market data ingestion via WebSocket.

Supports two providers:
  • Alpaca  – wss://stream.data.alpaca.markets/v2/{feed}
  • Polygon – wss://socket.polygon.io/stocks

Both produce a normalised MarketTick model that the rest of the pipeline
consumes without knowing which provider was used.

Environment variables (set in .env):
  ALPACA_API_KEY      – Alpaca key ID
  ALPACA_SECRET_KEY   – Alpaca secret
  ALPACA_FEED         – "iex" (free) | "sip" (paid)  [default: iex]
  POLYGON_API_KEY     – Polygon.io API key

Usage:
  # Alpaca
  feed = AlpacaMarketFeed(symbols=["AAPL","NVDA","BTC/USD"], callback=handle)
  await feed.run()

  # Polygon
  feed = PolygonMarketFeed(symbols=["AAPL","NVDA"], callback=handle)
  await feed.run()
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from pydantic import BaseModel, Field, field_validator

from .base_connector import BaseConnector, RetryConfig

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Normalised tick model  (provider-agnostic)
# ─────────────────────────────────────────────────────────────────────────────

class MarketTick(BaseModel):
    """Single normalised price tick."""
    symbol:     str
    price:      float
    volume:     float
    bid:        Optional[float] = None
    ask:        Optional[float] = None
    vwap:       Optional[float] = None       # volume-weighted avg price
    timestamp:  datetime
    source:     str                           # "alpaca" | "polygon"
    raw_type:   str                           # original message type

    @field_validator("price", "volume")
    @classmethod
    def must_be_positive(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"Expected non-negative value, got {v}")
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v
        if isinstance(v, (int, float)):
            # nanoseconds (Alpaca) vs milliseconds vs seconds
            if v > 1e12:
                v = v / 1e9
            elif v > 1e9:
                v = v / 1e3
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        raise ValueError(f"Cannot parse timestamp: {v!r}")


class TradeBar(BaseModel):
    """OHLCV bar (minute / day)."""
    symbol:     str
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     float
    vwap:       Optional[float] = None
    timestamp:  datetime
    bar_size:   str = "1Min"
    source:     str


# ─────────────────────────────────────────────────────────────────────────────
# Alpaca connector
# ─────────────────────────────────────────────────────────────────────────────

class AlpacaMarketFeed(BaseConnector):
    """
    Streams real-time trades and quotes from Alpaca.
    Free tier: IEX feed (~15-min delayed for stocks, real-time for crypto).
    Paid tier: SIP feed (real-time consolidated tape).
    """

    _BASE_URL = "wss://stream.data.alpaca.markets/v2/{feed}"

    def __init__(
        self,
        symbols: list[str],
        message_callback: Optional[Callable[[MarketTick | TradeBar], None]] = None,
        feed: Optional[str] = None,
        subscribe_trades: bool = True,
        subscribe_quotes: bool = True,
        subscribe_bars:   bool = True,
    ):
        super().__init__(
            name="AlpacaMarketFeed",
            retry_cfg=RetryConfig(initial_delay=2.0, max_delay=30.0),
            message_callback=message_callback,  # type: ignore[arg-type]
        )
        self.symbols          = [s.upper() for s in symbols]
        self._api_key         = os.environ["ALPACA_API_KEY"]
        self._secret_key      = os.environ["ALPACA_SECRET_KEY"]
        self._feed            = (feed or os.getenv("ALPACA_FEED", "iex")).lower()
        self.subscribe_trades = subscribe_trades
        self.subscribe_quotes = subscribe_quotes
        self.subscribe_bars   = subscribe_bars

    @property
    def ws_url(self) -> str:
        return self._BASE_URL.format(feed=self._feed)

    async def _subscribe(self, ws: Any) -> None:
        # 1) Authenticate
        auth = json.dumps({
            "action": "auth",
            "key":    self._api_key,
            "secret": self._secret_key,
        })
        await ws.send(auth)
        resp = json.loads(await ws.recv())
        logger.debug("[AlpacaMarketFeed] Auth response: %s", resp)

        # 2) Subscribe
        channels: dict[str, list[str]] = {}
        if self.subscribe_trades:
            channels["trades"] = self.symbols
        if self.subscribe_quotes:
            channels["quotes"] = self.symbols
        if self.subscribe_bars:
            channels["bars"]   = self.symbols

        sub_msg = json.dumps({"action": "subscribe", **channels})
        await ws.send(sub_msg)
        logger.info("[AlpacaMarketFeed] Subscribed to %s on %s", self.symbols, self._feed)

    def _parse_message(self, data: dict | list) -> Optional[MarketTick | TradeBar]:
        # Alpaca sends a list of event objects
        if isinstance(data, list):
            results = [self._parse_single(item) for item in data]
            non_none = [r for r in results if r is not None]
            return non_none[-1] if non_none else None
        return self._parse_single(data)

    def _parse_single(self, item: dict) -> Optional[MarketTick | TradeBar]:
        msg_type = item.get("T")

        if msg_type in ("t",):  # trade
            return MarketTick(
                symbol    = item["S"],
                price     = float(item["p"]),
                volume    = float(item.get("s", 0)),
                timestamp = item["t"],
                source    = "alpaca",
                raw_type  = "trade",
            )

        if msg_type in ("q",):  # quote
            mid = (float(item.get("ap", 0)) + float(item.get("bp", 0))) / 2
            return MarketTick(
                symbol    = item["S"],
                price     = mid if mid > 0 else float(item.get("ap", 0)),
                volume    = float(item.get("as", 0)) + float(item.get("bs", 0)),
                bid       = float(item.get("bp", 0)) or None,
                ask       = float(item.get("ap", 0)) or None,
                timestamp = item["t"],
                source    = "alpaca",
                raw_type  = "quote",
            )

        if msg_type in ("b",):  # bar
            return TradeBar(
                symbol    = item["S"],
                open      = float(item["o"]),
                high      = float(item["h"]),
                low       = float(item["l"]),
                close     = float(item["c"]),
                volume    = float(item["v"]),
                vwap      = float(item["vw"]) if "vw" in item else None,
                timestamp = item["t"],
                source    = "alpaca",
            )

        # Heartbeat / subscription confirmations — skip silently
        if msg_type in ("success", "subscription", "error"):
            logger.debug("[AlpacaMarketFeed] Control msg: %s", item)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Polygon connector
# ─────────────────────────────────────────────────────────────────────────────

class PolygonMarketFeed(BaseConnector):
    """
    Streams real-time trades and aggregates from Polygon.io.
    Requires a paid Polygon plan for true real-time data.
    """

    _WS_URL = "wss://socket.polygon.io/stocks"

    def __init__(
        self,
        symbols: list[str],
        message_callback: Optional[Callable[[MarketTick | TradeBar], None]] = None,
        subscribe_trades: bool = True,
        subscribe_aggs:   bool = True,
    ):
        super().__init__(
            name="PolygonMarketFeed",
            retry_cfg=RetryConfig(initial_delay=2.0, max_delay=60.0),
            message_callback=message_callback,  # type: ignore[arg-type]
        )
        self.symbols          = [s.upper() for s in symbols]
        self._api_key         = os.environ["POLYGON_API_KEY"]
        self.subscribe_trades = subscribe_trades
        self.subscribe_aggs   = subscribe_aggs

    @property
    def ws_url(self) -> str:
        return self._WS_URL

    async def _subscribe(self, ws: Any) -> None:
        # Auth
        await ws.send(json.dumps({"action": "auth", "params": self._api_key}))
        auth_resp = json.loads(await ws.recv())
        logger.debug("[PolygonMarketFeed] Auth: %s", auth_resp)

        # Trades: T.{SYMBOL}
        if self.subscribe_trades:
            subs = ",".join(f"T.{s}" for s in self.symbols)
            await ws.send(json.dumps({"action": "subscribe", "params": subs}))

        # Second aggregates: A.{SYMBOL}
        if self.subscribe_aggs:
            subs = ",".join(f"A.{s}" for s in self.symbols)
            await ws.send(json.dumps({"action": "subscribe", "params": subs}))

        logger.info("[PolygonMarketFeed] Subscribed to %s", self.symbols)

    def _parse_message(self, data: dict | list) -> Optional[MarketTick | TradeBar]:
        if isinstance(data, list):
            results = [self._parse_single(item) for item in data]
            non_none = [r for r in results if r is not None]
            return non_none[-1] if non_none else None
        return self._parse_single(data)

    def _parse_single(self, item: dict) -> Optional[MarketTick | TradeBar]:
        ev = item.get("ev")

        if ev == "T":  # trade
            return MarketTick(
                symbol    = item["sym"],
                price     = float(item["p"]),
                volume    = float(item.get("s", 0)),
                timestamp = item["t"],       # epoch ms
                source    = "polygon",
                raw_type  = "trade",
            )

        if ev == "A":  # second aggregate
            return TradeBar(
                symbol    = item["sym"],
                open      = float(item["op"]),
                high      = float(item["h"]),
                low       = float(item["l"]),
                close     = float(item["c"]),
                volume    = float(item["av"]),
                vwap      = float(item["vw"]) if "vw" in item else None,
                timestamp = item["s"],       # epoch ms start
                bar_size  = "1Sec",
                source    = "polygon",
            )

        if ev in ("status",):
            logger.debug("[PolygonMarketFeed] Status: %s", item)
        return None
