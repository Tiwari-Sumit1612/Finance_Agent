"""
ingestion/
─────────────────────────────────────────────────────────────────────────────
Real-time data ingestion layer for the financial agents system.

Public API surface:

  Market data
  ───────────
  AlpacaMarketFeed      – real-time trades/quotes/bars via Alpaca WebSocket
  PolygonMarketFeed     – real-time trades/aggregates via Polygon WebSocket
  MarketTick            – normalised tick model (price, volume, bid/ask)
  TradeBar              – normalised OHLCV bar model

  News & social
  ─────────────
  UnifiedNewsFeed       – RSS + Reddit + (optional) Twitter under one callback
  RSSNewsFeed           – RSS-only polling feed
  RedditNewsFeed        – Reddit new-post feed
  TwitterNewsFeed       – Twitter v2 filtered stream
  NewsItem              – normalised news model

  Macro & corporate events
  ────────────────────────
  UnifiedMacroFeed      – SEC + FRED + Earnings + Fed Reserve
  SECFilingFeed         – SEC EDGAR 8-K / 10-K / 10-Q filing watcher
  FREDIndicatorFeed     – Federal Reserve economic indicator releases
  EarningsCalendarFeed  – Alpha Vantage earnings calendar
  FedReserveFeed        – FOMC statements and Fed speeches
  MacroEvent            – base macro event model
  EarningsEvent         – earnings-specific event model
  FedEvent              – Fed announcement event model
  EconomicIndicator     – economic data release event model

  Alternative data
  ────────────────
  UnifiedAltDataFeed    – Reddit mentions + Google Trends + Fear&Greed + PCR
  RedditMentionFeed     – ticker mention velocity tracker
  GoogleTrendsFeed      – Google search interest (pytrends)
  FearGreedFeed         – CNN Fear & Greed index
  PutCallRatioFeed      – CBOE put/call ratio
  AltDataSignal         – normalised 0-100 score model

  Infrastructure
  ──────────────
  BaseConnector         – abstract WebSocket connector with retry + health
  ConnectorHealth       – health state enum
  ConnectorStats        – live stats dataclass
"""

from .base_connector import (
    BaseConnector,
    ConnectorHealth,
    ConnectorStats,
    RetryConfig,
)
from .market_feed import (
    AlpacaMarketFeed,
    MarketTick,
    PolygonMarketFeed,
    TradeBar,
)
from .news_feed import (
    NewsItem,
    RSSNewsFeed,
    RedditNewsFeed,
    TwitterNewsFeed,
    UnifiedNewsFeed,
    extract_symbols,
)
from .macro_feed import (
    EarningsCalendarFeed,
    EarningsEvent,
    EconomicIndicator,
    FedEvent,
    FedReserveFeed,
    FREDIndicatorFeed,
    MacroEvent,
    SECFilingFeed,
    UnifiedMacroFeed,
)
from .alt_data_feed import (
    AltDataSignal,
    FearGreedFeed,
    GoogleTrendsFeed,
    PutCallRatioFeed,
    RedditMentionFeed,
    UnifiedAltDataFeed,
)

__all__ = [
    # base
    "BaseConnector", "ConnectorHealth", "ConnectorStats", "RetryConfig",
    # market
    "AlpacaMarketFeed", "PolygonMarketFeed", "MarketTick", "TradeBar",
    # news
    "UnifiedNewsFeed", "RSSNewsFeed", "RedditNewsFeed", "TwitterNewsFeed",
    "NewsItem", "extract_symbols",
    # macro
    "UnifiedMacroFeed", "SECFilingFeed", "FREDIndicatorFeed",
    "EarningsCalendarFeed", "FedReserveFeed",
    "MacroEvent", "EarningsEvent", "FedEvent", "EconomicIndicator",
    # alt data
    "UnifiedAltDataFeed", "RedditMentionFeed", "GoogleTrendsFeed",
    "FearGreedFeed", "PutCallRatioFeed", "AltDataSignal",
]
