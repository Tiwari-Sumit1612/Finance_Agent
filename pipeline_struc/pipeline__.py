"""
pipeline_complete.py
─────────────────────────────────────────────────────────────────────────────
Complete Finance Agent pipeline.

Architecture:

  ┌──────────────────────────────────────────────────────────────┐
  │                    DATA INGESTION LAYER                      │
  │  AlpacaMarketFeed  RSSNewsFeed  FREDIndicatorFeed  FearGreed │
  └───────────────────────────┬──────────────────────────────────┘
                              │  typed Pydantic events
  ┌───────────────────────────▼──────────────────────────────────┐
  │                    EVENT ROUTER (this file)                  │
  │  routes each event type to the right agent queue             │
  └──────┬──────────────┬───────────────┬───────────────┬────────┘
         │              │               │               │
  ┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
  │  Market     │ │  Sentiment  │ │  Macro     │ │  Anomaly   │
  │  Agent      │ │  Agent      │ │  Agent     │ │  Agent     │
  └──────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
         └──────────────┴───────────────┴───────────────┘
                              │  agent outputs
  ┌───────────────────────────▼──────────────────────────────────┐
  │                 SUPERVISOR AGENT                             │
  │  aggregates signals, decides on final market insight         │
  └───────────────────────────┬──────────────────────────────────┘
                              │
  ┌───────────────────────────▼──────────────────────────────────┐
  │                    OUTPUT LAYER                              │
  │  WebSocket dashboard  |  Alerts  |  Audit log               │
  └──────────────────────────────────────────────────────────────┘

Usage:
    python pipeline_complete.py

Environment variables required (set in .env):
    ALPACA_API_KEY, ALPACA_SECRET_KEY
    FRED_API_KEY
    Optional: POLYGON_API_KEY, REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


# ─────────────────────────────────────────────────────────────────────────────
# Event Bus — fan-out to multiple subscribers
# ─────────────────────────────────────────────────────────────────────────────

class EventBus:
    """
    Simple in-process fan-out message bus.

    Producers call: bus.publish(event)
    Consumers call: bus.subscribe("MarketTick", callback)

    Multiple subscribers per event type are supported.
    """

    def __init__(self):
        self._subs: dict[str, list] = defaultdict(list)

    def subscribe(self, event_type: str, callback) -> None:
        self._subs[event_type].append(callback)

    async def publish(self, event) -> None:
        event_type = type(event).__name__
        for cb in self._subs.get(event_type, []):
            try:
                result = cb(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("[EventBus] Subscriber error for %s: %s", event_type, exc)

        # also publish to wildcard subscribers
        for cb in self._subs.get("*", []):
            try:
                result = cb(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("[EventBus] Wildcard subscriber error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# In-memory State Store (replace with Redis in production)
# ─────────────────────────────────────────────────────────────────────────────

class StateStore:
    """
    Holds the latest tick, bar, and signals for each symbol.
    Thread/async-safe for single-process use.
    """

    def __init__(self):
        self.latest_tick: dict[str, Any]  = {}   # symbol → MarketTick
        self.latest_bar:  dict[str, Any]  = {}   # symbol → TradeBar
        self.fear_greed:  Optional[float] = None  # 0-100
        self.macro_events: list           = []    # last 50
        self.news_items:   list           = []    # last 100
        self.anomalies:    list           = []    # last 20

    def update_tick(self, tick) -> None:
        self.latest_tick[tick.symbol] = tick

    def update_bar(self, bar) -> None:
        self.latest_bar[bar.symbol] = bar

    def add_macro_event(self, event) -> None:
        self.macro_events.append(event)
        if len(self.macro_events) > 50:
            self.macro_events.pop(0)

    def add_news(self, item) -> None:
        self.news_items.append(item)
        if len(self.news_items) > 100:
            self.news_items.pop(0)

    def add_anomaly(self, anomaly: dict) -> None:
        self.anomalies.append(anomaly)
        if len(self.anomalies) > 20:
            self.anomalies.pop(0)

    def snapshot(self) -> dict:
        return {
            "ticks":        {k: v.model_dump() for k, v in self.latest_tick.items()},
            "bars":         {k: v.model_dump() for k, v in self.latest_bar.items()},
            "fear_greed":   self.fear_greed,
            "macro_count":  len(self.macro_events),
            "news_count":   len(self.news_items),
            "anomaly_count": len(self.anomalies),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Agents (simplified — plug in your LLM calls here)
# ─────────────────────────────────────────────────────────────────────────────

class MarketAgent:
    """
    Processes MarketTick and TradeBar events.
    Detects price spikes, computes running VWAP, flags unusual volume.
    """

    def __init__(self, bus: EventBus, store: StateStore):
        self.bus   = bus
        self.store = store
        self._price_history: dict[str, list[float]] = defaultdict(list)

        bus.subscribe("MarketTick", self.on_tick)
        bus.subscribe("TradeBar",   self.on_bar)

    def on_tick(self, tick) -> None:
        self.store.update_tick(tick)
        history = self._price_history[tick.symbol]
        history.append(tick.price)
        if len(history) > 100:
            history.pop(0)

        # Simple spike detection: price > 3% above recent average
        if len(history) >= 10:
            avg = sum(history[-10:]) / 10
            if tick.price > avg * 1.03:
                anomaly = {
                    "type":      "price_spike",
                    "symbol":    tick.symbol,
                    "price":     tick.price,
                    "avg_price": round(avg, 2),
                    "pct_above": round((tick.price / avg - 1) * 100, 2),
                    "ts":        tick.timestamp.isoformat(),
                }
                self.store.add_anomaly(anomaly)
                logger.warning("[MarketAgent] Spike: %s @ %.2f (avg %.2f, +%.1f%%)",
                               tick.symbol, tick.price, avg, anomaly["pct_above"])

    def on_bar(self, bar) -> None:
        self.store.update_bar(bar)
        logger.debug("[MarketAgent] Bar %s O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f",
                     bar.symbol, bar.open, bar.high, bar.low, bar.close, bar.volume)


class SentimentAgent:
    """
    Processes NewsItem events.
    Extracts symbols, assigns a basic sentiment score, logs high-impact items.
    """

    def __init__(self, bus: EventBus, store: StateStore):
        self.bus   = bus
        self.store = store
        bus.subscribe("NewsItem", self.on_news)

    def on_news(self, item) -> None:
        self.store.add_news(item)
        symbols_str = ", ".join(item.symbols) if item.symbols else "market"
        logger.info("[SentimentAgent] News [%s]: %s", symbols_str, item.title[:80])
        # TODO: call LLM here for sentiment scoring
        #   score = await llm_client.score_sentiment(item.title + " " + item.summary)
        #   item.sentiment_raw = score


class MacroAgent:
    """
    Processes EconomicIndicator, EarningsEvent, FedEvent.
    Flags high-importance releases.
    """

    def __init__(self, bus: EventBus, store: StateStore):
        self.bus   = bus
        self.store = store
        bus.subscribe("EconomicIndicator", self.on_indicator)
        bus.subscribe("EarningsEvent",     self.on_earnings)
        bus.subscribe("FedEvent",          self.on_fed)

    def on_indicator(self, ind) -> None:
        self.store.add_macro_event(ind)
        logger.info("[MacroAgent] %s = %s %s (prior: %s)",
                    ind.indicator, ind.value, ind.unit, ind.prior_value)

    def on_earnings(self, ev) -> None:
        self.store.add_macro_event(ev)
        beat = "BEAT" if ev.beat else "MISS" if ev.beat is False else "?"
        logger.info("[MacroAgent] Earnings %s: EPS %s vs est %s [%s]",
                    ev.symbol, ev.eps_actual, ev.eps_estimate, beat)

    def on_fed(self, ev) -> None:
        self.store.add_macro_event(ev)
        logger.info("[MacroAgent] Fed: [%s] %s", ev.category, ev.title[:80])


class AltDataAgent:
    """
    Processes AltDataSignal (Fear&Greed, Put/Call Ratio, Reddit mentions).
    Updates market-wide sentiment in the state store.
    """

    def __init__(self, bus: EventBus, store: StateStore):
        self.bus   = bus
        self.store = store
        bus.subscribe("AltDataSignal", self.on_signal)

    def on_signal(self, sig) -> None:
        if sig.signal_type == "fear_greed_index":
            self.store.fear_greed = sig.score
            logger.info("[AltDataAgent] Fear/Greed: %.1f (%s)",
                        sig.score, sig.metadata.get("rating", ""))
        elif sig.signal_type == "put_call_ratio":
            logger.info("[AltDataAgent] Put/Call Ratio: %.3f (direction %+d)",
                        sig.raw_value or sig.score, sig.direction)
        elif sig.signal_type == "mention_velocity":
            logger.info("[AltDataAgent] Reddit mention spike: %s score=%.0f",
                        sig.symbol, sig.score)


class SupervisorAgent:
    """
    Periodically aggregates all signals and produces a market summary.
    In production this calls an LLM to synthesise a natural-language insight.
    """

    def __init__(self, store: StateStore, interval: int = 60):
        self.store    = store
        self.interval = interval

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            snap = self.store.snapshot()
            logger.info(
                "[SupervisorAgent] Snapshot — %d ticks | %d bars | "
                "F/G: %s | macro: %d | news: %d | anomalies: %d",
                len(snap["ticks"]),
                len(snap["bars"]),
                f"{snap['fear_greed']:.0f}" if snap["fear_greed"] else "n/a",
                snap["macro_count"],
                snap["news_count"],
                snap["anomaly_count"],
            )
            # TODO: call LLM for synthesis:
            # insight = await llm_client.synthesise(snap, self.store.anomalies[-5:])
            # await websocket_server.broadcast(insight)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline — wires everything together
# ─────────────────────────────────────────────────────────────────────────────

class FinancePipeline:
    """
    Top-level orchestrator.

    Call:
        pipeline = FinancePipeline(symbols=["AAPL", "NVDA", "MSFT"])
        await pipeline.run()
    """

    def __init__(
        self,
        symbols: list[str],
        use_alpaca: bool = True,
        use_rss:    bool = True,
        use_fred:   bool = True,
        use_altdata: bool = True,
        supervisor_interval: int = 60,
    ):
        self.symbols = symbols
        self.bus     = EventBus()
        self.store   = StateStore()
        self._tasks: list[asyncio.Task] = []
        self._feeds: list = []

        # ── instantiate agents (they self-register with the bus) ───────────
        self.market_agent   = MarketAgent(self.bus, self.store)
        self.sentiment_agent = SentimentAgent(self.bus, self.store)
        self.macro_agent    = MacroAgent(self.bus, self.store)
        self.alt_agent      = AltDataAgent(self.bus, self.store)
        self.supervisor     = SupervisorAgent(self.store, interval=supervisor_interval)

        # ── configure feeds ───────────────────────────────────────────────
        self._use_alpaca  = use_alpaca  and bool(os.getenv("ALPACA_API_KEY"))
        self._use_rss     = use_rss
        self._use_fred    = use_fred    and bool(os.getenv("FRED_API_KEY"))
        self._use_altdata = use_altdata

    async def run(self) -> None:
        """Start all feeds and agents. Runs until interrupted."""
        logger.info("=" * 60)
        logger.info("Finance Agent Pipeline starting")
        logger.info("Symbols: %s", self.symbols)
        logger.info("=" * 60)

        tasks: list[asyncio.Task] = []

        # ── Market feed ───────────────────────────────────────────────────
        if self._use_alpaca:
            from ingestion.market_feed import AlpacaMarketFeed, MarketTick, TradeBar

            async def _market_cb(event):
                await self.bus.publish(event)

            market_feed = AlpacaMarketFeed(
                symbols=self.symbols,
                message_callback=lambda e: asyncio.ensure_future(self.bus.publish(e)),
            )
            tasks.append(asyncio.create_task(market_feed.run(), name="alpaca_feed"))
            logger.info("[Pipeline] Alpaca market feed enabled")
        else:
            logger.warning("[Pipeline] Alpaca feed disabled (no ALPACA_API_KEY)")

        # ── News / RSS feed ───────────────────────────────────────────────
        if self._use_rss:
            from ingestion.news_feed import RSSNewsFeed

            news_feed = RSSNewsFeed(
                callback=lambda e: asyncio.ensure_future(self.bus.publish(e)),
                poll_interval=60,
            )
            tasks.append(asyncio.create_task(news_feed.run(), name="rss_news_feed"))
            logger.info("[Pipeline] RSS news feed enabled")

        # ── FRED macro feed ───────────────────────────────────────────────
        if self._use_fred:
            from ingestion.macro_feed import FREDIndicatorFeed

            fred_feed = FREDIndicatorFeed(
                callback=lambda e: asyncio.ensure_future(self.bus.publish(e)),
                poll_interval=300,
            )
            tasks.append(asyncio.create_task(fred_feed.run(), name="fred_feed"))
            logger.info("[Pipeline] FRED macro feed enabled")
        else:
            logger.warning("[Pipeline] FRED feed disabled (no FRED_API_KEY)")

        # ── Alt data (Fear & Greed, Put/Call) ─────────────────────────────
        if self._use_altdata:
            from ingestion.alt_data_feed import FearGreedFeed, PutCallRatioFeed

            fg_feed = FearGreedFeed(
                callback=lambda e: asyncio.ensure_future(self.bus.publish(e)),
                poll_interval=600,
            )
            pcr_feed = PutCallRatioFeed(
                callback=lambda e: asyncio.ensure_future(self.bus.publish(e)),
                poll_interval=300,
            )
            tasks.append(asyncio.create_task(fg_feed.run(),  name="fear_greed_feed"))
            tasks.append(asyncio.create_task(pcr_feed.run(), name="put_call_feed"))
            logger.info("[Pipeline] Alt data feeds enabled")

        # ── Supervisor ────────────────────────────────────────────────────
        tasks.append(asyncio.create_task(self.supervisor.run(), name="supervisor"))

        # ── Shutdown handler ──────────────────────────────────────────────
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._shutdown, tasks)

        logger.info("[Pipeline] All feeds running. Press Ctrl+C to stop.")

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("[Pipeline] Shutdown complete.")

    def _shutdown(self, tasks: list[asyncio.Task]) -> None:
        logger.info("[Pipeline] Shutdown signal received…")
        for t in tasks:
            t.cancel()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Load .env if python-dotenv is installed
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    pipeline = FinancePipeline(
        symbols=["AAPL", "NVDA", "MSFT", "TSLA", "AMZN"],
        use_alpaca  = True,
        use_rss     = True,
        use_fred    = True,
        use_altdata = True,
        supervisor_interval = 60,
    )

    asyncio.run(pipeline.run())
