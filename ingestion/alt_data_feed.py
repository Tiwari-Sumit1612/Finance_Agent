"""
alt_data_feed.py
─────────────────────────────────────────────────────────────────────────────
Alternative data ingestion — signals that supplement price/news data.

Sources:
  • Reddit Mention Velocity  – posts/comments per hour per ticker (r/wsb etc.)
  • Google Trends            – search interest via pytrends (unofficial API)
  • Fear & Greed Index       – CNN Money (scraped, public page)
  • Options Flow             – put/call ratio via CBOE delayed data (free)

All sources emit an AltDataSignal model with a normalised 0-100 score
and direction (+1 / 0 / -1) so agents can compare across source types.

Environment variables:
  REDDIT_CLIENT_ID
  REDDIT_CLIENT_SECRET
  REDDIT_USER_AGENT
  ALT_DATA_POLL_INTERVAL     – seconds (default 300)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Optional

import aiohttp
import asyncpraw
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

ALT_POLL_INTERVAL = int(os.getenv("ALT_DATA_POLL_INTERVAL", "300"))


# ─────────────────────────────────────────────────────────────────────────────
# Normalised alt-data signal
# ─────────────────────────────────────────────────────────────────────────────

class AltDataSignal(BaseModel):
    source:      str                              # "reddit", "google_trends", "fear_greed", "options_flow"
    signal_type: str                              # "mention_velocity", "search_interest", "sentiment_index", "put_call_ratio"
    symbol:      Optional[str] = None            # None for market-wide signals (e.g. Fear & Greed)
    score:       float                            # 0-100 normalised
    direction:   Literal[-1, 0, 1] = 0           # -1 bearish / 0 neutral / +1 bullish
    raw_value:   Optional[float]   = None        # original un-normalised value
    timestamp:   datetime
    metadata:    dict[str, Any]    = Field(default_factory=dict)

    @field_validator("score")
    @classmethod
    def clamp_score(cls, v: float) -> float:
        return max(0.0, min(100.0, v))


# ─────────────────────────────────────────────────────────────────────────────
# Reddit mention velocity tracker
# ─────────────────────────────────────────────────────────────────────────────

_SUBREDDITS_FOR_MENTIONS = [
    "wallstreetbets", "investing", "stocks", "options",
    "SecurityAnalysis", "StockMarket",
]


class RedditMentionFeed:
    """
    Counts ticker mentions in recent posts/comments across financial subreddits.
    Calculates a velocity score = mentions per hour, normalised 0-100.
    Compares current window against the previous window to detect acceleration.
    """

    def __init__(
        self,
        symbols:       list[str],
        callback:      Optional[Callable[[AltDataSignal], None]] = None,
        poll_interval: int = 0,
        window_minutes: int = 60,
    ):
        self.symbols        = set(s.upper() for s in symbols)
        self.callback       = callback
        self.poll_interval  = poll_interval or ALT_POLL_INTERVAL
        self.window_minutes = window_minutes
        self._stop_event    = asyncio.Event()
        # Rolling counts: symbol → list of (timestamp, count) tuples
        self._windows: dict[str, list[tuple[float, int]]] = {}
        self._baseline: dict[str, float] = {}   # for normalisation

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        reddit = asyncpraw.Reddit(
            client_id     = os.environ["REDDIT_CLIENT_ID"],
            client_secret = os.environ["REDDIT_CLIENT_SECRET"],
            user_agent    = os.getenv("REDDIT_USER_AGENT", "financial-agents/1.0"),
        )
        logger.info("[RedditMentionFeed] Starting mention velocity tracking")
        try:
            while not self._stop_event.is_set():
                counts = await self._count_mentions(reddit)
                for symbol, count in counts.items():
                    await self._emit_signal(symbol, count)
                await asyncio.sleep(self.poll_interval)
        finally:
            await reddit.close()

    async def _count_mentions(self, reddit: asyncpraw.Reddit) -> dict[str, int]:
        counts: dict[str, int] = {s: 0 for s in self.symbols}
        cutoff = time.time() - (self.window_minutes * 60)

        for sub_name in _SUBREDDITS_FOR_MENTIONS:
            try:
                subreddit = await reddit.subreddit(sub_name)
                async for post in subreddit.new(limit=50):
                    if post.created_utc < cutoff:
                        break
                    text = f"{post.title} {post.selftext or ''}".upper()
                    for symbol in self.symbols:
                        # Match $AAPL or standalone AAPL surrounded by non-alpha
                        if f"${symbol}" in text or f" {symbol} " in text or f"\n{symbol}\n" in text:
                            counts[symbol] += 1
            except Exception as exc:
                logger.debug("[RedditMentionFeed] r/%s error: %s", sub_name, exc)

        return {s: c for s, c in counts.items() if c > 0}

    async def _emit_signal(self, symbol: str, count: int) -> None:
        now = time.time()

        # Maintain rolling window
        if symbol not in self._windows:
            self._windows[symbol] = []
        self._windows[symbol].append((now, count))

        # Keep only last 24h for baseline
        cutoff = now - 86_400
        self._windows[symbol] = [
            (ts, c) for ts, c in self._windows[symbol] if ts > cutoff
        ]

        # Velocity = mentions / hour in current window
        velocity = count * (60 / self.window_minutes)

        # Normalise: compare to 24h average
        all_counts = [c for _, c in self._windows[symbol]]
        avg_24h    = sum(all_counts) / len(all_counts) if all_counts else 1.0
        self._baseline[symbol] = avg_24h

        # Score: velocity relative to baseline, capped at 2× = 100
        raw_ratio = velocity / (avg_24h + 1e-9)
        score     = min(raw_ratio * 50, 100)          # 2× baseline → 100

        direction = 1 if raw_ratio > 1.3 else (-1 if raw_ratio < 0.7 else 0)

        signal = AltDataSignal(
            source      = "reddit",
            signal_type = "mention_velocity",
            symbol      = symbol,
            score       = score,
            direction   = direction,
            raw_value   = velocity,
            timestamp   = datetime.now(timezone.utc),
            metadata    = {
                "mentions_in_window": count,
                "window_minutes":     self.window_minutes,
                "avg_24h":            round(avg_24h, 2),
                "velocity_per_hour":  round(velocity, 2),
            },
        )

        if self.callback:
            try:
                result = self.callback(signal)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("[RedditMentionFeed] Callback error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Google Trends feed (via pytrends)
# ─────────────────────────────────────────────────────────────────────────────

class GoogleTrendsFeed:
    """
    Fetches relative search interest for ticker symbols using pytrends.
    pytrends is an unofficial Google Trends API (no auth required).

    Emits AltDataSignal with signal_type="search_interest".
    Score = Google's 0-100 relative interest value for the last 7 days.

    Note: Google Trends enforces rate limits aggressively — use a generous
    poll_interval (≥ 10 min) and batch queries (max 5 symbols per request).
    """

    def __init__(
        self,
        symbols:       list[str],
        callback:      Optional[Callable[[AltDataSignal], None]] = None,
        poll_interval: int = 0,
        batch_size:    int = 4,    # stay ≤ 5 to avoid rate limits
    ):
        self.symbols       = [s.upper() for s in symbols]
        self.callback      = callback
        self.poll_interval = poll_interval or max(ALT_POLL_INTERVAL, 600)
        self.batch_size    = batch_size
        self._prev_scores: dict[str, float] = {}
        self._stop_event   = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        logger.info("[GoogleTrendsFeed] Starting — %d symbols", len(self.symbols))
        while not self._stop_event.is_set():
            # Run pytrends in thread pool (it's synchronous)
            await asyncio.get_event_loop().run_in_executor(None, self._poll_all)
            await asyncio.sleep(self.poll_interval)

    def _poll_all(self) -> None:
        try:
            from pytrends.request import TrendReq
        except ImportError:
            logger.warning("[GoogleTrendsFeed] pytrends not installed — skipping")
            return

        pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25))

        # Process in batches
        for i in range(0, len(self.symbols), self.batch_size):
            if self._stop_event.is_set():
                break
            batch = self.symbols[i: i + self.batch_size]
            try:
                self._poll_batch(pytrends, batch)
                time.sleep(2.0)   # polite delay between batches
            except Exception as exc:
                logger.debug("[GoogleTrendsFeed] Batch %s error: %s", batch, exc)

    def _poll_batch(self, pytrends: Any, symbols: list[str]) -> None:
        # Use "$AAPL" as keywords — more likely to surface financial intent
        kw_map  = {f"${s}": s for s in symbols}
        keywords = list(kw_map.keys())

        pytrends.build_payload(keywords, timeframe="now 7-d", geo="US")
        df = pytrends.interest_over_time()

        if df.empty:
            return

        for kw, symbol in kw_map.items():
            if kw not in df.columns:
                continue

            latest_score = float(df[kw].iloc[-1])
            prev_score   = self._prev_scores.get(symbol, latest_score)
            self._prev_scores[symbol] = latest_score

            # Direction: rising vs falling trend
            recent_avg = float(df[kw].tail(24).mean()) if len(df) >= 24 else latest_score
            direction  = 1 if latest_score > recent_avg * 1.1 else (
                        -1 if latest_score < recent_avg * 0.9 else 0)

            signal = AltDataSignal(
                source      = "google_trends",
                signal_type = "search_interest",
                symbol      = symbol,
                score       = latest_score,
                direction   = direction,
                raw_value   = latest_score,
                timestamp   = datetime.now(timezone.utc),
                metadata    = {
                    "previous_score": round(prev_score, 1),
                    "recent_avg_24h": round(recent_avg, 1),
                    "timeframe":      "7d",
                },
            )

            if self.callback:
                import asyncio as _asyncio
                loop = _asyncio.get_event_loop()
                if loop.is_running():
                    _asyncio.run_coroutine_threadsafe(
                        self._invoke_callback(signal), loop
                    )

    async def _invoke_callback(self, signal: AltDataSignal) -> None:
        if self.callback:
            try:
                result = self.callback(signal)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("[GoogleTrendsFeed] Callback error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Fear & Greed Index (CNN Money)
# ─────────────────────────────────────────────────────────────────────────────

class FearGreedFeed:
    """
    Fetches the CNN Fear & Greed Index via the public API endpoint.
    Market-wide signal — no symbol attached.

    Score interpretation:
      0-24   Extreme Fear  → direction -1
      25-44  Fear          → direction -1
      45-55  Neutral       → direction 0
      56-74  Greed         → direction +1
      75-100 Extreme Greed → direction +1
    """

    _API_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

    def __init__(
        self,
        callback:      Optional[Callable[[AltDataSignal], None]] = None,
        poll_interval: int = 0,
    ):
        self.callback      = callback
        self.poll_interval = poll_interval or max(ALT_POLL_INTERVAL, 600)
        self._stop_event   = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        headers = {
            "User-Agent":  "Mozilla/5.0 (compatible; financial-agents/1.0)",
            "Referer":     "https://money.cnn.com/data/fear-and-greed/",
        }
        async with aiohttp.ClientSession(
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            logger.info("[FearGreedFeed] Starting")
            while not self._stop_event.is_set():
                await self._poll(session)
                await asyncio.sleep(self.poll_interval)

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        try:
            async with session.get(self._API_URL) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.debug("[FearGreedFeed] Fetch error: %s", exc)
            return

        try:
            fg_data = data.get("fear_and_greed", {})
            score   = float(fg_data.get("score", 50))
            rating  = fg_data.get("rating", "Neutral")
        except (KeyError, TypeError, ValueError) as exc:
            logger.debug("[FearGreedFeed] Parse error: %s", exc)
            return

        if score < 45:
            direction = -1
        elif score > 55:
            direction = 1
        else:
            direction = 0

        signal = AltDataSignal(
            source      = "cnn_money",
            signal_type = "fear_greed_index",
            symbol      = None,
            score       = score,
            direction   = direction,
            raw_value   = score,
            timestamp   = datetime.now(timezone.utc),
            metadata    = {
                "rating":              rating,
                "previous_close":      fg_data.get("previous_close"),
                "previous_1_week":     fg_data.get("previous_1_week"),
                "previous_1_month":    fg_data.get("previous_1_month"),
                "previous_1_year":     fg_data.get("previous_1_year"),
            },
        )

        logger.info("[FearGreedFeed] Score: %.1f (%s)", score, rating)

        if self.callback:
            try:
                result = self.callback(signal)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("[FearGreedFeed] Callback error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# CBOE Put/Call Ratio (options flow)
# ─────────────────────────────────────────────────────────────────────────────

class PutCallRatioFeed:
    """
    Fetches the CBOE total put/call ratio.
    A ratio > 1.0 means more puts than calls → bearish sentiment.
    A ratio < 0.7 means heavy call buying → bullish (or potentially frothy).

    Uses CBOE's publicly-accessible delayed data endpoint.
    No API key required.
    """

    _CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/_VIX.json"
    _PCR_URL  = "https://www.cboe.com/data/market-statistics-data-files/"

    def __init__(
        self,
        callback:      Optional[Callable[[AltDataSignal], None]] = None,
        poll_interval: int = 0,
    ):
        self.callback      = callback
        self.poll_interval = poll_interval or max(ALT_POLL_INTERVAL, 900)  # 15 min
        self._stop_event   = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            headers={"User-Agent": "financial-agents/1.0"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            logger.info("[PutCallRatioFeed] Starting")
            while not self._stop_event.is_set():
                await self._poll(session)
                await asyncio.sleep(self.poll_interval)

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        """
        CBOE publishes a daily CSV with put/call ratio.
        We fetch the most recent value via the options statistics page.
        """
        url = "https://cdn.cboe.com/api/global/delayed_quotes/options/statistics.json"
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.debug("[PutCallRatioFeed] Fetch error: %s", exc)
            return

        try:
            # Navigate CBOE JSON structure
            pcr = float(data.get("data", {}).get("put_call_ratio", 1.0))
        except (TypeError, ValueError, KeyError) as exc:
            logger.debug("[PutCallRatioFeed] Parse error: %s", exc)
            return

        # Normalise to 0-100:
        # pcr = 0.5 → very bullish → score ~25
        # pcr = 1.0 → neutral → score ~50
        # pcr = 1.5 → bearish → score ~75
        score     = min(max((pcr / 2.0) * 100, 0), 100)
        direction = -1 if pcr > 1.1 else (1 if pcr < 0.75 else 0)

        signal = AltDataSignal(
            source      = "cboe",
            signal_type = "put_call_ratio",
            symbol      = None,
            score       = score,
            direction   = direction,
            raw_value   = pcr,
            timestamp   = datetime.now(timezone.utc),
            metadata    = {
                "put_call_ratio": pcr,
                "interpretation": (
                    "bearish" if pcr > 1.1 else
                    "bullish" if pcr < 0.75 else "neutral"
                ),
            },
        )

        logger.info("[PutCallRatioFeed] P/C ratio: %.3f", pcr)

        if self.callback:
            try:
                result = self.callback(signal)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("[PutCallRatioFeed] Callback error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Unified alt data feed
# ─────────────────────────────────────────────────────────────────────────────

class UnifiedAltDataFeed:
    """
    Runs all alternative data sources concurrently.

    Usage:
        feed = UnifiedAltDataFeed(symbols=watchlist, callback=handle_alt)
        await feed.run()
    """

    def __init__(
        self,
        symbols:       list[str],
        callback:      Optional[Callable[[AltDataSignal], None]] = None,
        enable_trends: bool = True,
    ):
        self._reddit    = RedditMentionFeed(symbols=symbols, callback=callback)
        self._fear_greed = FearGreedFeed(callback=callback)
        self._pcr       = PutCallRatioFeed(callback=callback)
        self._trends: Optional[GoogleTrendsFeed] = (
            GoogleTrendsFeed(symbols=symbols, callback=callback)
            if enable_trends else None
        )

    def stop(self) -> None:
        self._reddit.stop()
        self._fear_greed.stop()
        self._pcr.stop()
        if self._trends:
            self._trends.stop()

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self._reddit.run()),
            asyncio.create_task(self._fear_greed.run()),
            asyncio.create_task(self._pcr.run()),
        ]
        if self._trends:
            tasks.append(asyncio.create_task(self._trends.run()))

        await asyncio.gather(*tasks, return_exceptions=True)
