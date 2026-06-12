"""
news_feed.py
─────────────────────────────────────────────────────────────────────────────
Real-time financial news ingestion.

Sources:
  • RSS feeds   – Bloomberg, Reuters, FT, CNBC, MarketWatch, Seeking Alpha
  • Reddit      – r/wallstreetbets, r/investing, r/stocks (via PRAW async)
  • Twitter/X   – filtered stream on $cashtags (requires Elevated API access)

All sources produce a normalised NewsItem model.

Polling schedule:
  RSS   – every RSS_POLL_INTERVAL seconds  (default 60)
  Reddit– every REDDIT_POLL_INTERVAL seconds (default 120)
  Twitter – persistent filtered stream (no polling)

Environment variables:
  REDDIT_CLIENT_ID      – Reddit OAuth client ID
  REDDIT_CLIENT_SECRET  – Reddit OAuth secret
  REDDIT_USER_AGENT     – e.g. "financial-agents/1.0"
  TWITTER_BEARER_TOKEN  – Twitter API v2 Bearer token
  RSS_POLL_INTERVAL     – seconds between RSS polls (default 60)
  REDDIT_POLL_INTERVAL  – seconds between Reddit polls (default 120)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional, Callable, AsyncGenerator
from xml.etree import ElementTree as ET

import aiohttp
# import asyncpraw
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Normalised news model
# ─────────────────────────────────────────────────────────────────────────────

class NewsItem(BaseModel):
    id:           str               # stable hash of (source + url/id)
    title:        str
    summary:      str
    url:          str
    source:       str               # "reuters", "reddit", "twitter", etc.
    source_type:  str               # "rss" | "reddit" | "twitter"
    symbols:      list[str]         # $AAPL → ["AAPL"]  (extracted from text)
    sentiment_raw: Optional[float]  = None  # filled later by SentimentAgent
    published_at: datetime
    ingested_at:  datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def make_id(cls, source: str, unique_str: str) -> str:
        return hashlib.sha256(f"{source}:{unique_str}".encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# Symbol extractor (cashtags + plain tickers)
# ─────────────────────────────────────────────────────────────────────────────

_CASHTAG_RE = re.compile(r'\$([A-Z]{1,5})')
_KNOWN_TICKERS: set[str] = set()          # populated at runtime from watchlist


def extract_symbols(text: str, watchlist: Optional[set[str]] = None) -> list[str]:
    """Extract ticker symbols from free text."""
    found: set[str] = set()

    # $CASHTAGS are unambiguous
    found.update(m.upper() for m in _CASHTAG_RE.findall(text))

    # Cross-reference uppercase words against watchlist
    if watchlist:
        words = set(re.findall(r'\b[A-Z]{1,5}\b', text))
        found.update(words & watchlist)

    return sorted(found)


# ─────────────────────────────────────────────────────────────────────────────
# RSS feed poller
# ─────────────────────────────────────────────────────────────────────────────

RSS_SOURCES = {
    "reuters":     "https://feeds.reuters.com/reuters/businessNews",
    "cnbc":        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "marketwatch": "https://feeds.marketwatch.com/marketwatch/topstories/",
    "seekingalpha":"https://seekingalpha.com/market_currents.xml",
    "ft":          "https://www.ft.com/?format=rss",
}


class RSSNewsFeed:
    """
    Polls multiple RSS feeds on a configurable interval.
    Deduplicates via an in-memory seen-IDs set (resets on restart — acceptable
    because the pipeline is stateless and downstream handles idempotency).
    """

    def __init__(
        self,
        sources: Optional[dict[str, str]] = None,
        callback: Optional[Callable[[NewsItem], None]] = None,
        poll_interval: int = 0,
        watchlist: Optional[set[str]] = None,
    ):
        self.sources       = sources or RSS_SOURCES
        self.callback      = callback
        self.poll_interval = poll_interval or int(os.getenv("RSS_POLL_INTERVAL", "60"))
        self.watchlist     = watchlist or set()
        self._seen_ids: set[str] = set()
        self._stop_event   = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        logger.info("[RSSNewsFeed] Starting — %d sources, interval=%ds",
                    len(self.sources), self.poll_interval)
        async with aiohttp.ClientSession(
            headers={"User-Agent": "financial-agents/1.0"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            while not self._stop_event.is_set():
                tasks = [
                    self._poll_feed(session, name, url)
                    for name, url in self.sources.items()
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.warning("[RSSNewsFeed] Poll error: %s", r)

                await asyncio.sleep(self.poll_interval)

    async def _poll_feed(
        self, session: aiohttp.ClientSession, name: str, url: str
    ) -> None:
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                content = await resp.text()
        except Exception as exc:
            logger.debug("[RSSNewsFeed] %s fetch failed: %s", name, exc)
            return

        try:
            root = ET.fromstring(content)
        except ET.ParseError as exc:
            logger.debug("[RSSNewsFeed] %s XML parse error: %s", name, exc)
            return

        # Works for both RSS 2.0 and Atom
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)

        for item in items:
            news = self._parse_rss_item(item, name, ns)
            if news and news.id not in self._seen_ids:
                self._seen_ids.add(news.id)
                if self.callback:
                    try:
                        result = self.callback(news)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as exc:
                        logger.error("[RSSNewsFeed] Callback error: %s", exc)

    def _parse_rss_item(
        self, item: ET.Element, source_name: str, ns: dict
    ) -> Optional[NewsItem]:
        def text(tag: str) -> str:
            el = item.find(tag)
            if el is None:
             el = item.find(f"atom:{tag}", ns)
            return (el.text or "").strip() if el is not None else ""

        title   = text("title")
        link    = text("link") or text("id")
        summary = text("description") or text("summary") or text("content")
        pub_str = text("pubDate") or text("updated") or text("published")

        if not title or not link:
            return None

        try:
            published_at = parsedate_to_datetime(pub_str)
        except Exception:
            try:
                published_at = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
            except Exception:
                published_at = datetime.now(timezone.utc)

        full_text = f"{title} {summary}"
        symbols   = extract_symbols(full_text, self.watchlist)

        return NewsItem(
            id           = NewsItem.make_id(source_name, link),
            title        = title,
            summary      = summary[:500],
            url          = link,
            source       = source_name,
            source_type  = "rss",
            symbols      = symbols,
            published_at = published_at,
        )

    async def stream(self) -> AsyncGenerator[NewsItem, None]:
        queue: asyncio.Queue[NewsItem] = asyncio.Queue(maxsize=5_000)
        original_cb = self.callback

        async def _enqueue(item: NewsItem) -> None:
            await queue.put(item)
            if original_cb:
                original_cb(item)

        self.callback = _enqueue  # type: ignore[assignment]
        run_task = asyncio.create_task(self.run())

        try:
            while not self._stop_event.is_set() or not queue.empty():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield item
                except asyncio.TimeoutError:
                    continue
        finally:
            run_task.cancel()
            self.callback = original_cb


# # ─────────────────────────────────────────────────────────────────────────────
# # Reddit feed poller (r/wallstreetbets, r/investing, r/stocks)
# # ─────────────────────────────────────────────────────────────────────────────

# REDDIT_SUBREDDITS = ["wallstreetbets", "investing", "stocks", "options"]


# class RedditNewsFeed:
#     """
#     Polls Reddit hot/new posts from financial subreddits.
#     Uses asyncpraw (official async Reddit API wrapper).
#     """

#     def __init__(
#         self,
#         subreddits: Optional[list[str]] = None,
#         callback:   Optional[Callable[[NewsItem], None]] = None,
#         poll_interval: int = 0,
#         post_limit: int = 25,
#         watchlist:  Optional[set[str]] = None,
#     ):
#         self.subreddits    = subreddits or REDDIT_SUBREDDITS
#         self.callback      = callback
#         self.poll_interval = poll_interval or int(os.getenv("REDDIT_POLL_INTERVAL", "120"))
#         self.post_limit    = post_limit
#         self.watchlist     = watchlist or set()
#         self._seen_ids: set[str] = set()
#         self._stop_event   = asyncio.Event()

#     def stop(self) -> None:
#         self._stop_event.set()

#     async def run(self) -> None:
#         reddit = asyncpraw.Reddit(
#             client_id     = os.environ["REDDIT_CLIENT_ID"],
#             client_secret = os.environ["REDDIT_CLIENT_SECRET"],
#             user_agent    = os.getenv("REDDIT_USER_AGENT", "financial-agents/1.0"),
#         )
#         logger.info("[RedditNewsFeed] Starting — subreddits: %s", self.subreddits)

#         try:
#             while not self._stop_event.is_set():
#                 for sub_name in self.subreddits:
#                     if self._stop_event.is_set():
#                         break
#                     await self._poll_subreddit(reddit, sub_name)

#                 await asyncio.sleep(self.poll_interval)
#         finally:
#             await reddit.close()

#     async def _poll_subreddit(self, reddit: asyncpraw.Reddit, sub_name: str) -> None:
#         try:
#             subreddit = await reddit.subreddit(sub_name)
#             async for post in subreddit.new(limit=self.post_limit):
#                 if post.id in self._seen_ids:
#                     continue
#                 self._seen_ids.add(post.id)

#                 full_text = f"{post.title} {post.selftext or ''}"
#                 symbols   = extract_symbols(full_text, self.watchlist)

#                 item = NewsItem(
#                     id           = NewsItem.make_id("reddit", post.id),
#                     title        = post.title,
#                     summary      = (post.selftext or "")[:500],
#                     url          = f"https://reddit.com{post.permalink}",
#                     source       = f"reddit/{sub_name}",
#                     source_type  = "reddit",
#                     symbols      = symbols,
#                     published_at = datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
#                 )

#                 if self.callback:
#                     try:
#                         result = self.callback(item)
#                         if asyncio.iscoroutine(result):
#                             await result
#                     except Exception as exc:
#                         logger.error("[RedditNewsFeed] Callback error: %s", exc)

#         except Exception as exc:
#             logger.warning("[RedditNewsFeed] Error polling r/%s: %s", sub_name, exc)

# ─────────────────────────────────────────────────────────────────────────────
# Polygon News Feed
# ─────────────────────────────────────────────────────────────────────────────

class PolygonNewsFeed:
    """
    Polls Polygon.io news endpoint.
    Requires:
        POLYGON_API_KEY
        POLYGON_POLL_INTERVAL (optional)
    """

    def __init__(
        self,
        callback: Optional[Callable[[NewsItem], None]] = None,
        poll_interval: int = 0,
        watchlist: Optional[set[str]] = None,
        limit: int = 50,
    ):
        self.callback = callback
        self.poll_interval = poll_interval or int(
            os.getenv("POLYGON_POLL_INTERVAL", "60")
        )
        self.watchlist = watchlist or set()
        self.limit = limit
        self.api_key = os.environ["POLYGON_API_KEY"]

        self._seen_ids: set[str] = set()
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        logger.info("[PolygonNewsFeed] Starting")

        async with aiohttp.ClientSession() as session:
            while not self._stop_event.is_set():
                try:
                    await self._poll_news(session)
                except Exception as exc:
                    logger.warning(
                        "[PolygonNewsFeed] Poll error: %s",
                        exc
                    )

                await asyncio.sleep(self.poll_interval)

    async def _poll_news(
        self,
        session: aiohttp.ClientSession,
    ) -> None:

        url = (
            "https://api.polygon.io/v2/reference/news"
            f"?limit={self.limit}"
            f"&apiKey={self.api_key}"
        )

        async with session.get(url) as resp:
            resp.raise_for_status()
            payload = await resp.json()

        for article in payload.get("results", []):

            article_id = str(article.get("id"))

            if article_id in self._seen_ids:
                continue

            self._seen_ids.add(article_id)

            title = article.get("title", "")
            summary = article.get("description", "")
            tickers = article.get("tickers", [])

            try:
                published_at = datetime.fromisoformat(
                    article["published_utc"].replace(
                        "Z",
                        "+00:00"
                    )
                )
            except Exception:
                published_at = datetime.now(timezone.utc)

            item = NewsItem(
                id=NewsItem.make_id(
                    "polygon",
                    article_id
                ),
                title=title,
                summary=summary[:500],
                url=article.get("article_url", ""),
                source="polygon",
                source_type="polygon",
                symbols=tickers,
                published_at=published_at,
            )

            if self.callback:
                try:
                    result = self.callback(item)

                    if asyncio.iscoroutine(result):
                        await result

                except Exception as exc:
                    logger.error(
                        "[PolygonNewsFeed] Callback error: %s",
                        exc
                    )

# ─────────────────────────────────────────────────────────────────────────────
# Twitter/X filtered stream
# ─────────────────────────────────────────────────────────────────────────────

class TwitterNewsFeed:
    """
    Connects to Twitter API v2 filtered stream.
    Filters on cashtags for watchlist symbols.
    Requires Elevated or Academic Research access (Basic tier does not
    support filtered stream).
    """

    _RULES_URL  = "https://api.twitter.com/2/tweets/search/stream/rules"
    _STREAM_URL = (
        "https://api.twitter.com/2/tweets/search/stream"
        "?tweet.fields=created_at,author_id,text,entities"
        "&expansions=author_id"
    )

    def __init__(
        self,
        symbols: list[str],
        callback: Optional[Callable[[NewsItem], None]] = None,
        watchlist: Optional[set[str]] = None,
    ):
        self.symbols      = [s.upper() for s in symbols]
        self.callback     = callback
        self.watchlist    = watchlist or set(self.symbols)
        self._bearer      = os.environ["TWITTER_BEARER_TOKEN"]
        self._stop_event  = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._bearer}"}

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            await self._set_rules(session)
            await self._stream(session)

    async def _set_rules(self, session: aiohttp.ClientSession) -> None:
        """Delete old rules and create fresh cashtag rules."""
        # Fetch existing
        async with session.get(self._RULES_URL, headers=self._headers()) as resp:
            existing = await resp.json()

        rule_ids = [r["id"] for r in existing.get("data", [])]
        if rule_ids:
            await session.post(
                self._RULES_URL,
                headers=self._headers(),
                json={"delete": {"ids": rule_ids}},
            )

        # Build new rule: ($AAPL OR $NVDA OR ...) lang:en -is:retweet
        cashtags = " OR ".join(f"${s}" for s in self.symbols[:25])  # max 25 per rule
        rule = f"({cashtags}) lang:en -is:retweet"
        await session.post(
            self._RULES_URL,
            headers=self._headers(),
            json={"add": [{"value": rule, "tag": "financial_watchlist"}]},
        )
        logger.info("[TwitterNewsFeed] Rule set: %s", rule[:80])

    async def _stream(self, session: aiohttp.ClientSession) -> None:
        logger.info("[TwitterNewsFeed] Connecting to filtered stream…")
        try:
            async with session.get(
                self._STREAM_URL,
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=None, sock_read=90),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(f"Twitter stream {resp.status}: {body[:200]}")

                async for line in resp.content:
                    if self._stop_event.is_set():
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = __import__("json").loads(line)
                        item    = self._parse_tweet(payload)
                        if item and self.callback:
                            result = self.callback(item)
                            if asyncio.iscoroutine(result):
                                await result
                    except Exception as exc:
                        logger.debug("[TwitterNewsFeed] Parse error: %s", exc)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("[TwitterNewsFeed] Stream error: %s", exc)
            raise

    def _parse_tweet(self, payload: dict) -> Optional[NewsItem]:
        data = payload.get("data", {})
        text = data.get("text", "")
        if not text:
            return None

        tweet_id = data.get("id", "")
        created  = data.get("created_at", "")

        try:
            published_at = datetime.fromisoformat(created.replace("Z", "+00:00"))
        except Exception:
            published_at = datetime.now(timezone.utc)

        symbols = extract_symbols(text, self.watchlist)

        return NewsItem(
            id           = NewsItem.make_id("twitter", tweet_id),
            title        = text[:280],
            summary      = text[:280],
            url          = f"https://twitter.com/i/web/status/{tweet_id}",
            source       = "twitter",
            source_type  = "twitter",
            symbols      = symbols,
            published_at = published_at,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Unified news feed  (runs all sources concurrently)
# ─────────────────────────────────────────────────────────────────────────────

class UnifiedNewsFeed:
    """
    Runs RSS + Reddit (+ optionally Twitter) concurrently and routes all
    events through a single callback.

    Usage:
        feed = UnifiedNewsFeed(symbols=watchlist, callback=handle_news)
        await feed.run()
    """

    def __init__(
        self,
        symbols: list[str],
        callback: Optional[Callable[[NewsItem], None]] = None,
        enable_twitter: bool = False,
    ):
        watchlist  = set(s.upper() for s in symbols)
        self._rss  = RSSNewsFeed(callback=callback, watchlist=watchlist)
        self._polygon = PolygonNewsFeed(
    callback=callback,
    watchlist=watchlist
)
        self._twitter: Optional[TwitterNewsFeed] = (
            TwitterNewsFeed(symbols=symbols, callback=callback, watchlist=watchlist)
            if enable_twitter else None
        )

    def stop(self) -> None:
        self._rss.stop()
        self._polygon.stop()
        if self._twitter:
            self._twitter.stop()

    async def run(self) -> None:
        tasks = [
    asyncio.create_task(self._rss.run()),
    asyncio.create_task(self._polygon.run()),
]
        if self._twitter:
            tasks.append(asyncio.create_task(self._twitter.run()))

        await asyncio.gather(*tasks, return_exceptions=True)