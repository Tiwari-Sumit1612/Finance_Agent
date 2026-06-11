"""
macro_feed.py
─────────────────────────────────────────────────────────────────────────────
Macro-economic and corporate event ingestion.

Sources:
  • SEC EDGAR XBRL API     – 8-K, 10-K, 10-Q filings in near-real-time
  • FRED (St Louis Fed)    – economic indicators (CPI, unemployment, GDP)
  • Earnings Whispers RSS  – pre-market / after-hours earnings schedule
  • Fed Reserve RSS        – FOMC statements, press releases
  • Alpha Vantage (REST)   – earnings calendar (free tier, 25 req/day)

All sources emit typed pydantic events — MacroEvent, EarningsEvent,
FedEvent, EconomicIndicator — so downstream agents can react per-type.

Environment variables:
  ALPHA_VANTAGE_API_KEY   – Alpha Vantage key (free tier available)
  FRED_API_KEY            – FRED API key (free)
  SEC_USER_AGENT          – required by SEC e.g. "YourName yourmail@example.com"
  MACRO_POLL_INTERVAL     – seconds between polls (default 300)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, date, timezone, timedelta
from typing import Literal, Optional, Callable, Any
from xml.etree import ElementTree as ET

import aiohttp
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

MACRO_POLL_INTERVAL = int(os.getenv("MACRO_POLL_INTERVAL", "300"))


# ─────────────────────────────────────────────────────────────────────────────
# Event models
# ─────────────────────────────────────────────────────────────────────────────

class MacroEvent(BaseModel):
    event_type:  Literal["sec_filing", "earnings", "fed", "economic_indicator"]
    title:       str
    description: str
    url:         str
    symbols:     list[str] = Field(default_factory=list)
    timestamp:   datetime
    source:      str
    importance:  Literal["low", "medium", "high"] = "medium"
    metadata:    dict[str, Any] = Field(default_factory=dict)


class EarningsEvent(MacroEvent):
    event_type:   Literal["earnings"] = "earnings"  # type: ignore[assignment]
    symbol:       str
    eps_estimate: Optional[float] = None
    eps_actual:   Optional[float] = None
    revenue_est:  Optional[float] = None
    revenue_act:  Optional[float] = None
    beat:         Optional[bool]  = None
    fiscal_period: str = ""


class FedEvent(MacroEvent):
    event_type:  Literal["fed"] = "fed"  # type: ignore[assignment]
    category:    str = ""   # "FOMC", "speech", "minutes", "press_release"


class EconomicIndicator(MacroEvent):
    event_type:   Literal["economic_indicator"] = "economic_indicator"  # type: ignore[assignment]
    indicator:    str          # "CPI", "NFP", "GDP", etc.
    value:        Optional[float] = None
    prior_value:  Optional[float] = None
    unit:         str = ""
    period:       str = ""


# ─────────────────────────────────────────────────────────────────────────────
# SEC EDGAR filing watcher
# ─────────────────────────────────────────────────────────────────────────────

class SECFilingFeed:
    """
    Polls SEC EDGAR XBRL submissions API for recent 8-K, 10-K, 10-Q filings.
    Free, no API key required. SEC requires a User-Agent header.

    High-importance filing types:
      8-K  – material events (earnings, M&A, leadership changes)
      10-K – annual report
      10-Q – quarterly report
      SC 13D/G – large ownership changes
    """

    _SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
    _COMPANY_SEARCH  = "https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&dateRange=custom&startdt={start}&enddt={end}&forms={forms}"
    _EDGAR_LATEST    = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&dateb=&owner=include&count=20&search_text=&output=atom"

    # Importance by form type
    _IMPORTANCE: dict[str, str] = {
        "8-K":   "high",
        "10-K":  "high",
        "10-Q":  "medium",
        "SC 13D":"high",
        "SC 13G":"medium",
        "S-1":   "medium",
        "DEF 14A":"low",
    }

    def __init__(
        self,
        symbols: list[str],
        callback: Optional[Callable[[MacroEvent], None]] = None,
        form_types: Optional[list[str]] = None,
        poll_interval: int = 0,
    ):
        self.symbols       = [s.upper() for s in symbols]
        self.callback      = callback
        self.form_types    = form_types or ["8-K", "10-K", "10-Q"]
        self.poll_interval = poll_interval or MACRO_POLL_INTERVAL
        self._seen_ids: set[str] = set()
        self._stop_event   = asyncio.Event()
        self._user_agent   = os.getenv(
            "SEC_USER_AGENT", "financial-agents research@example.com"
        )

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        headers = {
            "User-Agent": self._user_agent,
            "Accept":     "application/json, application/atom+xml",
        }
        async with aiohttp.ClientSession(
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as session:
            logger.info("[SECFilingFeed] Starting — watching %s", self.symbols)
            while not self._stop_event.is_set():
                for form in self.form_types:
                    await self._poll_form(session, form)
                    if self._stop_event.is_set():
                        break
                await asyncio.sleep(self.poll_interval)

    async def _poll_form(self, session: aiohttp.ClientSession, form: str) -> None:
        url = self._EDGAR_LATEST.format(form=form.replace(" ", "+"))
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                content = await resp.text()
        except Exception as exc:
            logger.debug("[SECFilingFeed] Fetch error (%s): %s", form, exc)
            return

        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            return

        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            def t(tag: str) -> str:
                el = entry.find(f"atom:{tag}", ns)
                return (el.text or "").strip() if el is not None else ""

            entry_id = t("id")
            if entry_id in self._seen_ids:
                continue
            self._seen_ids.add(entry_id)

            title     = t("title")
            link_el   = entry.find("atom:link", ns)
            url       = link_el.attrib.get("href", "") if link_el is not None else ""
            updated   = t("updated")

            # Extract ticker from summary / title
            summary = t("summary")
            from .news_feed import extract_symbols
            symbols = extract_symbols(f"{title} {summary}")

            try:
                ts = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            except Exception:
                ts = datetime.now(timezone.utc)

            event = MacroEvent(
                event_type  = "sec_filing",
                title       = title,
                description = summary[:300],
                url         = url,
                symbols     = symbols,
                timestamp   = ts,
                source      = "sec_edgar",
                importance  = self._IMPORTANCE.get(form, "low"),
                metadata    = {"form_type": form},
            )

            if self.callback:
                try:
                    result = self.callback(event)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    logger.error("[SECFilingFeed] Callback error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# FRED economic indicators
# ─────────────────────────────────────────────────────────────────────────────

# Series IDs and human names for key economic indicators
FRED_SERIES = {
    "CPIAUCSL":  ("CPI (All Urban Consumers)", "index"),
    "UNRATE":    ("Unemployment Rate",          "%"),
    "GDP":       ("GDP",                        "billion USD"),
    "FEDFUNDS":  ("Federal Funds Rate",         "%"),
    "T10Y2Y":    ("10Y-2Y Treasury Spread",     "%"),
    "VIXCLS":    ("CBOE VIX",                   "index"),
    "DGS10":     ("10-Year Treasury Yield",     "%"),
    "M2SL":      ("M2 Money Supply",            "billion USD"),
}


class FREDIndicatorFeed:
    """
    Polls the St Louis Fed FRED API for economic indicator releases.
    Emits EconomicIndicator events when new data points are published.

    Free tier: 120 requests/minute, 500 series lookups.
    """

    _OBSERVATIONS_URL = (
        "https://api.stlouisfed.org/fred/series/observations"
        "?series_id={series_id}&api_key={api_key}&file_type=json"
        "&sort_order=desc&limit=2"    # latest 2 — compare to detect new release
    )

    def __init__(
        self,
        callback: Optional[Callable[[EconomicIndicator], None]] = None,
        series:   Optional[dict[str, tuple[str, str]]] = None,
        poll_interval: int = 0,
    ):
        self.callback      = callback
        self.series        = series or FRED_SERIES
        self.poll_interval = poll_interval or MACRO_POLL_INTERVAL
        self._api_key      = os.environ["FRED_API_KEY"]
        self._last_date: dict[str, str] = {}
        self._stop_event   = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            logger.info("[FREDIndicatorFeed] Starting — %d series", len(self.series))
            while not self._stop_event.is_set():
                for series_id, (name, unit) in self.series.items():
                    await self._poll_series(session, series_id, name, unit)
                    if self._stop_event.is_set():
                        break
                    await asyncio.sleep(0.5)   # gentle rate limiting
                await asyncio.sleep(self.poll_interval)

    async def _poll_series(
        self,
        session: aiohttp.ClientSession,
        series_id: str,
        name: str,
        unit: str,
    ) -> None:
        url = self._OBSERVATIONS_URL.format(
            series_id=series_id, api_key=self._api_key
        )
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except Exception as exc:
            logger.debug("[FREDIndicatorFeed] %s fetch error: %s", series_id, exc)
            return

        obs = data.get("observations", [])
        if not obs:
            return

        latest   = obs[0]
        prior    = obs[1] if len(obs) > 1 else {}
        date_str = latest.get("date", "")

        # Only emit if we have a genuinely new release date
        if date_str == self._last_date.get(series_id):
            return
        self._last_date[series_id] = date_str

        try:
            value = float(latest.get("value", "nan"))
        except ValueError:
            value = None  # FRED uses "." for missing values

        try:
            prior_value = float(prior.get("value", "nan"))
        except ValueError:
            prior_value = None

        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            ts = datetime.now(timezone.utc)

        indicator = EconomicIndicator(
            title       = f"{name}: {value} {unit}",
            description = (
                f"{name} released at {value} {unit}. "
                f"Prior: {prior_value} {unit}."
            ),
            url         = f"https://fred.stlouisfed.org/series/{series_id}",
            symbols     = [],
            timestamp   = ts,
            source      = "fred",
            importance  = "high" if series_id in ("CPIAUCSL", "UNRATE", "FEDFUNDS") else "medium",
            indicator   = name,
            value       = value,
            prior_value = prior_value,
            unit        = unit,
            period      = date_str,
        )

        logger.info("[FREDIndicatorFeed] New release — %s: %s %s", series_id, value, unit)

        if self.callback:
            try:
                result = self.callback(indicator)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("[FREDIndicatorFeed] Callback error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Earnings calendar (Alpha Vantage)
# ─────────────────────────────────────────────────────────────────────────────

class EarningsCalendarFeed:
    """
    Fetches upcoming and recently-released earnings from Alpha Vantage.
    Emits EarningsEvent for each company.

    Free tier: 25 requests/day — sufficient for daily refresh.
    """

    _AV_URL = (
        "https://www.alphavantage.co/query"
        "?function=EARNINGS_CALENDAR&horizon=3month&apikey={key}"
    )

    def __init__(
        self,
        symbols: list[str],
        callback: Optional[Callable[[EarningsEvent], None]] = None,
        poll_interval: int = 0,
    ):
        self.symbols       = set(s.upper() for s in symbols)
        self.callback      = callback
        self.poll_interval = poll_interval or max(MACRO_POLL_INTERVAL, 3600)  # ≥ 1hr
        self._api_key      = os.environ["ALPHA_VANTAGE_API_KEY"]
        self._seen_ids: set[str] = set()
        self._stop_event   = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        ) as session:
            logger.info("[EarningsCalendarFeed] Starting")
            while not self._stop_event.is_set():
                await self._poll(session)
                await asyncio.sleep(self.poll_interval)

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        url = self._AV_URL.format(key=self._api_key)
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                # Alpha Vantage returns CSV for this endpoint
                content = await resp.text()
        except Exception as exc:
            logger.debug("[EarningsCalendarFeed] Fetch error: %s", exc)
            return

        lines = content.strip().splitlines()
        if len(lines) < 2:
            return

        headers = [h.strip().lower() for h in lines[0].split(",")]

        for line in lines[1:]:
            values = line.split(",")
            row    = dict(zip(headers, [v.strip() for v in values]))

            symbol = row.get("symbol", "").upper()
            if symbol not in self.symbols:
                continue

            report_date  = row.get("reportdate", "")
            fiscal_period = row.get("fiscaldateending", "")
            unique_id    = f"{symbol}:{report_date}:{fiscal_period}"

            if unique_id in self._seen_ids:
                continue
            self._seen_ids.add(unique_id)

            try:
                ts = datetime.strptime(report_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                ts = datetime.now(timezone.utc)

            def _float(key: str) -> Optional[float]:
                v = row.get(key, "").strip()
                try:
                    return float(v) if v else None
                except ValueError:
                    return None

            event = EarningsEvent(
                title         = f"{symbol} Earnings — {report_date}",
                description   = (
                    f"{symbol} reports {fiscal_period}. "
                    f"EPS estimate: {row.get('estimate', 'n/a')}. "
                    f"Currency: {row.get('currency', 'USD')}."
                ),
                url           = f"https://finance.yahoo.com/quote/{symbol}/financials",
                symbols       = [symbol],
                timestamp     = ts,
                source        = "alpha_vantage",
                importance    = "high",
                symbol        = symbol,
                eps_estimate  = _float("estimate"),
                fiscal_period = fiscal_period,
            )

            if self.callback:
                try:
                    result = self.callback(event)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    logger.error("[EarningsCalendarFeed] Callback error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Fed Reserve RSS feed
# ─────────────────────────────────────────────────────────────────────────────

FED_RSS_SOURCES = {
    "fomc_statements": "https://www.federalreserve.gov/feeds/press_monetary.xml",
    "speeches":        "https://www.federalreserve.gov/feeds/speeches.xml",
    "press_releases":  "https://www.federalreserve.gov/feeds/press_all.xml",
}


class FedReserveFeed:
    """
    Polls Federal Reserve RSS feeds for FOMC statements, speeches,
    and press releases.  Emits FedEvent.
    """

    def __init__(
        self,
        callback: Optional[Callable[[FedEvent], None]] = None,
        poll_interval: int = 0,
    ):
        self.callback      = callback
        self.poll_interval = poll_interval or MACRO_POLL_INTERVAL
        self._seen_ids: set[str] = set()
        self._stop_event   = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            headers={"User-Agent": "financial-agents/1.0"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            logger.info("[FedReserveFeed] Starting")
            while not self._stop_event.is_set():
                for category, url in FED_RSS_SOURCES.items():
                    await self._poll(session, category, url)
                await asyncio.sleep(self.poll_interval)

    async def _poll(
        self, session: aiohttp.ClientSession, category: str, url: str
    ) -> None:
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                content = await resp.text()
        except Exception as exc:
            logger.debug("[FedReserveFeed] %s fetch error: %s", category, exc)
            return

        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            return

        for item in root.findall(".//item"):
            def t(tag: str) -> str:
                el = item.find(tag)
                return (el.text or "").strip() if el is not None else ""

            link = t("link")
            if link in self._seen_ids:
                continue
            self._seen_ids.add(link)

            title   = t("title")
            desc    = t("description")
            pub_str = t("pubDate")

            try:
                from email.utils import parsedate_to_datetime
                ts = parsedate_to_datetime(pub_str)
            except Exception:
                ts = datetime.now(timezone.utc)

            event = FedEvent(
                title       = title,
                description = desc[:500],
                url         = link,
                symbols     = [],
                timestamp   = ts,
                source      = "federal_reserve",
                importance  = "high" if "statement" in title.lower() else "medium",
                category    = category,
            )

            if self.callback:
                try:
                    result = self.callback(event)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    logger.error("[FedReserveFeed] Callback error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Unified macro feed
# ─────────────────────────────────────────────────────────────────────────────

class UnifiedMacroFeed:
    """
    Runs all macro sources concurrently under a single callback.

    Usage:
        feed = UnifiedMacroFeed(symbols=watchlist, callback=handle_macro)
        await feed.run()
    """

    def __init__(
        self,
        symbols: list[str],
        callback: Optional[Callable[[MacroEvent], None]] = None,
    ):
        self._sec      = SECFilingFeed(symbols=symbols, callback=callback)    # type: ignore[arg-type]
        self._fred     = FREDIndicatorFeed(callback=callback)                 # type: ignore[arg-type]
        self._earnings = EarningsCalendarFeed(symbols=symbols, callback=callback)  # type: ignore[arg-type]
        self._fed      = FedReserveFeed(callback=callback)                    # type: ignore[arg-type]

    def stop(self) -> None:
        for feed in (self._sec, self._fred, self._earnings, self._fed):
            feed.stop()

    async def run(self) -> None:
        await asyncio.gather(
            self._sec.run(),
            self._fred.run(),
            self._earnings.run(),
            self._fed.run(),
            return_exceptions=True,
        )
