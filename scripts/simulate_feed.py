"""
scripts/simulate_feed.py
─────────────────────────────────────────────────────────────────────────────
Simulates all ingestion feeds locally — NO real API keys needed.

Injects mock MarketTick, NewsItem, MacroEvent, and AltDataSignal events
into the same callback interface that the real feeds use, so you can test
the full pipeline (pipeline/ → agents/ → output/) without any external calls.

Run:
    python scripts/simulate_feed.py                  # all feeds, default symbols
    python scripts/simulate_feed.py --symbols AAPL NVDA TSLA
    python scripts/simulate_feed.py --feed market    # only market ticks
    python scripts/simulate_feed.py --feed news
    python scripts/simulate_feed.py --feed macro
    python scripts/simulate_feed.py --feed alt
    python scripts/simulate_feed.py --anomaly        # inject a spike every 30s
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import time
from datetime import datetime, timezone

from rich.console import Console
from rich.table import Table
from rich import print as rprint

# Make sure ingestion/ is importable when running from project root
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from ingestion import (
    MarketTick, TradeBar,
    NewsItem,
    MacroEvent, EarningsEvent, EconomicIndicator,
    AltDataSignal,
)

logging.basicConfig(level=logging.WARNING)   # silence debug noise during sim
console = Console()


# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────

PRICES: dict[str, float] = {}
RECEIVED: list[dict] = []
STOP_EVENT = asyncio.Event()


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# Callbacks — printed to console in real time
# ─────────────────────────────────────────────────────────────────────────────

def on_market_tick(tick: MarketTick) -> None:
    RECEIVED.append({"type": "tick", "data": tick})
    change = ""
    prev = PRICES.get(tick.symbol)
    if prev:
        pct = ((tick.price - prev) / prev) * 100
        arrow = "▲" if pct > 0 else "▼"
        color = "green" if pct > 0 else "red"
        change = f"[{color}]{arrow} {pct:+.2f}%[/{color}]"
    PRICES[tick.symbol] = tick.price
    console.print(
        f"[cyan]TICK[/cyan]  {tick.symbol:<6} "
        f"[bold]${tick.price:.2f}[/bold]  vol={tick.volume:.0f}  {change}"
    )


def on_news(item: NewsItem) -> None:
    RECEIVED.append({"type": "news", "data": item})
    syms = ", ".join(item.symbols) if item.symbols else "—"
    console.print(
        f"[yellow]NEWS[/yellow]  [{item.source}] {item.title[:70]}  "
        f"[dim]symbols: {syms}[/dim]"
    )


def on_macro(event: MacroEvent) -> None:
    RECEIVED.append({"type": "macro", "data": event})
    imp_color = {"high": "red", "medium": "yellow", "low": "dim"}.get(event.importance, "white")
    console.print(
        f"[magenta]MACRO[/magenta]  [{event.source}] "
        f"[{imp_color}]{event.title[:70]}[/{imp_color}]"
    )


def on_alt(signal: AltDataSignal) -> None:
    RECEIVED.append({"type": "alt", "data": signal})
    dir_str = {1: "[green]▲ bullish[/green]", -1: "[red]▼ bearish[/red]", 0: "[dim]→ neutral[/dim]"}[signal.direction]
    sym = signal.symbol or "MARKET"
    console.print(
        f"[blue]ALT  [/blue]  [{signal.source}] {signal.signal_type}  "
        f"{sym}  score={signal.score:.1f}  {dir_str}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Market tick simulator
# ─────────────────────────────────────────────────────────────────────────────

BASE_PRICES = {
    "AAPL": 189.5, "NVDA": 875.3, "TSLA": 177.2,
    "MSFT": 415.0, "AMZN": 182.4, "GOOGL": 175.6,
    "META": 503.2, "BTC":  67500.0,
}


async def simulate_market(symbols: list[str], anomaly: bool = False) -> None:
    """Emits realistic random-walk ticks at ~2/sec per symbol."""
    # Seed prices
    for s in symbols:
        PRICES[s] = BASE_PRICES.get(s, 100.0)

    tick_count = 0
    anomaly_injected_at = 0.0

    while not STOP_EVENT.is_set():
        for symbol in symbols:
            price = PRICES[symbol]

            # Random walk: ±0.05% per tick normally
            drift = random.gauss(0, 0.0005) * price

            # Inject a sharp spike every 30s if --anomaly flag set
            if anomaly and (time.time() - anomaly_injected_at) > 30:
                spike_symbol = random.choice(symbols)
                if symbol == spike_symbol:
                    drift = price * random.uniform(0.02, 0.05) * random.choice([-1, 1])
                    anomaly_injected_at = time.time()
                    console.print(
                        f"\n[bold red]⚡ ANOMALY INJECTED — {symbol} "
                        f"spike: {drift/price*100:+.2f}%[/bold red]\n"
                    )

            new_price = max(price + drift, 0.01)
            PRICES[symbol] = new_price

            tick = MarketTick(
                symbol    = symbol,
                price     = round(new_price, 4),
                volume    = random.uniform(100, 10_000),
                bid       = round(new_price * 0.9998, 4),
                ask       = round(new_price * 1.0002, 4),
                timestamp = _now(),
                source    = "simulator",
                raw_type  = "trade",
            )
            on_market_tick(tick)

        tick_count += 1
        await asyncio.sleep(0.5)   # ~2 ticks/sec per symbol


# ─────────────────────────────────────────────────────────────────────────────
# News simulator
# ─────────────────────────────────────────────────────────────────────────────

_NEWS_TEMPLATES = [
    ("{symbol} reports record quarterly revenue, beats EPS estimate",        "reuters",     "high"),
    ("{symbol} faces regulatory scrutiny over antitrust concerns",           "cnbc",        "high"),
    ("Analyst upgrades {symbol} to Buy, raises price target to ${target}",   "seekingalpha","medium"),
    ("{symbol} announces $5B share buyback programme",                        "marketwatch", "medium"),
    ("Short interest in {symbol} hits 18-month high",                        "ft",          "medium"),
    ("{symbol} CEO sells $120M in stock, sparking investor concern",         "reuters",     "high"),
    ("Options market pricing 8% move for {symbol} post-earnings",            "cnbc",        "medium"),
    ("r/wallstreetbets targets {symbol} as next short squeeze candidate",     "reddit/wsb",  "low"),
]


async def simulate_news(symbols: list[str]) -> None:
    """Emits a news item every 8-15 seconds."""
    while not STOP_EVENT.is_set():
        symbol   = random.choice(symbols)
        tmpl, src, _ = random.choice(_NEWS_TEMPLATES)
        title = tmpl.format(symbol=symbol, target=random.randint(150, 900))

        item = NewsItem(
            id           = NewsItem.make_id("sim", f"{symbol}{time.time()}"),
            title        = title,
            summary      = title,
            url          = f"https://example.com/news/{symbol.lower()}-{int(time.time())}",
            source       = src,
            source_type  = "rss" if src not in ("reddit/wsb",) else "reddit",
            symbols      = [symbol],
            published_at = _now(),
        )
        on_news(item)
        await asyncio.sleep(random.uniform(8, 15))


# ─────────────────────────────────────────────────────────────────────────────
# Macro event simulator
# ─────────────────────────────────────────────────────────────────────────────

async def simulate_macro(symbols: list[str]) -> None:
    """Emits a macro event every 20-40 seconds."""
    events = [
        lambda: MacroEvent(
            event_type  = "fed",
            title       = "FOMC Minutes: Committee sees two rate cuts in 2025",
            description = "Fed minutes released showing split opinion on timing of rate cuts.",
            url         = "https://federalreserve.gov/monetarypolicy/fomcminutes.htm",
            symbols     = [],
            timestamp   = _now(),
            source      = "federal_reserve",
            importance  = "high",
        ),
        lambda: EconomicIndicator(
            title        = f"CPI: {random.uniform(2.8, 3.9):.1f}% (YoY)",
            description  = "Bureau of Labor Statistics releases monthly CPI data.",
            url          = "https://fred.stlouisfed.org/series/CPIAUCSL",
            symbols      = [],
            timestamp    = _now(),
            source       = "fred",
            importance   = "high",
            indicator    = "CPI",
            value        = round(random.uniform(2.8, 3.9), 2),
            prior_value  = 3.2,
            unit         = "%",
            period       = "2025-04",
        ),
        lambda: EarningsEvent(
            title         = f"{random.choice(symbols)} Earnings Beat",
            description   = "Company reports EPS above consensus estimates.",
            url           = "https://finance.yahoo.com",
            symbols       = [random.choice(symbols)],
            timestamp     = _now(),
            source        = "alpha_vantage",
            importance    = "high",
            symbol        = random.choice(symbols),
            eps_estimate  = round(random.uniform(1.5, 4.0), 2),
            eps_actual    = round(random.uniform(2.0, 4.5), 2),
            beat          = True,
            fiscal_period = "Q1 2025",
        ),
    ]

    while not STOP_EVENT.is_set():
        event = random.choice(events)()
        on_macro(event)
        await asyncio.sleep(random.uniform(20, 40))


# ─────────────────────────────────────────────────────────────────────────────
# Alt-data simulator
# ─────────────────────────────────────────────────────────────────────────────

async def simulate_alt(symbols: list[str]) -> None:
    """Emits alt-data signals every 15-25 seconds."""
    while not STOP_EVENT.is_set():
        symbol = random.choice(symbols)

        # Reddit mention velocity
        score = random.uniform(10, 95)
        on_alt(AltDataSignal(
            source      = "reddit",
            signal_type = "mention_velocity",
            symbol      = symbol,
            score       = score,
            direction   = 1 if score > 65 else (-1 if score < 35 else 0),
            raw_value   = score / 10,
            timestamp   = _now(),
            metadata    = {"mentions_in_window": int(score / 10), "window_minutes": 60},
        ))

        await asyncio.sleep(5)

        # Fear & Greed
        fg_score = random.uniform(20, 80)
        on_alt(AltDataSignal(
            source      = "cnn_money",
            signal_type = "fear_greed_index",
            symbol      = None,
            score       = fg_score,
            direction   = 1 if fg_score > 55 else (-1 if fg_score < 45 else 0),
            raw_value   = fg_score,
            timestamp   = _now(),
        ))

        await asyncio.sleep(random.uniform(15, 25))


# ─────────────────────────────────────────────────────────────────────────────
# Stats printer
# ─────────────────────────────────────────────────────────────────────────────

async def print_stats() -> None:
    """Prints a live summary table every 30 seconds."""
    await asyncio.sleep(30)
    while not STOP_EVENT.is_set():
        table = Table(title="Ingestion Stats (last 30s)", show_lines=True)
        table.add_column("Type",    style="cyan")
        table.add_column("Count",   style="bold")
        table.add_column("Latest",  style="dim")

        types = ["tick", "news", "macro", "alt"]
        for t in types:
            items = [r for r in RECEIVED if r["type"] == t]
            latest = ""
            if items:
                d = items[-1]["data"]
                if hasattr(d, "title"):
                    latest = d.title[:40]
                elif hasattr(d, "symbol"):
                    latest = f"{d.symbol} @ {getattr(d, 'price', d.score):.2f}"
            table.add_row(t, str(len(items)), latest)

        console.print(table)
        console.print(f"[dim]Total events: {len(RECEIVED)}[/dim]\n")
        await asyncio.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    symbols = [s.upper() for s in args.symbols]

    console.print(f"\n[bold green]🚀 Financial Agents — Ingestion Simulator[/bold green]")
    console.print(f"[dim]Symbols: {symbols}[/dim]")
    console.print(f"[dim]Anomaly injection: {'ON' if args.anomaly else 'OFF'}[/dim]\n")

    tasks = []
    feed = getattr(args, "feed", "all")

    if feed in ("all", "market"):
        tasks.append(asyncio.create_task(simulate_market(symbols, args.anomaly)))
    if feed in ("all", "news"):
        tasks.append(asyncio.create_task(simulate_news(symbols)))
    if feed in ("all", "macro"):
        tasks.append(asyncio.create_task(simulate_macro(symbols)))
    if feed in ("all", "alt"):
        tasks.append(asyncio.create_task(simulate_alt(symbols)))

    tasks.append(asyncio.create_task(print_stats()))

    console.print("[bold]Press Ctrl+C to stop.[/bold]\n")
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate financial data ingestion feeds")
    parser.add_argument(
        "--symbols", nargs="+",
        default=["AAPL", "NVDA", "TSLA", "MSFT"],
        help="Ticker symbols to simulate (default: AAPL NVDA TSLA MSFT)",
    )
    parser.add_argument(
        "--feed", choices=["all", "market", "news", "macro", "alt"],
        default="all",
        help="Which feed to simulate (default: all)",
    )
    parser.add_argument(
        "--anomaly", action="store_true",
        help="Inject a random price spike every 30 seconds",
    )

    args = parser.parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")