# Finance Agent — Ingestion Layer: Complete Guide

---

## Part 1 — Why Test Cases Are Failing

The tests import the real modules, which in turn import third-party libraries.
Your environment does not have those installed. Every test therefore fails with
an `ImportError` before a single assertion runs. The missing packages are:

| Module imported | Package to install |
|---|---|
| `pydantic` | `pydantic>=2.0` |
| `websockets` | `websockets` |
| `aiohttp` | `aiohttp` |
| `pytest_asyncio` | `pytest-asyncio` |

Install once:

```bash
pip install pydantic websockets aiohttp pytest pytest-asyncio
```

Beyond the import errors, there is one logic bug in `base_connector.py` that
causes `test_dead_letter_populated_on_validation_error` to fail.

### The bug

The test sends `{"symbol": "AAPL"}` (missing `price`) and expects a dead-letter
entry. But `_handle_raw` calls `_parse_message`, which is implemented in the
`_ConcreteConnector` test helper — not in `BaseConnector` itself. The test
helper raises a `KeyError` (because `price` is missing), not a `ValidationError`.
`_handle_raw` only catches `ValidationError`, so the exception bubbles up
uncaught and the dead-letter queue stays empty.

**Fix** — wrap the `_parse_message` call in a broader try-except:

```python
# base_connector.py  _handle_raw()  — change this block:
try:
    parsed = self._parse_message(data)
except ValidationError as exc:           # ← too narrow
    ...
```

```python
# to:
try:
    parsed = self._parse_message(data)
except (ValidationError, Exception) as exc:   # ← catches KeyError too
    logger.debug("[%s] Parse/validation error: %s", self.name, exc)
    self._stats.messages_failed += 1
    self._push_dead_letter(data, str(exc))
    return
```

Also, `test_handle_raw_skip_returns_none` passes `{"skip": True}` which the
test helper returns `None` for. The assertion checks `messages_received == 0`,
which is correct — skipped messages should NOT increment the counter. This
currently passes, but only if the import error is resolved first.

---

## Part 2 — How to Write the Ingestion Layer From Scratch

The ingestion layer has a simple three-layer pattern:

```
External Source  →  Connector (fetch/stream)  →  Pydantic Model  →  callback()
```

### Step 1 — Define your data model

Every data source produces a typed, validated Python object. Use Pydantic.

```python
# ingestion/models.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Any
from pydantic import BaseModel, Field, field_validator

class MarketTick(BaseModel):
    symbol:    str
    price:     float
    volume:    float
    bid:       Optional[float] = None
    ask:       Optional[float] = None
    timestamp: datetime
    source:    str
    raw_type:  str              # "trade" | "quote" | "bar"

    @field_validator("price", "volume")
    @classmethod
    def must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"Expected >= 0, got {v}")
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_ts(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v
        if isinstance(v, (int, float)):
            # Alpaca sends nanoseconds, Polygon sends milliseconds
            if v > 1e12:
                v = v / 1e9          # ns → s
            elif v > 1e9:
                v = v / 1e3          # ms → s
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        raise ValueError(f"Cannot parse: {v!r}")
```

Why Pydantic?
- **Automatic validation** — bad data raises `ValidationError` before it
  reaches your agent
- **Type coercion** — `"189.5"` (string from JSON) becomes `189.5` (float)
- **Documentation** — the model is the spec

### Step 2 — Write the abstract base connector

The base handles all the repetitive infrastructure — reconnection, logging,
health tracking — so each concrete connector only needs to implement three
things: `ws_url`, `_subscribe()`, and `_parse_message()`.

```python
# ingestion/base_connector.py
import asyncio, json, logging, random, time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional
import websockets
from pydantic import BaseModel, ValidationError

class ConnectorHealth(str, Enum):
    CONNECTED    = "connected"
    DISCONNECTED = "disconnected"
    DEGRADED     = "degraded"

@dataclass
class ConnectorStats:
    messages_received: int   = 0
    messages_failed:   int   = 0
    reconnect_count:   int   = 0

    @property
    def error_rate(self) -> float:
        total = self.messages_received + self.messages_failed
        return self.messages_failed / total if total else 0.0

class BaseConnector(ABC):
    def __init__(self, name: str, message_callback: Optional[Callable] = None):
        self.name             = name
        self.message_callback = message_callback
        self._stop_event      = asyncio.Event()
        self._health          = ConnectorHealth.DISCONNECTED
        self._stats           = ConnectorStats()
        self._dead_letters    = []

    # ── You MUST implement these three ────────────────────────────────────────

    @property
    @abstractmethod
    def ws_url(self) -> str: ...

    @abstractmethod
    async def _subscribe(self, ws: Any) -> None: ...

    @abstractmethod
    def _parse_message(self, data: dict) -> Optional[BaseModel]: ...

    # ── Infrastructure (you get this for free) ────────────────────────────────

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        delay = 1.0
        while not self._stop_event.is_set():
            try:
                await self._connect_and_stream()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._health = ConnectorHealth.DISCONNECTED
                self._stats.reconnect_count += 1
                await asyncio.sleep(min(delay, 60.0))
                delay *= 2

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(self.ws_url) as ws:
            self._health = ConnectorHealth.CONNECTED
            await self._subscribe(ws)
            async for raw in ws:
                if self._stop_event.is_set():
                    break
                await self._handle_raw(raw)

    async def _handle_raw(self, raw: str) -> None:
        # Step 1: parse JSON
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            self._stats.messages_failed += 1
            return

        # Step 2: connector-specific parsing + Pydantic validation
        try:
            parsed = self._parse_message(data)
        except (ValidationError, Exception) as exc:
            self._stats.messages_failed += 1
            self._dead_letters.append({"data": data, "reason": str(exc)})
            return

        # Step 3: None means "skip this message" (heartbeats, acks, etc.)
        if parsed is None:
            return

        self._stats.messages_received += 1

        # Step 4: fire callback
        if self.message_callback:
            result = self.message_callback(parsed)
            if asyncio.iscoroutine(result):
                await result
```

### Step 3 — Implement a concrete connector

Now you just fill in the three abstract methods. Everything else is inherited.

```python
# ingestion/alpaca_feed.py
import os, json
from typing import Any, Optional
from .base_connector import BaseConnector
from .models import MarketTick, TradeBar

class AlpacaMarketFeed(BaseConnector):

    def __init__(self, symbols: list[str], **kwargs):
        super().__init__(name="AlpacaMarketFeed", **kwargs)
        self.symbols     = [s.upper() for s in symbols]
        self._api_key    = os.environ["ALPACA_API_KEY"]
        self._secret_key = os.environ["ALPACA_SECRET_KEY"]
        self._feed       = os.getenv("ALPACA_FEED", "iex")

    @property
    def ws_url(self) -> str:
        return f"wss://stream.data.alpaca.markets/v2/{self._feed}"

    async def _subscribe(self, ws: Any) -> None:
        # 1. Authenticate
        await ws.send(json.dumps({
            "action": "auth",
            "key":    self._api_key,
            "secret": self._secret_key,
        }))
        await ws.recv()    # auth confirmation
        # 2. Subscribe to symbols
        await ws.send(json.dumps({
            "action": "subscribe",
            "trades": self.symbols,
            "quotes": self.symbols,
        }))

    def _parse_message(self, data: dict | list) -> Optional[MarketTick | TradeBar]:
        # Alpaca sends a list of events in one frame
        if isinstance(data, list):
            results = [self._parse_single(item) for item in data]
            hits = [r for r in results if r is not None]
            return hits[-1] if hits else None
        return self._parse_single(data)

    def _parse_single(self, item: dict):
        t = item.get("T")
        if t == "t":    # trade
            return MarketTick(
                symbol=item["S"], price=float(item["p"]),
                volume=float(item.get("s", 0)),
                timestamp=item["t"], source="alpaca", raw_type="trade",
            )
        if t == "q":    # quote
            return MarketTick(
                symbol=item["S"],
                price=(float(item.get("ap",0)) + float(item.get("bp",0))) / 2,
                volume=float(item.get("as",0)),
                bid=float(item.get("bp",0)) or None,
                ask=float(item.get("ap",0)) or None,
                timestamp=item["t"], source="alpaca", raw_type="quote",
            )
        return None   # heartbeats, acks → skip
```

### Step 4 — Wire it all together

```python
# main.py
import asyncio

async def handle_tick(tick):
    print(f"[{tick.source}] {tick.symbol} @ {tick.price:.2f}")

async def main():
    from ingestion.alpaca_feed import AlpacaMarketFeed
    feed = AlpacaMarketFeed(
        symbols=["AAPL", "NVDA"],
        message_callback=handle_tick,
    )
    await feed.run()   # runs forever; call feed.stop() to exit

asyncio.run(main())
```

---

## Part 3 — How Testing Works (So You Can Write Tests Yourself)

### The core idea

You never make real network calls in unit tests. You **mock** every external
dependency and only test the logic inside your code.

Think of it like this: you're testing that a chef can cook the recipe
correctly — so you give them fake pre-prepared ingredients and check that
the dish comes out right. You're not testing whether the market is open.

### Key tools

| Tool | What it does |
|---|---|
| `pytest` | Test runner — finds files named `test_*.py`, runs functions named `test_*` |
| `pytest-asyncio` | Allows `async def test_...` functions to run inside pytest |
| `unittest.mock.MagicMock` | Replaces a synchronous object with a fake |
| `unittest.mock.AsyncMock` | Replaces an async function/method with a fake |
| `patch.dict("os.environ", {...})` | Sets fake environment variables for a test |

### Pattern 1 — Test a pure function

The simplest kind. Call the function, assert the output.

```python
# Test: extract_symbols() finds cashtags correctly
def test_cashtags():
    symbols = extract_symbols("$AAPL is up while $NVDA drops")
    assert "AAPL" in symbols
    assert "NVDA" in symbols

def test_empty_text():
    assert extract_symbols("") == []
```

No mocking needed because `extract_symbols` doesn't touch the network.

### Pattern 2 — Test a class with environment variables

Some classes read `os.environ` at construction time. Use `patch.dict` to inject
fake values without actually setting them on your machine.

```python
def test_ws_url_uses_iex_feed():
    with patch.dict("os.environ", {
        "ALPACA_API_KEY":    "fake_key",
        "ALPACA_SECRET_KEY": "fake_secret",
    }):
        feed = AlpacaMarketFeed(symbols=["AAPL"])

    assert "iex" in feed.ws_url    # default feed should be "iex"
```

The `with` block sets those env vars only for the code inside the block.
After the `with` exits, the original environment is restored.

### Pattern 3 — Test parsing logic (no network)

The `_parse_message` method is a pure function — it takes a `dict` and returns
a Pydantic model. Call it directly with hand-crafted data.

```python
def test_parse_trade_message():
    feed = AlpacaMarketFeed(symbols=["AAPL"])

    # This is what Alpaca's WebSocket actually sends:
    raw = [{"T": "t", "S": "AAPL", "p": "189.5", "s": "500", "t": "2025-01-01T14:00:00Z"}]

    result = feed._parse_message(raw)

    assert isinstance(result, MarketTick)
    assert result.symbol == "AAPL"
    assert result.price  == 189.5      # was a string in raw, now a float ✓
    assert result.raw_type == "trade"
```

You built the input yourself, so you know exactly what to expect.

### Pattern 4 — Test async HTTP polling (mock aiohttp)

This is the most common pattern for the HTTP-polling feeds. You need to mock
the `aiohttp.ClientSession` to return fake HTTP responses.

```python
@pytest.mark.asyncio    # ← tells pytest this is an async test
async def test_parses_rss_and_calls_back():
    received = []

    feed = RSSNewsFeed(
        sources={"test": "https://example.com/rss"},
        callback=received.append,    # collect items in a list
        poll_interval=9999,          # prevent real polling loop
    )

    # ── Build the fake HTTP response ────────────────────────────────────────
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.text   = AsyncMock(return_value=MOCK_RSS)   # fake RSS XML
    mock_resp.raise_for_status = MagicMock()

    # aiohttp uses context managers: `async with session.get(url) as resp:`
    # AsyncMock.__aenter__ is what gets called when you enter the `async with`
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__  = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_resp)

    # ── Call the method under test directly ─────────────────────────────────
    await feed._poll_feed(mock_session, "test", "https://example.com/rss")

    # ── Assert ───────────────────────────────────────────────────────────────
    assert len(received) == 2
    assert any("AAPL" in item.title for item in received)
```

The key insight: instead of starting the full `run()` loop (which would poll
forever), call the internal `_poll_feed` method directly and pass it the fake
session. This is why methods like `_poll_feed` and `_poll_series` are not
hidden inside `run()` — they're kept separate so you can test them.

### Pattern 5 — Test Pydantic validation

Pydantic raises `ValidationError` for bad data. Use `pytest.raises` to assert
that happens.

```python
def test_rejects_negative_price():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        MarketTick(
            symbol="AAPL", price=-10.0, volume=100,
            timestamp="2025-01-01T00:00:00Z",
            source="test", raw_type="trade",
        )
```

`pytest.raises(ValidationError)` acts as a context manager. If the code inside
does NOT raise `ValidationError`, the test fails.

### Pattern 6 — Test deduplication

Call the same method twice with identical data and check results don't double.

```python
@pytest.mark.asyncio
async def test_deduplication():
    received = []
    feed = RSSNewsFeed(sources={"test": "..."}, callback=received.append, poll_interval=9999)

    # ... set up mock_session as before ...

    await feed._poll_feed(mock_session, "test", "https://example.com/rss")
    await feed._poll_feed(mock_session, "test", "https://example.com/rss")

    assert len(received) == 2   # NOT 4 — second poll sees same IDs, skips
```

### Running the tests

```bash
# Run all ingestion tests
pytest tests/unit/test_ingestion.py -v

# Run only tests matching a pattern
pytest tests/unit/test_ingestion.py -v -k "test_market"

# Run with detailed output on failure
pytest tests/unit/test_ingestion.py -v --tb=short

# Run a single test by name
pytest tests/unit/test_ingestion.py::TestAlpacaMarketFeed::test_parse_trade_message -v
```

---

## Part 4 — Complete Pipeline Code

See `pipeline_complete.py` in the outputs folder.
