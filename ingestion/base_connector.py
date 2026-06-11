"""
base_connector.py
─────────────────────────────────────────────────────────────────────────────
Abstract base class for all WebSocket / HTTP streaming data connectors.

Responsibilities
  • Exponential back-off reconnection with jitter
  • Per-message validation via pydantic schemas
  • Structured logging with connector identity
  • Health-state tracking (connected / disconnected / degraded)
  • Graceful shutdown via asyncio.Event
  • Dead-letter queue for messages that fail validation

Usage
  Subclass BaseConnector, implement _subscribe() and _parse_message(),
  then call await connector.run() in your async entry-point.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Optional

import websockets
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Health state
# ─────────────────────────────────────────────────────────────────────────────

class ConnectorHealth(str, Enum):
    CONNECTED    = "connected"
    DISCONNECTED = "disconnected"
    DEGRADED     = "degraded"   # connected but error-rate above threshold


@dataclass
class ConnectorStats:
    messages_received: int = 0
    messages_failed:   int = 0
    reconnect_count:   int = 0
    last_message_ts:   float = 0.0
    connected_since:   float = 0.0

    @property
    def error_rate(self) -> float:
        total = self.messages_received + self.messages_failed
        return self.messages_failed / total if total else 0.0

    @property
    def seconds_since_last_message(self) -> float:
        return time.time() - self.last_message_ts if self.last_message_ts else float("inf")


# ─────────────────────────────────────────────────────────────────────────────
# Retry config
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RetryConfig:
    initial_delay:  float = 1.0    # seconds
    max_delay:      float = 60.0   # seconds
    multiplier:     float = 2.0    # exponential factor
    jitter:         float = 0.3    # ± fraction of computed delay
    max_attempts:   int   = 0      # 0 = infinite


# ─────────────────────────────────────────────────────────────────────────────
# Base connector
# ─────────────────────────────────────────────────────────────────────────────

class BaseConnector(ABC):
    """
    Abstract WebSocket connector.  Subclasses must implement:
      • ws_url          – property returning the endpoint URL
      • _subscribe()    – coroutine that sends subscription frames after connect
      • _parse_message() – converts raw JSON dict → validated pydantic model (or None to skip)
    """

    def __init__(
        self,
        name: str,
        retry_cfg: Optional[RetryConfig] = None,
        message_callback: Optional[Callable[[BaseModel], None]] = None,
        dead_letter_limit: int = 500,
    ):
        self.name               = name
        self.retry_cfg          = retry_cfg or RetryConfig()
        self.message_callback   = message_callback
        self._stop_event        = asyncio.Event()
        self._health            = ConnectorHealth.DISCONNECTED
        self._stats             = ConnectorStats()
        self._dead_letters: list[dict] = []
        self._dead_letter_limit = dead_letter_limit
        self._ws: Any           = None  # active websockets.WebSocketClientProtocol

    # ── public API ────────────────────────────────────────────────────────────

    @property
    def health(self) -> ConnectorHealth:
        return self._health

    @property
    def stats(self) -> ConnectorStats:
        return self._stats

    def stop(self) -> None:
        """Signal the run loop to exit gracefully."""
        self._stop_event.set()

    async def run(self) -> None:
        """Main entry-point. Runs forever until stop() is called."""
        attempt = 0
        delay   = self.retry_cfg.initial_delay

        while not self._stop_event.is_set():
            attempt += 1
            try:
                logger.info("[%s] Connecting (attempt %d)…", self.name, attempt)
                await self._connect_and_stream()

            except asyncio.CancelledError:
                logger.info("[%s] Cancelled — exiting.", self.name)
                break

            except Exception as exc:
                self._health = ConnectorHealth.DISCONNECTED
                self._stats.reconnect_count += 1

                if self.retry_cfg.max_attempts and attempt >= self.retry_cfg.max_attempts:
                    logger.error("[%s] Max reconnect attempts reached. Giving up.", self.name)
                    break

                jitter  = random.uniform(-self.retry_cfg.jitter, self.retry_cfg.jitter)
                sleep_t = min(delay * (1 + jitter), self.retry_cfg.max_delay)
                logger.warning(
                    "[%s] Connection error (%s). Retrying in %.1fs…",
                    self.name, exc, sleep_t,
                )
                await asyncio.sleep(sleep_t)
                delay = min(delay * self.retry_cfg.multiplier, self.retry_cfg.max_delay)

        logger.info("[%s] Connector stopped.", self.name)

    async def stream(self) -> AsyncGenerator[BaseModel, None]:
        """
        Alternative to run() for caller-controlled iteration.
        Usage:
            async for msg in connector.stream():
                process(msg)
        """
        queue: asyncio.Queue[BaseModel] = asyncio.Queue(maxsize=10_000)
        original_cb = self.message_callback

        async def _enqueue(msg: BaseModel) -> None:
            await queue.put(msg)
            if original_cb:
                original_cb(msg)

        self.message_callback = _enqueue  # type: ignore[assignment]
        run_task = asyncio.create_task(self.run())

        try:
            while not (self._stop_event.is_set() and queue.empty()):
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield msg
                except asyncio.TimeoutError:
                    continue
        finally:
            run_task.cancel()
            self.message_callback = original_cb

    # ── internals ─────────────────────────────────────────────────────────────

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._health = ConnectorHealth.CONNECTED
            self._stats.connected_since = time.time()
            logger.info("[%s] Connected to %s", self.name, self.ws_url)

            await self._subscribe(ws)

            async for raw in ws:
                if self._stop_event.is_set():
                    break
                await self._handle_raw(raw)
                self._update_health()

    async def _handle_raw(self, raw: str | bytes) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.debug("[%s] JSON decode error: %s", self.name, exc)
            self._stats.messages_failed += 1
            return

        try:
            parsed = self._parse_message(data)
        except ValidationError as exc:
            logger.debug("[%s] Validation error: %s", self.name, exc)
            self._stats.messages_failed += 1
            self._push_dead_letter(data, str(exc))
            return

        if parsed is None:
            return  # connector chose to skip (heartbeat, ack, etc.)

        self._stats.messages_received += 1
        self._stats.last_message_ts = time.time()

        if self.message_callback:
            try:
                result = self.message_callback(parsed)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("[%s] Callback error: %s", self.name, exc)

    def _push_dead_letter(self, data: dict, reason: str) -> None:
        if len(self._dead_letters) >= self._dead_letter_limit:
            self._dead_letters.pop(0)
        self._dead_letters.append({"ts": time.time(), "data": data, "reason": reason})

    def _update_health(self) -> None:
        if self._stats.error_rate > 0.10:
            self._health = ConnectorHealth.DEGRADED
        elif self._health != ConnectorHealth.CONNECTED:
            self._health = ConnectorHealth.CONNECTED

    # ── abstract interface ────────────────────────────────────────────────────

    @property
    @abstractmethod
    def ws_url(self) -> str:
        """WebSocket endpoint URL."""

    @abstractmethod
    async def _subscribe(self, ws: Any) -> None:
        """Send subscription / auth frames after the connection is established."""

    @abstractmethod
    def _parse_message(self, data: dict) -> Optional[BaseModel]:
        """
        Validate and convert a raw JSON dict to a typed model.
        Return None to silently skip the message (heartbeats, acks, etc.).
        Raise pydantic.ValidationError if the message is malformed.
        """
