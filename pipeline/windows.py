from collections import deque
from datetime import datetime
from typing import Optional

from ingestion.market_feed import MarketTick, TradeBar


class RollingWindow:
    """
    Stores the latest N market data points for one symbol.

    Example:
    If max_size = 5 and prices come:
    100, 101, 102, 103, 104, 105

    It keeps only:
    101, 102, 103, 104, 105
    """

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.prices: deque[float] = deque(maxlen=max_size)
        self.volumes: deque[float] = deque(maxlen=max_size)
        self.timestamps: deque[datetime] = deque(maxlen=max_size)

    def append_tick(self, tick: MarketTick) -> None:
        self.prices.append(tick.price)
        self.volumes.append(tick.volume)
        self.timestamps.append(tick.timestamp)

    def append_bar(self, bar: TradeBar) -> None:
        self.prices.append(bar.close)
        self.volumes.append(bar.volume)
        self.timestamps.append(bar.timestamp)

    def get_prices(self) -> list[float]:
        return list(self.prices)

    def get_volumes(self) -> list[float]:
        return list(self.volumes)

    def get_latest_price(self) -> Optional[float]:
        if not self.prices:
            return None
        return self.prices[-1]

    def is_ready(self, min_points: int) -> bool:
        return len(self.prices) >= min_points

    def __len__(self) -> int:
        return len(self.prices)