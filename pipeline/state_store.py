from typing import Optional
from ingestion.market_feed import MarketTick, TradeBar
from pipeline.windows import RollingWindow
class InMemoryStateStore:
    """
    Stores rolling windows and latest features in memory.
    Later we can replace this with Redis.
    For now, simple dictionary is enough.
    """
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.windows: dict[str, RollingWindow] = {}
        self.latest_features: dict[str, object] = {}

    def get_or_create_window(self, symbol: str) -> RollingWindow:
        if symbol not in self.windows:
            self.windows[symbol] = RollingWindow(max_size=self.window_size)

        return self.windows[symbol]

    def update_with_tick(self, tick: MarketTick) -> RollingWindow:
        window = self.get_or_create_window(tick.symbol)
        window.append_tick(tick)
        return window

    def update_with_bar(self, bar: TradeBar) -> RollingWindow:
        window = self.get_or_create_window(bar.symbol)
        window.append_bar(bar)
        return window

    def save_features(self, symbol: str, features: object) -> None:
        self.latest_features[symbol] = features

    def get_latest_features(self, symbol: str) -> Optional[object]:
        return self.latest_features.get(symbol)