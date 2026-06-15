from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from ingestion.market_feed import MarketTick, TradeBar
from pipeline.indicators import (
    percentage_return,
    momentum,
    rolling_volatility,
    rsi,
    vwap,
    volume_spike_ratio,
)
from pipeline.state_store import InMemoryStateStore


class FeatureEvent(BaseModel):
    symbol: str
    price: float
    return_1: Optional[float] = None
    return_5: Optional[float] = None
    momentum_5: Optional[float] = None
    volatility_20: Optional[float] = None
    rsi_14: Optional[float] = None
    vwap_20: Optional[float] = None
    volume_spike: Optional[float] = None
    timestamp: datetime


class StreamProcessor:
    """
    Converts raw MarketTick / TradeBar into FeatureEvent.
    """

    def __init__(self, state_store: Optional[InMemoryStateStore] = None):
        self.state_store = state_store or InMemoryStateStore(window_size=100)

    def process_market_tick(self, tick: MarketTick) -> FeatureEvent:
        window = self.state_store.update_with_tick(tick)

        prices = window.get_prices()
        volumes = window.get_volumes()

        features = FeatureEvent(
            symbol=tick.symbol,
            price=tick.price,
            return_1=percentage_return(prices, periods=1),
            return_5=percentage_return(prices, periods=5),
            momentum_5=momentum(prices, periods=5),
            volatility_20=rolling_volatility(prices, window=20),
            rsi_14=rsi(prices, window=14),
            vwap_20=vwap(prices, volumes, window=20),
            volume_spike=volume_spike_ratio(volumes, window=20),
            timestamp=tick.timestamp,
        )

        self.state_store.save_features(tick.symbol, features)
        return features

    def process_trade_bar(self, bar: TradeBar) -> FeatureEvent:
        window = self.state_store.update_with_bar(bar)

        prices = window.get_prices()
        volumes = window.get_volumes()

        features = FeatureEvent(
            symbol=bar.symbol,
            price=bar.close,
            return_1=percentage_return(prices, periods=1),
            return_5=percentage_return(prices, periods=5),
            momentum_5=momentum(prices, periods=5),
            volatility_20=rolling_volatility(prices, window=20),
            rsi_14=rsi(prices, window=14),
            vwap_20=vwap(prices, volumes, window=20),
            volume_spike=volume_spike_ratio(volumes, window=20),
            timestamp=bar.timestamp,
        )

        self.state_store.save_features(bar.symbol, features)
        return features

    def process(self, event: MarketTick | TradeBar) -> FeatureEvent:
        if isinstance(event, MarketTick):
            return self.process_market_tick(event)

        if isinstance(event, TradeBar):
            return self.process_trade_bar(event)

        raise TypeError(f"Unsupported event type: {type(event)}")