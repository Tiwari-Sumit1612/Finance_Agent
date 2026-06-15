import math
from typing import Optional


def percentage_return(prices: list[float], periods: int = 1) -> Optional[float]:
    if len(prices) <= periods:
        return None

    old_price = prices[-periods - 1]
    new_price = prices[-1]

    if old_price == 0:
        return None

    return (new_price - old_price) / old_price


def momentum(prices: list[float], periods: int = 5) -> Optional[float]:
    if len(prices) <= periods:
        return None

    return prices[-1] - prices[-periods - 1]


def rolling_mean(values: list[float], window: int) -> Optional[float]:
    if len(values) < window:
        return None

    recent = values[-window:]
    return sum(recent) / window


def rolling_volatility(prices: list[float], window: int = 20) -> Optional[float]:
    if len(prices) <= window:
        return None

    returns = []

    recent_prices = prices[-window - 1:]

    for i in range(1, len(recent_prices)):
        old_price = recent_prices[i - 1]
        new_price = recent_prices[i]

        if old_price == 0:
            continue

        returns.append((new_price - old_price) / old_price)

    if len(returns) < 2:
        return None

    mean_return = sum(returns) / len(returns)
    variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)

    return math.sqrt(variance)


def rsi(prices: list[float], window: int = 14) -> Optional[float]:
    if len(prices) <= window:
        return None

    gains = []
    losses = []

    recent_prices = prices[-window - 1:]

    for i in range(1, len(recent_prices)):
        change = recent_prices[i] - recent_prices[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains) / window
    avg_loss = sum(losses) / window

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def vwap(prices: list[float], volumes: list[float], window: int = 20) -> Optional[float]:
    if len(prices) < window or len(volumes) < window:
        return None

    recent_prices = prices[-window:]
    recent_volumes = volumes[-window:]

    total_volume = sum(recent_volumes)

    if total_volume == 0:
        return None

    return sum(p * v for p, v in zip(recent_prices, recent_volumes)) / total_volume


def volume_spike_ratio(volumes: list[float], window: int = 20) -> Optional[float]:
    if len(volumes) < window:
        return None

    current_volume = volumes[-1]
    average_volume = sum(volumes[-window:]) / window

    if average_volume == 0:
        return None

    return current_volume / average_volume