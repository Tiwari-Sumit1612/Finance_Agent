import math
from collections import defaultdict, deque
from typing import Optional


class CorrelationTracker:
    def __init__(self, window_size: int = 50):
        self.window_size = window_size
        self.returns: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=window_size)
        )

    def update(self, symbol: str, return_value: Optional[float]) -> None:
        if return_value is None:
            return

        self.returns[symbol].append(return_value)

    def correlation(self, symbol_a: str, symbol_b: str) -> Optional[float]:
        a = list(self.returns[symbol_a])
        b = list(self.returns[symbol_b])

        n = min(len(a), len(b))

        if n < 2:
            return None

        a = a[-n:]
        b = b[-n:]

        mean_a = sum(a) / n
        mean_b = sum(b) / n

        numerator = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n))

        denom_a = math.sqrt(sum((x - mean_a) ** 2 for x in a))
        denom_b = math.sqrt(sum((x - mean_b) ** 2 for x in b))

        if denom_a == 0 or denom_b == 0:
            return None

        return numerator / (denom_a * denom_b)