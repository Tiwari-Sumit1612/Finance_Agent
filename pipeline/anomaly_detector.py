from dataclasses import dataclass
from typing import Optional

from pipeline.stream_processor import FeatureEvent


@dataclass
class AnomalyResult:
    symbol: str
    is_anomaly: bool
    reason: Optional[str] = None
    severity: str = "normal"


class AnomalyDetector:
    def __init__(
        self,
        return_threshold: float = 0.03,
        volume_spike_threshold: float = 3.0,
        volatility_threshold: float = 0.05,
    ):
        self.return_threshold = return_threshold
        self.volume_spike_threshold = volume_spike_threshold
        self.volatility_threshold = volatility_threshold

    def detect(self, event: FeatureEvent) -> AnomalyResult:
        if event.return_1 is not None and abs(event.return_1) >= self.return_threshold:
            return AnomalyResult(
                symbol=event.symbol,
                is_anomaly=True,
                reason=f"Large price move detected: {event.return_1:.2%}",
                severity="high",
            )

        if (
            event.volume_spike is not None
            and event.volume_spike >= self.volume_spike_threshold
        ):
            return AnomalyResult(
                symbol=event.symbol,
                is_anomaly=True,
                reason=f"Volume spike detected: {event.volume_spike:.2f}x normal volume",
                severity="medium",
            )

        if (
            event.volatility_20 is not None
            and event.volatility_20 >= self.volatility_threshold
        ):
            return AnomalyResult(
                symbol=event.symbol,
                is_anomaly=True,
                reason=f"High volatility detected: {event.volatility_20:.2%}",
                severity="medium",
            )

        return AnomalyResult(
            symbol=event.symbol,
            is_anomaly=False,
            reason=None,
            severity="normal",
        )