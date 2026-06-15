from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from pipeline.stream_processor import FeatureEvent


@dataclass
class JoinedEvent:
    symbol: str
    features: FeatureEvent
    news: Optional[Any] = None
    macro: Optional[Any] = None
    alt_data: Optional[Any] = None
    timestamp: datetime | None = None


class EventJoiner:
    def join(
        self,
        features: FeatureEvent,
        news: Optional[Any] = None,
        macro: Optional[Any] = None,
        alt_data: Optional[Any] = None,
    ) -> JoinedEvent:
        return JoinedEvent(
            symbol=features.symbol,
            features=features,
            news=news,
            macro=macro,
            alt_data=alt_data,
            timestamp=features.timestamp,
        )