from dataclasses import dataclass
from typing import Optional

import joblib
import pandas as pd

from pipeline.stream_processor import FeatureEvent


FEATURE_COLUMNS = [
    "return_1",
    "return_5",
    "momentum_5",
    "volatility_20",
    "rsi_14",
    "volume_spike",
    "ema_diff",
    "high_low_range",
    "close_open_return",
    "volume_change",
    "price_position_20",
]


@dataclass
class PredictionResult:
    direction: str
    confidence: float
    probability_up: float
    probability_down: float


class SignalPredictor:
    def __init__(self, model_path: str = "ml/artifacts/signal_model.pkl"):
        self.model_path = model_path
        self.model = joblib.load(model_path)

    def predict(self, event: FeatureEvent) -> Optional[PredictionResult]:
        values = {
            "return_1": event.return_1,
            "return_5": event.return_5,
            "momentum_5": event.momentum_5,
            "volatility_20": event.volatility_20,
            "rsi_14": event.rsi_14,
            "volume_spike": event.volume_spike,
        }

        if any(v is None for v in values.values()):
            return None

        X = pd.DataFrame([values], columns=FEATURE_COLUMNS)

        probabilities = self.model.predict_proba(X)[0]

        probability_down = float(probabilities[0])
        probability_up = float(probabilities[1])

        if probability_up >= probability_down:
            return PredictionResult(
                direction="UP",
                confidence=probability_up,
                probability_up=probability_up,
                probability_down=probability_down,
            )

        return PredictionResult(
            direction="DOWN",
            confidence=probability_down,
            probability_up=probability_up,
            probability_down=probability_down,
        )