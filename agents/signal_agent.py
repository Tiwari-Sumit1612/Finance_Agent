from datetime import datetime, timezone

from agents.base_agent import AgentOutput, BaseAgent
from ml.predictor import SignalPredictor
from pipeline.stream_processor import FeatureEvent
class SignalAgent(BaseAgent):
    def __init__(self, predictor: SignalPredictor | None = None):
        super().__init__("signal_agent")
        self.predictor = predictor

        if self.predictor is None:
            try:
                self.predictor = SignalPredictor()
            except FileNotFoundError:
                self.predictor = None

    def run(self, data: FeatureEvent) -> AgentOutput:
        signal = "HOLD"
        confidence = 0.5
        reason = "HOLD because signal strength is not enough."
        metadata = {
            "return_5": data.return_5,
            "rsi_14": data.rsi_14,
            "price": data.price,
            "ml_used": False,
        }

        prediction = None

        if self.predictor is not None:
            prediction = self.predictor.predict(data)

        if prediction is not None:
            metadata.update(
                {
                    "ml_used": True,
                    "ml_direction": prediction.direction,
                    "ml_confidence": prediction.confidence,
                    "probability_up": prediction.probability_up,
                    "probability_down": prediction.probability_down,
                }
            )

            if prediction.direction == "UP" and prediction.confidence >= 0.65:
                signal = "BUY"
                confidence = prediction.confidence
                reason = (
                    f"BUY because ML model predicts upward movement "
                    f"with {prediction.confidence:.2%} confidence."
                )

            elif prediction.direction == "DOWN" and prediction.confidence >= 0.65:
                signal = "SELL"
                confidence = prediction.confidence
                reason = (
                    f"SELL because ML model predicts downward movement "
                    f"with {prediction.confidence:.2%} confidence."
                )

            else:
                signal = "HOLD"
                confidence = prediction.confidence
                reason = "HOLD because ML model confidence is not strong enough."

        elif data.return_5 is not None and data.rsi_14 is not None:
            if data.return_5 > 0.015 and data.rsi_14 < 70:
                signal = "BUY"
                confidence = 0.78
                reason = "BUY because short-term return is positive and RSI is not overbought."
            elif data.return_5 < -0.015 and data.rsi_14 > 30:
                signal = "SELL"
                confidence = 0.78
                reason = "SELL because short-term return is negative and RSI is not oversold."

        return AgentOutput(
            agent_name=self.name,
            decision=signal,
            confidence=confidence,
            reason=reason,
            metadata=metadata,
            timestamp=datetime.now(timezone.utc),
        )
    
