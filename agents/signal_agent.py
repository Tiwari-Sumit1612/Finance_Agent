from datetime import datetime, timezone

from agents.base_agent import AgentOutput, BaseAgent
from pipeline.stream_processor import FeatureEvent


class SignalAgent(BaseAgent):
    def __init__(self):
        super().__init__("signal_agent")

    def run(self, data: FeatureEvent) -> AgentOutput:
        signal = "HOLD"
        confidence = 0.5
        reason = "HOLD because signal strength is not enough."

        if data.return_5 is not None and data.rsi_14 is not None:
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
            metadata={
                "return_5": data.return_5,
                "rsi_14": data.rsi_14,
                "price": data.price,
            },
            timestamp=datetime.now(timezone.utc),
        )