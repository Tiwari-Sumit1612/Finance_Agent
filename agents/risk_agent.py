from datetime import datetime, timezone

from agents.base_agent import AgentOutput, BaseAgent
from pipeline.stream_processor import FeatureEvent


class RiskAgent(BaseAgent):
    def __init__(self):
        super().__init__("risk_agent")

    def run(self, data: FeatureEvent) -> AgentOutput:
        risk = "unknown"
        confidence = 0.5
        reason = "Not enough risk data."

        if data.volatility_20 is not None and data.volume_spike is not None:
            if data.volatility_20 >= 0.05 or data.volume_spike >= 3.0:
                risk = "high"
                confidence = 0.9
                reason = "Risk is high due to high volatility or abnormal volume spike."
            elif data.volatility_20 >= 0.025 or data.volume_spike >= 2.0:
                risk = "medium"
                confidence = 0.75
                reason = "Risk is medium because volatility or volume activity is elevated."
            else:
                risk = "low"
                confidence = 0.7
                reason = "Risk is low because volatility and volume activity are normal."

        return AgentOutput(
            agent_name=self.name,
            decision=risk,
            confidence=confidence,
            reason=reason,
            metadata={
                "volatility_20": data.volatility_20,
                "volume_spike": data.volume_spike,
            },
            timestamp=datetime.now(timezone.utc),
        )