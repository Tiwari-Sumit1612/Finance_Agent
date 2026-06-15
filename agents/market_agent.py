from datetime import datetime, timezone

from agents.base_agent import AgentOutput, BaseAgent
from pipeline.stream_processor import FeatureEvent


class MarketAgent(BaseAgent):
    def __init__(self):
        super().__init__("market_agent")

    def run(self, data: FeatureEvent) -> AgentOutput:
        regime = "unknown"
        confidence = 0.5
        reason = "Not enough market data."

        if data.volatility_20 is not None and data.momentum_5 is not None:
            if data.volatility_20 >= 0.04:
                regime = "volatile"
                confidence = 0.85
                reason = "Market is volatile because rolling volatility is high."
            elif data.momentum_5 > 2:
                regime = "uptrend"
                confidence = 0.8
                reason = "Market is in uptrend because recent momentum is positive."
            elif data.momentum_5 < -2:
                regime = "downtrend"
                confidence = 0.8
                reason = "Market is in downtrend because recent momentum is negative."
            else:
                regime = "ranging"
                confidence = 0.65
                reason = "Market is ranging because momentum and volatility are moderate."

        return AgentOutput(
            agent_name=self.name,
            decision=regime,
            confidence=confidence,
            reason=reason,
            metadata={
                "momentum_5": data.momentum_5,
                "volatility_20": data.volatility_20,
            },
            timestamp=datetime.now(timezone.utc),
        )