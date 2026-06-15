from datetime import datetime, timezone

from pydantic import BaseModel, Field

from agents.base_agent import AgentOutput
from agents.explanation_agent import ExplanationAgent
from agents.market_agent import MarketAgent
from agents.risk_agent import RiskAgent
from agents.signal_agent import SignalAgent
from pipeline.stream_processor import FeatureEvent


class FinalDecision(BaseModel):
    symbol: str
    action: str
    confidence: float = Field(ge=0.0, le=1.0)
    market_regime: str
    risk_level: str
    explanation: str
    timestamp: datetime
    agent_outputs: dict[str, AgentOutput]


class DecisionEngine:
    def __init__(self):
        self.market_agent = MarketAgent()
        self.risk_agent = RiskAgent()
        self.signal_agent = SignalAgent()
        self.explanation_agent = ExplanationAgent()

    def decide(self, event: FeatureEvent) -> FinalDecision:
        market_output = self.market_agent.run(event)
        risk_output = self.risk_agent.run(event)
        signal_output = self.signal_agent.run(event)

        final_action = signal_output.decision

        if risk_output.decision == "high" and final_action == "BUY":
            final_action = "HOLD"

        if market_output.decision == "volatile" and final_action == "BUY":
            final_action = "HOLD"

        explanation_output = self.explanation_agent.run(
            {
                "market": market_output,
                "risk": risk_output,
                "signal": signal_output,
                "final_action": final_action,
            }
        )

        confidence = (
            market_output.confidence
            + risk_output.confidence
            + signal_output.confidence
        ) / 3

        return FinalDecision(
            symbol=event.symbol,
            action=final_action,
            confidence=confidence,
            market_regime=market_output.decision,
            risk_level=risk_output.decision,
            explanation=explanation_output.reason,
            timestamp=datetime.now(timezone.utc),
            agent_outputs={
                "market": market_output,
                "risk": risk_output,
                "signal": signal_output,
                "explanation": explanation_output,
            },
        )