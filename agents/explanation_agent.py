from datetime import datetime, timezone

from agents.base_agent import AgentOutput, BaseAgent


class ExplanationAgent(BaseAgent):
    def __init__(self):
        super().__init__("explanation_agent")

    def run(self, data: dict) -> AgentOutput:
        market = data["market"]
        risk = data["risk"]
        signal = data["signal"]
        final_action = data["final_action"]

        explanation = (
            f"Final decision is {final_action}. "
            f"Market agent detected {market.decision}. "
            f"Risk agent marked risk as {risk.decision}. "
            f"Signal agent suggested {signal.decision}. "
            f"Reason: {signal.reason}"
        )

        confidence = (
            market.confidence + risk.confidence + signal.confidence
        ) / 3

        return AgentOutput(
            agent_name=self.name,
            decision="EXPLANATION",
            confidence=confidence,
            reason=explanation,
            metadata={
                "market_reason": market.reason,
                "risk_reason": risk.reason,
                "signal_reason": signal.reason,
            },
            timestamp=datetime.now(timezone.utc),
        )