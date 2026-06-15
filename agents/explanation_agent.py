from datetime import datetime, timezone
from pathlib import Path

from agents.base_agent import AgentOutput, BaseAgent


class ExplanationAgent(BaseAgent):
    def __init__(self, use_llm: bool = False):
        super().__init__("explanation_agent")
        self.use_llm = use_llm
        self.llm_client = None

        if self.use_llm:
            try:
                from llm.llm_client import LLMClient

                self.llm_client = LLMClient()
            except Exception:
                self.llm_client = None

    def _rule_based_explanation(self, data: dict) -> str:
        market = data["market"]
        risk = data["risk"]
        signal = data["signal"]
        final_action = data["final_action"]

        return (
            f"Final decision is {final_action}. "
            f"Market agent detected {market.decision}. "
            f"Risk agent marked risk as {risk.decision}. "
            f"Signal agent suggested {signal.decision}. "
            f"Reason: {signal.reason}"
        )

    def _llm_explanation(self, data: dict) -> str:
        market = data["market"]
        risk = data["risk"]
        signal = data["signal"]
        final_action = data["final_action"]
        symbol = data["symbol"]
        confidence = data["confidence"]
        market_regime = data["market_regime"]
        risk_level = data["risk_level"]

        prompt_path = Path("llm/prompts/decision_prompt.txt")
        template = prompt_path.read_text(encoding="utf-8")

        prompt = template.format(
            symbol=symbol,
            action=final_action,
            confidence=f"{confidence:.2%}",
            market_regime=market_regime,
            risk_level=risk_level,
            market_reason=market.reason,
            risk_reason=risk.reason,
            signal_reason=signal.reason,
        )

        return self.llm_client.generate(prompt)

    def run(self, data: dict) -> AgentOutput:
        market = data["market"]
        risk = data["risk"]
        signal = data["signal"]

        confidence = (
            market.confidence + risk.confidence + signal.confidence
        ) / 3

        if self.use_llm and self.llm_client is not None:
            try:
                explanation = self._llm_explanation(data)
                source = "llm"
            except Exception as e:
                explanation = self._rule_based_explanation(data)
                source = f"fallback_rule_based: {str(e)}"
        else:
            explanation = self._rule_based_explanation(data)
            source = "rule_based"

        return AgentOutput(
            agent_name=self.name,
            decision="EXPLANATION",
            confidence=confidence,
            reason=explanation,
            metadata={
                "source": source,
                "market_reason": market.reason,
                "risk_reason": risk.reason,
                "signal_reason": signal.reason,
            },
            timestamp=datetime.now(timezone.utc),
        )