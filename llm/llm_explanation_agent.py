from datetime import datetime, timezone
from pathlib import Path

from agents.base_agent import AgentOutput, BaseAgent
from agents.decision_engine import FinalDecision
from llm.llm_client import LLMClient


class LLMExplanationAgent(BaseAgent):
    def __init__(self, client: LLMClient | None = None):
        super().__init__("llm_explanation_agent")
        self.client = client or LLMClient()

        prompt_path = Path("llm/prompts/decision_prompt.txt")
        self.prompt_template = prompt_path.read_text(encoding="utf-8")

    def run(self, data: FinalDecision) -> AgentOutput:
        market = data.agent_outputs["market"]
        risk = data.agent_outputs["risk"]
        signal = data.agent_outputs["signal"]

        prompt = self.prompt_template.format(
            symbol=data.symbol,
            action=data.action,
            confidence=f"{data.confidence:.2%}",
            market_regime=data.market_regime,
            risk_level=data.risk_level,
            market_reason=market.reason,
            risk_reason=risk.reason,
            signal_reason=signal.reason,
        )

        explanation = self.client.generate(prompt)

        return AgentOutput(
            agent_name=self.name,
            decision="LLM_EXPLANATION",
            confidence=data.confidence,
            reason=explanation,
            metadata={
                "source": "groq",
                "model": "llama-3.1-8b-instant",
            },
            timestamp=datetime.now(timezone.utc),
        )