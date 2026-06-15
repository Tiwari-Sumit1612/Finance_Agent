from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class AgentOutput(BaseModel):
    agent_name: str
    decision: str
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    metadata: dict[str, Any] = {}
    timestamp: datetime


class BaseAgent(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def run(self, data: Any) -> AgentOutput:
        pass