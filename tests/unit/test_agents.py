from datetime import datetime, timezone

from agents.decision_engine import DecisionEngine
from agents.market_agent import MarketAgent
from agents.risk_agent import RiskAgent
from agents.signal_agent import SignalAgent
from pipeline.stream_processor import FeatureEvent


def make_feature(
    return_5=0.02,
    rsi_14=55,
    momentum_5=3,
    volatility_20=0.01,
    volume_spike=1.2,
):
    return FeatureEvent(
        symbol="AAPL",
        price=150,
        return_1=0.005,
        return_5=return_5,
        momentum_5=momentum_5,
        volatility_20=volatility_20,
        rsi_14=rsi_14,
        vwap_20=148,
        volume_spike=volume_spike,
        timestamp=datetime.now(timezone.utc),
    )


def test_market_agent_detects_uptrend():
    agent = MarketAgent()
    output = agent.run(make_feature(momentum_5=4, volatility_20=0.01))

    assert output.decision == "uptrend"


def test_risk_agent_detects_high_risk():
    agent = RiskAgent()
    output = agent.run(make_feature(volatility_20=0.06, volume_spike=1.0))

    assert output.decision == "high"


def test_signal_agent_returns_buy():
    agent = SignalAgent()
    output = agent.run(make_feature(return_5=0.03, rsi_14=60))

    assert output.decision == "BUY"


def test_decision_engine_blocks_buy_when_risk_high():
    engine = DecisionEngine()

    feature = make_feature(
        return_5=0.03,
        rsi_14=60,
        momentum_5=4,
        volatility_20=0.06,
        volume_spike=1.0,
    )

    decision = engine.decide(feature)

    assert decision.action == "HOLD"
    assert decision.risk_level == "high"


def test_decision_engine_allows_buy_when_risk_low():
    engine = DecisionEngine()

    feature = make_feature(
        return_5=0.03,
        rsi_14=60,
        momentum_5=4,
        volatility_20=0.01,
        volume_spike=1.0,
    )

    decision = engine.decide(feature)

    assert decision.action == "BUY"
    assert decision.risk_level == "low"