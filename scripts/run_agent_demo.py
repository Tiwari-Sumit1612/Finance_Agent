from datetime import datetime, timezone
from random import randint

from ingestion.market_feed import MarketTick
from pipeline.stream_processor import StreamProcessor
from pipeline.anomaly_detector import AnomalyDetector
from agents.decision_engine import DecisionEngine


def make_tick(symbol: str, price: float, volume: float) -> MarketTick:
    return MarketTick(
        symbol=symbol,
        price=price,
        volume=volume,
        bid=None,
        ask=None,
        vwap=None,
        timestamp=datetime.now(timezone.utc),
        source="demo",
        raw_type="trade",
    )


def main():
    processor = StreamProcessor()
    detector = AnomalyDetector()
    engine = DecisionEngine()

    price = 100.0

    for i in range(30):
        price += 1
        volume = 1000 + randint(0, 500)

        tick = make_tick("AAPL", price, volume)
        features = processor.process(tick)
        anomaly = detector.detect(features)
        decision = engine.decide(features)

        print("=" * 60)
        print(f"Tick: {i + 1}")
        print(f"Symbol: {features.symbol}")
        print(f"Price: {features.price}")
        print(f"Return 5: {features.return_5}")
        print(f"RSI 14: {features.rsi_14}")
        print(f"Volatility 20: {features.volatility_20}")
        print(f"Volume Spike: {features.volume_spike}")
        print(f"Anomaly: {anomaly.is_anomaly} | {anomaly.reason}")
        print(f"Decision: {decision.action}")
        print(f"Market: {decision.market_regime}")
        print(f"Risk: {decision.risk_level}")
        print(f"Explanation: {decision.explanation}")


if __name__ == "__main__":
    main()