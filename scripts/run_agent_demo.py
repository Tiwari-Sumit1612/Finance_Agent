from datetime import datetime, timezone
from random import randint

from dotenv import load_dotenv

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
    load_dotenv()

    processor = StreamProcessor()
    detector = AnomalyDetector()
    engine = DecisionEngine()

    price = 100.0
    moves = [1, -0.4, 1.2, -0.3, 0.8, 1.1, -0.5, 1.3]

    for i in range(30):
        price += moves[i % len(moves)]
        volume = 1000 + randint(0, 500)

        tick = make_tick("BTCUSDT", price, volume)

        features = processor.process(tick)
        anomaly = detector.detect(features)
        decision = engine.decide(features)

        print("=" * 60)
        print(f"Tick: {i + 1}")
        print(f"Symbol: {features.symbol}")
        print(f"Price: {features.price:.2f}")
        print(f"Return 5: {features.return_5}")
        print(f"RSI 14: {features.rsi_14}")
        print(f"Volatility 20: {features.volatility_20}")
        print(f"Volume Spike: {features.volume_spike}")
        print(f"Anomaly: {anomaly.is_anomaly} | {anomaly.reason}")
        print(f"Decision: {decision.action}")
        print(f"Confidence: {decision.confidence:.2%}")
        print(f"Market: {decision.market_regime}")
        print(f"Risk: {decision.risk_level}")
        print("Explanation:")
        print(decision.explanation)


if __name__ == "__main__":
    main()