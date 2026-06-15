import asyncio
import json
from datetime import datetime, timezone

import websockets
from dotenv import load_dotenv

from ingestion.market_feed import MarketTick
from pipeline.stream_processor import StreamProcessor
from pipeline.anomaly_detector import AnomalyDetector
from agents.decision_engine import DecisionEngine


BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"


def make_tick(data: dict) -> MarketTick:
    return MarketTick(
        symbol="BTCUSDT",
        price=float(data["p"]),
        volume=float(data["q"]),
        bid=None,
        ask=None,
        vwap=None,
        timestamp=datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc),
        source="binance",
        raw_type="trade",
    )


async def main():
    load_dotenv()

    processor = StreamProcessor()
    detector = AnomalyDetector()
    engine = DecisionEngine()

    print("Starting live BTCUSDT agent...")
    print("Press Ctrl+C to stop.")

    async with websockets.connect(BINANCE_WS_URL) as websocket:
        tick_count = 0

        while True:
            message = await websocket.recv()
            data = json.loads(message)

            tick = make_tick(data)
            features = processor.process(tick)
            anomaly = detector.detect(features)
            decision = engine.decide(features)

            tick_count += 1

            print("=" * 70)
            print(f"Tick: {tick_count}")
            print(f"Symbol: {features.symbol}")
            print(f"Price: {features.price}")
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
    asyncio.run(main())