from datetime import datetime, timezone

from ingestion.market_feed import MarketTick
from pipeline.stream_processor import StreamProcessor
from pipeline.anomaly_detector import AnomalyDetector
from pipeline.correlation_tracker import CorrelationTracker
from pipeline.event_joiner import EventJoiner


def make_tick(symbol: str, price: float, volume: float = 1000):
    return MarketTick(
        symbol=symbol,
        price=price,
        volume=volume,
        bid=None,
        ask=None,
        vwap=None,
        timestamp=datetime.now(timezone.utc),
        source="test",
        raw_type="trade",
    )


def test_stream_processor_creates_features_after_enough_ticks():
    processor = StreamProcessor()

    feature = None

    for i in range(25):
        tick = make_tick("AAPL", price=100 + i, volume=1000 + i * 10)
        feature = processor.process(tick)

    assert feature is not None
    assert feature.symbol == "AAPL"
    assert feature.price == 124
    assert feature.return_1 is not None
    assert feature.return_5 is not None
    assert feature.momentum_5 is not None
    assert feature.volatility_20 is not None
    assert feature.rsi_14 is not None
    assert feature.vwap_20 is not None
    assert feature.volume_spike is not None


def test_early_ticks_have_none_features():
    processor = StreamProcessor()

    tick = make_tick("AAPL", price=100)
    feature = processor.process(tick)

    assert feature.symbol == "AAPL"
    assert feature.return_1 is None
    assert feature.rsi_14 is None
    assert feature.volatility_20 is None


def test_anomaly_detector_detects_large_return():
    processor = StreamProcessor()
    detector = AnomalyDetector(return_threshold=0.03)

    processor.process(make_tick("AAPL", 100))
    feature = processor.process(make_tick("AAPL", 110))

    result = detector.detect(feature)

    assert result.is_anomaly is True
    assert result.severity == "high"


def test_correlation_tracker():
    tracker = CorrelationTracker(window_size=5)

    a_returns = [0.01, 0.02, 0.03, 0.04, 0.05]
    b_returns = [0.02, 0.04, 0.06, 0.08, 0.10]

    for a, b in zip(a_returns, b_returns):
        tracker.update("AAPL", a)
        tracker.update("MSFT", b)

    corr = tracker.correlation("AAPL", "MSFT")

    assert corr is not None
    assert corr > 0.9


def test_event_joiner():
    processor = StreamProcessor()
    joiner = EventJoiner()

    feature = processor.process(make_tick("AAPL", 100))

    joined = joiner.join(
        features=feature,
        news={"headline": "Apple stock rises"},
        macro={"event": "Fed decision"},
        alt_data={"score": 70},
    )

    assert joined.symbol == "AAPL"
    assert joined.features.symbol == "AAPL"
    assert joined.news is not None