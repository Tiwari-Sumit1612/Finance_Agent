import os

import pandas as pd


def calculate_rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def build_features(
    input_path: str = "ml/artifacts/btcusdt_5m.csv",
    output_path: str = "ml/artifacts/btcusdt_features.csv",
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df = pd.read_csv(input_path)

    df["return_1"] = df["close"].pct_change(1)
    df["return_5"] = df["close"].pct_change(5)
    df["momentum_5"] = df["close"] - df["close"].shift(5)
    df["volatility_20"] = df["return_1"].rolling(20).std()
    df["rsi_14"] = calculate_rsi(df["close"], 14)

    df["vwap_20"] = (
        (df["close"] * df["volume"]).rolling(20).sum()
        / df["volume"].rolling(20).sum()
    )

    df["volume_avg_20"] = df["volume"].rolling(20).mean()
    df["volume_spike"] = df["volume"] / df["volume_avg_20"]
    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema_diff"] = (df["ema_9"] - df["ema_21"]) / df["close"]

    df["high_low_range"] = (df["high"] - df["low"]) / df["close"]
    df["close_open_return"] = (df["close"] - df["open"]) / df["open"]

    df["volume_change"] = df["volume"].pct_change(1)

    df["rolling_high_20"] = df["high"].rolling(20).max()
    df["rolling_low_20"] = df["low"].rolling(20).min()

    df["price_position_20"] = (
       (df["close"] - df["rolling_low_20"])
       / (df["rolling_high_20"] - df["rolling_low_20"])
    )

    prediction_horizon = 5

    df["future_close"] = df["close"].shift(-prediction_horizon)
    df["future_return"] = (df["future_close"] - df["close"]) / df["close"]

    df["target"] = (df["future_return"] > 0.002).astype(int)

    feature_cols = [
    "return_1",
    "return_5",
    "momentum_5",
    "volatility_20",
    "rsi_14",
    "volume_spike",
    "ema_diff",
    "high_low_range",
    "close_open_return",
    "volume_change",
    "price_position_20",
    "target",
    ]

    final_df = df[feature_cols].dropna()

    final_df.to_csv(output_path, index=False)

    print(f"Saved {len(final_df)} feature rows to {output_path}")


if __name__ == "__main__":
    build_features()