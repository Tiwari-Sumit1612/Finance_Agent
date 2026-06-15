import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests


BASE_URL = "https://api.binance.com/api/v3/klines"


def to_milliseconds(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def download_klines(
    symbol: str = "BTCUSDT",
    interval: str = "5m",
    start_date: str = "2024-01-01",
    end_date: str = "2024-06-01",
    output_path: str = "ml/artifacts/btcusdt_5m.csv",
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    start_time = to_milliseconds(start_date)
    end_time = to_milliseconds(end_date)

    all_rows = []

    while start_time < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1000,
        }

        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        if not data:
            break

        all_rows.extend(data)

        last_open_time = data[-1][0]
        start_time = last_open_time + 1

        print(f"Downloaded rows: {len(all_rows)}")

        time.sleep(0.2)

    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]

    df = pd.DataFrame(all_rows, columns=columns)

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = df[col].astype(float)

    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

    df.to_csv(output_path, index=False)

    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    download_klines()