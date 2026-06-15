import random
from dataclasses import dataclass

import pandas as pd

@dataclass
class DatasetConfig:
    rows: int = 5000
    output_path: str = "ml/artifacts/training_data.csv"


class SyntheticDatasetBuilder:
    """
    Creates fake financial feature data for first ML prototype.

    Later we will replace this with real Binance/Polygon historical data.
    """

    def __init__(self, config: DatasetConfig = DatasetConfig()):
        self.config = config

    def build(self) -> pd.DataFrame:
        data = []

        for _ in range(self.config.rows):
            return_1 = random.uniform(-0.03, 0.03)
            return_5 = random.uniform(-0.08, 0.08)
            momentum_5 = random.uniform(-5, 5)
            volatility_20 = random.uniform(0.001, 0.08)
            rsi_14 = random.uniform(10, 90)
            volume_spike = random.uniform(0.5, 4.0)

            score = (
                return_5 * 2.5
                + momentum_5 * 0.01
                - volatility_20 * 1.5
                + (50 - abs(rsi_14 - 50)) * 0.001
            )

            target = 1 if score > 0 else 0

            data.append(
                {
                    "return_1": return_1,
                    "return_5": return_5,
                    "momentum_5": momentum_5,
                    "volatility_20": volatility_20,
                    "rsi_14": rsi_14,
                    "volume_spike": volume_spike,
                    "target": target,
                }
            )

        return pd.DataFrame(data)

    def save(self) -> str:
        df = self.build()
        df.to_csv(self.config.output_path, index=False)
        return self.config.output_path


if __name__ == "__main__":
    path = SyntheticDatasetBuilder().save()
    print(f"Saved dataset to {path}")