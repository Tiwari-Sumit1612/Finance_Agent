import os

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import TimeSeriesSplit


FEATURE_COLUMNS = [
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
]


def train_model(
    data_path: str = "ml/artifacts/btcusdt_features.csv",
    model_path: str = "ml/artifacts/signal_model.pkl",
):
    df = pd.read_csv(data_path)

    X = df[FEATURE_COLUMNS]
    y = df["target"]

    split_index = int(len(df) * 0.8)

    X_train = X.iloc[:split_index]
    X_test = X.iloc[split_index:]

    y_train = y.iloc[:split_index]
    y_test = y.iloc[split_index:]

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_leaf=20,
        random_state=42,
        class_weight="balanced",
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    print("Rows:", len(df))
    print("Train rows:", len(X_train))
    print("Test rows:", len(X_test))
    print("Accuracy:", accuracy_score(y_test, preds))
    print(classification_report(y_test, preds))

    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    joblib.dump(model, model_path)

    print(f"Saved model to {model_path}")


if __name__ == "__main__":
    train_model()