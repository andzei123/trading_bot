import pandas as pd

def load_candles(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")
    df = df.set_index("timestamp")
    return df[["open", "high", "low", "close", "volume"]].astype(float)
