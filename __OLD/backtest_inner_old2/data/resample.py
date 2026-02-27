import pandas as pd

def resample_candles(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Resample žvakes į aukštesnį TF (1H, 4H, D)
    """
    return df.resample(timeframe).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }).dropna()
