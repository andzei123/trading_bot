from pathlib import Path
import pandas as pd

METRICS_PATH = Path("exports_live/model_metrics.csv")


def append_model_metrics(df: pd.DataFrame):
    """
    Append model metrics rows:
    model, side, timestamp
    """
    if df is None or df.empty:
        return

    cols = ["model", "side", "timestamp"]
    if not set(cols).issubset(df.columns):
        return

    out = df[cols].copy()

    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)

    header = not METRICS_PATH.exists()
    out.to_csv(METRICS_PATH, mode="a", header=header, index=False)
