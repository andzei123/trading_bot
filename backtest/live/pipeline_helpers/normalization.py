import pandas as pd


def _safe_to_datetime_utc(s) -> pd.Series:
    return pd.to_datetime(s, utc=True, errors="coerce")


def _series_col_or_default(df: pd.DataFrame, col: str, default: float) -> pd.Series:
    """Return numeric Series for df[col] or a constant Series(default) aligned to df.index."""
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if col in df.columns:
        s = pd.to_numeric(df[col], errors="coerce")
        return s.fillna(float(default))
    return pd.Series([float(default)] * len(df), index=df.index, dtype=float)