from __future__ import annotations

from typing import Iterable
import pandas as pd


MODEL_MAX_AGE_CANDLES = {
    "RANGE_TOP_SHORT_V2": 3,
    "TDP_REENTRY": 8,
}

ASSUMED_CANDLE_INTERVAL = pd.Timedelta(minutes=15)


def filter_live_emit_candidates(
    df: pd.DataFrame,
    candles_df: pd.DataFrame,
    latest_ts: pd.Timestamp,
) -> pd.DataFrame:
    """Drop candidates whose TP/SL was already hit before live emit.

    This is an execution-safety filter for live mode only.
    It does not change pipeline logic. It prevents emitting stale setups that
    were valid historically but were already resolved before the current live
    observation time.
    """
    if df is None or df.empty:
        return df
    if candles_df is None or candles_df.empty:
        return df

    c = candles_df.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if c.empty:
        return df

    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if pd.isna(latest_ts):
        return df

    keep: list[bool] = []
    for _, row in df.iterrows():
        side = str(row.get("side", "")).upper().strip()

        setup_ts = pd.to_datetime(row.get("timestamp"), utc=True, errors="coerce")

        sl = pd.to_numeric(row.get("sl"), errors="coerce")
        tp = pd.to_numeric(row.get("tp"), errors="coerce")

        if pd.isna(setup_ts) or pd.isna(sl) or pd.isna(tp) or side not in ("LONG", "SHORT"):
            keep.append(True)
            continue

        # Check only candles after setup_ts and up to the current live candle.
        # If TP/SL has already been touched anywhere in that interval, the setup
        # is stale and must not be emitted live.
        # leidžiam tik entry window (tas pats kaip freshness)
        max_allowed_ts = setup_ts + pd.Timedelta(minutes=30)  # 2 barai (15m + 15m)

        post = c.loc[
            (c["timestamp"] > setup_ts) &
            (c["timestamp"] <= min(latest_ts, max_allowed_ts))
            ].copy()
        if post.empty:
            keep.append(True)
            continue

        if side == "LONG":
            stale = bool(((post["low"] <= float(sl)) | (post["high"] >= float(tp))).any())
        else:
            stale = bool(((post["high"] >= float(sl)) | (post["low"] <= float(tp))).any())

        keep.append(not stale)

    out_df = df.loc[keep].copy()
   # out_df = apply_model_age_filter(out_df, latest_ts)
    return out_df


def apply_model_age_filter(
    out_df: pd.DataFrame,
    latest_ts: pd.Timestamp,
) -> pd.DataFrame:
    """Drop candidates that are too old for their model.

    Age is measured in candles using an assumed 15-minute interval.
    Models not present in MODEL_MAX_AGE_CANDLES are not filtered by age.
    """
    if out_df is None or out_df.empty:
        return out_df

    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if pd.isna(latest_ts):
        return out_df

    keep: list[bool] = []
    for _, row in out_df.iterrows():
        model_name = str(row.get("model", "")).strip()
        max_age_candles = MODEL_MAX_AGE_CANDLES.get(model_name)

        if max_age_candles is None:
            keep.append(True)
            continue

        setup_ts = pd.to_datetime(row.get("timestamp"), utc=True, errors="coerce")
        if pd.isna(setup_ts):
            keep.append(True)
            continue

        age_delta = latest_ts - setup_ts
        if pd.isna(age_delta):
            keep.append(True)
            continue

        age_candles = age_delta / ASSUMED_CANDLE_INTERVAL
        keep.append(age_candles <= max_age_candles)

    return out_df.loc[keep].copy()


def select_newest_live_candidate(df: pd.DataFrame) -> pd.DataFrame:
    """Keep at most one candidate per symbol/cycle, preferring the newest setup."""
    if df is None or df.empty:
        return df
    return df.sort_values("timestamp").tail(1).copy()