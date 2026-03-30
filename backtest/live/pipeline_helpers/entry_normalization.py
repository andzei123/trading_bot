from typing import Any

import numpy as np
import pandas as pd

from backtest.live.pipeline_helpers.normalization import _safe_to_datetime_utc
from backtest.live.pipeline_helpers.schema import LIVE_ENTRIES_COLUMNS, _empty_entries_df


def _entries_to_df(entries: list[Any], *, symbol: str) -> pd.DataFrame:
    """Normalize entry objects/dicts into a dataframe."""

    if not entries:
        df = _empty_entries_df()
        df["symbol"] = df["symbol"].astype("object")
        return df

    rows = []
    for e in entries:
        if isinstance(e, dict):
            get = e.get
        else:
            get = lambda k, default=None: getattr(e, k, default)
        ts = get("timestamp", None) or get("ts", None) or get("time", None)
        rows.append(
            {
                "timestamp": ts,
                "signal_ts": get("signal_ts", None),
                "model": get("model", ""),
                "side": get("side", ""),
                "entry": get("entry", None),
                "sl": get("sl", None),
                "tp": get("tp", None),
                "rr": get("rr", None),
                "ctx_sub_label": get("ctx_sub_label", get("sub_label", None)),
                "regime": get("regime", None),
                "trend_dir": get("trend_dir", None),
                "trend_strength": get("trend_strength", None),
                "atr_pct": get("atr_pct", None),
                "phase": get("phase", None),
                "symbol": symbol,
                "liq_bias": get("liq_bias", None),
                "liq_risk_multiplier": get("liq_risk_multiplier", None),
                "risk_multiplier": get("risk_multiplier", None),
                "block_reason": get("block_reason", None),
                "context_allow": get("context_allow", None),
                "macro_allow": get("macro_allow", None),
                "macro_reason": get("macro_reason", None),
                "macro_bias": get("macro_bias", None),
                "macro_bias_mismatch": get("macro_bias_mismatch", None),
                "news_allow": get("news_allow", None),
                "news_reason": get("news_reason", None),
                "liq_allow": get("liq_allow", None),
                "liq_reason": get("liq_reason", None),
                "freeze_new_signals": get("freeze_new_signals", None),
                "setup_age_hours": get("setup_age_hours", None),
                "setup_age_candles": get("setup_age_candles", None),
            }
        )

    df = pd.DataFrame(rows)
    # Guarantee columns
    for c in LIVE_ENTRIES_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[LIVE_ENTRIES_COLUMNS]
    # timestamps
    df["timestamp"] = _safe_to_datetime_utc(df["timestamp"])
    if "signal_ts" in df.columns:
        df["signal_ts"] = _safe_to_datetime_utc(df["signal_ts"])
    return df