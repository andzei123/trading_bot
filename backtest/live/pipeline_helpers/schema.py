import pandas as pd


LIVE_ENTRIES_COLUMNS = [
    "timestamp",
    "signal_ts",
    "model",
    "side",
    "entry",
    "sl",
    "tp",
    "rr",
    "ctx_sub_label",
    "regime",
    "trend_dir",
    "trend_strength",
    "atr_pct",
    "phase",
    "symbol",
    "liq_bias",
    "liq_risk_multiplier",
    "risk_multiplier",
    "block_reason",
    "context_allow",
    "macro_allow",
    "macro_reason",
    "macro_bias",
    "macro_bias_mismatch",
    "news_allow",
    "news_reason",
    "liq_allow",
    "liq_reason",
    "freeze_new_signals",
    "setup_age_hours",
    "setup_age_candles",
]


def _empty_entries_df() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in LIVE_ENTRIES_COLUMNS})