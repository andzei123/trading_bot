from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

import pandas as pd


def enforce_idempotency(
    df_e: pd.DataFrame,
    *,
    once: bool,
    emit_last_candles: int,
    state_path: Path,
    read_state_fn: Callable[[Path], pd.Timestamp | None],
) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    """
    Preserve the current runner-owned last_seen duplicate-prevention boundary exactly.

    Behavior:
      - apply only in continuous live loop
      - skip in --once mode
      - skip in emit_last_candles/backfill mode
      - skip when BYPASS_LAST_SEEN=1
      - coerce signal_ts to UTC
      - keep only rows where signal_ts > last_ts
    """
    last_ts = None
    if (not once) and emit_last_candles == 0 and os.getenv("BYPASS_LAST_SEEN", "0") != "1":
        last_ts = read_state_fn(state_path)
        df_e = df_e.copy()
        df_e["signal_ts"] = pd.to_datetime(df_e["signal_ts"], utc=True, errors="coerce")
        df_e = df_e[df_e["signal_ts"] > last_ts].copy()
    return df_e, last_ts