from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pandas as pd


def emit_signals(
    *,
    df_e: pd.DataFrame,
    out_csv: Path,
    bybit_symbol: str,
    live_entries_columns: list[str],
    live_entries_dtypes: dict[str, str],
    ensure_live_entries_csv: Callable[[Path], None],
    append_csv: Callable[[Path, pd.DataFrame], None],
    diag_log: Callable[..., None],
    diag_payload_from_row: Callable[..., dict[str, Any]],
    now_utc_str: Callable[[], str],
) -> None:
    """Write already-finalized live entries exactly as prepared by the runner."""
    try:
        ensure_live_entries_csv(out_csv)
        df_out = df_e.copy()

        # Ensure stable schema (columns + dtypes)
        for _c, _dtype in live_entries_dtypes.items():
            if _c not in df_out.columns:
                df_out[_c] = pd.Series(dtype=_dtype)

        df_out = df_out.reindex(columns=live_entries_columns)

        try:
            for _, _row in df_out.iterrows():
                diag_log(
                    "SETUP_EMITTED",
                    **diag_payload_from_row(_row, symbol=str(bybit_symbol)),
                )
        except Exception:
            pass

        append_csv(out_csv, df_out)
    except Exception as _e:
        print(f"[{now_utc_str()}] [LIVE_ENTRIES][WARN] write failed: {repr(_e)}")
