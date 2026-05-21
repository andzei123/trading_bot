from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from backtest.journal.identity import (
    LIFECYCLE_CLOSED,
    _append_terminal_lifecycle_row,
    _ensure_canonical_setup_key,
)


POSITION_STATE_COLUMNS = [
    "symbol",
    "canonical_setup_key",
    "setup_id",
    "setup_created_ts",
    "signal_ts",
    "opened_ts",
    "status",
    "closed_ts",
    "close_reason",
    "lifecycle_state",
]


def _load_position_state(position_state_csv: Path) -> pd.DataFrame:
    if not position_state_csv.exists() or position_state_csv.stat().st_size == 0:
        return pd.DataFrame(columns=POSITION_STATE_COLUMNS)
    try:
        df = pd.read_csv(position_state_csv)
    except Exception:
        return pd.DataFrame(columns=POSITION_STATE_COLUMNS)

    if df.empty:
        return pd.DataFrame(columns=POSITION_STATE_COLUMNS)

    for col in POSITION_STATE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[POSITION_STATE_COLUMNS].copy()


def _load_live_entries(out_csv: Path) -> pd.DataFrame:
    if not out_csv.exists() or out_csv.stat().st_size == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(out_csv)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    if "setup_id" not in df.columns and "canonical_setup_key" not in df.columns:
        return pd.DataFrame()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = _ensure_canonical_setup_key(df)
    return df


def _lookup_entry_row(out_csv: Path, setup_id: str, canonical_setup_key: str = "") -> Optional[pd.Series]:
    entries = _load_live_entries(out_csv)
    if entries.empty:
        return None
    if canonical_setup_key and "canonical_setup_key" in entries.columns:
        matches = entries.loc[entries["canonical_setup_key"].astype(str) == str(canonical_setup_key)].copy()
    else:
        matches = entries.loc[entries["setup_id"].astype(str) == str(setup_id)].copy()
    if matches.empty:
        return None
    if "timestamp" in matches.columns:
        matches = matches.sort_values("timestamp")
    return matches.iloc[-1]


def _detect_close(
    *,
    side: str,
    candles_df: pd.DataFrame,
    opened_ts: pd.Timestamp,
    sl: float,
    tp: float,
) -> tuple[Optional[pd.Timestamp], Optional[str]]:
    if candles_df is None or candles_df.empty:
        return None, None

    c = candles_df.copy()
    if "timestamp" not in c.columns or "high" not in c.columns or "low" not in c.columns:
        return None, None

    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if c.empty:
        return None, None

    opened_ts = pd.to_datetime(opened_ts, utc=True, errors="coerce")
    if pd.isna(opened_ts):
        return None, None

    post = c.loc[c["timestamp"] > opened_ts].copy()
    if post.empty:
        return None, None

    side_u = str(side or "").upper().strip()
    for _, r in post.iterrows():
        ts = pd.Timestamp(r["timestamp"])
        high = float(r["high"])
        low = float(r["low"])

        if side_u == "LONG":
            sl_hit = low <= float(sl)
            tp_hit = high >= float(tp)
            if sl_hit and tp_hit:
                return ts, "SL"
            if sl_hit:
                return ts, "SL"
            if tp_hit:
                return ts, "TP"
        else:
            sl_hit = high >= float(sl)
            tp_hit = low <= float(tp)
            if sl_hit and tp_hit:
                return ts, "SL"
            if sl_hit:
                return ts, "SL"
            if tp_hit:
                return ts, "TP"

    return None, None


def close_symbol_if_hit(
    *,
    symbol: str,
    candles_df: pd.DataFrame,
    position_state_csv: Path,
    out_csv: Path,
) -> bool:
    """Close OPEN position rows for one symbol if TP/SL has been hit.

    Returns True if at least one OPEN row for this symbol was moved to CLOSED.
    """
    state = _load_position_state(position_state_csv)
    if state.empty:
        return False

    sym = str(symbol or "").upper()
    mask = (
        state["symbol"].astype(str).str.upper() == sym
    ) & (
        state["status"].astype(str).str.upper() == "OPEN"
    )
    open_rows = state.loc[mask].copy()
    if open_rows.empty:
        return False

    changed = False
    for idx, row in open_rows.iterrows():
        setup_id = str(row.get("setup_id", "") or "")
        canonical_setup_key = str(row.get("canonical_setup_key", "") or "")
        opened_ts = pd.to_datetime(row.get("opened_ts"), utc=True, errors="coerce")
        if (not setup_id and not canonical_setup_key) or pd.isna(opened_ts):
            continue

        entry_row = _lookup_entry_row(out_csv, setup_id, canonical_setup_key)
        if entry_row is None:
            continue

        try:
            side = str(entry_row.get("side", "") or "").upper().strip()
            sl = float(entry_row.get("sl"))
            tp = float(entry_row.get("tp"))
        except Exception:
            continue

        close_ts, close_reason = _detect_close(
            side=side,
            candles_df=candles_df,
            opened_ts=opened_ts,
            sl=sl,
            tp=tp,
        )
        if close_ts is None or close_reason is None:
            continue

        if "closed_ts" in state.columns:
            state["closed_ts"] = state["closed_ts"].astype("object")
        if "close_reason" in state.columns:
            state["close_reason"] = state["close_reason"].astype("object")

        state.loc[idx, "status"] = "CLOSED"
        state.loc[idx, "closed_ts"] = str(pd.Timestamp(close_ts).tz_convert("UTC"))
        state.loc[idx, "close_reason"] = close_reason
        state.loc[idx, "lifecycle_state"] = LIFECYCLE_CLOSED
        changed = True
        terminal_row = row.to_dict()
        terminal_row["canonical_setup_key"] = canonical_setup_key
        terminal_row["setup_id"] = setup_id
        terminal_row["symbol"] = sym
        _append_terminal_lifecycle_row(
            position_state_csv.parent / "terminal_lifecycle_registry.csv",
            row=terminal_row,
            terminal_stage=LIFECYCLE_CLOSED,
            terminal_ts=close_ts,
            reason=str(close_reason),
        )
        print(f"[POSITION_CLOSER][{sym}] canonical={canonical_setup_key} setup_id={setup_id} closed={close_reason} close_ts={close_ts}")
    if changed:
        state.to_csv(position_state_csv, index=False)

    return changed
