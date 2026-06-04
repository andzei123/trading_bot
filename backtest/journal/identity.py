from __future__ import annotations

from pathlib import Path
from typing import Optional, Set

import pandas as pd

LIFECYCLE_WAIT_REJECTED = "WAIT_REJECTED"
LIFECYCLE_STALE = "STALE"
LIFECYCLE_FIRED = "FIRED"
LIFECYCLE_OPENED = "OPENED"
LIFECYCLE_CLOSED = "CLOSED"

TERMINAL_LIFECYCLE_STATES = {
    LIFECYCLE_STALE,
    LIFECYCLE_FIRED,
    LIFECYCLE_OPENED,
    LIFECYCLE_CLOSED,
}

TERMINAL_REGISTRY_COLUMNS = [
    "canonical_setup_key",
    "setup_id",
    "symbol",
    "model",
    "side",
    "setup_created_ts",
    "terminal_stage",
    "lifecycle_state",
    "terminal_ts",
    "reason",
]


def _build_canonical_setup_key(symbol: object, model: object, side: object, setup_created_ts: object) -> str:
    ts = pd.to_datetime(setup_created_ts, utc=True, errors="coerce")
    if pd.isna(ts):
        return ""
    return f"{str(symbol).upper()}|{str(model).upper()}|{str(side).upper()}|{pd.Timestamp(ts)}"


def _get_setup_created_ts(row: object) -> pd.Timestamp:
    def _row_get(key: str, default=None):
        if isinstance(row, dict):
            return row.get(key, default)
        try:
            return row.get(key, default)
        except Exception:
            return getattr(row, key, default)

    for col in ("setup_created_ts", "structural_ts", "origin_ts", "candidate_ts", "timestamp"):
        ts = pd.to_datetime(_row_get(col), utc=True, errors="coerce")
        if pd.notna(ts):
            return ts
    return pd.NaT


def _ensure_canonical_setup_key(obj, symbol: Optional[str] = None):
    if obj is None:
        return obj

    if isinstance(obj, pd.DataFrame):
        out = obj.copy()
        if "setup_created_ts" not in out.columns:
            out["setup_created_ts"] = pd.to_datetime(out.get("timestamp"), utc=True, errors="coerce")
        else:
            out["setup_created_ts"] = pd.to_datetime(out["setup_created_ts"], utc=True, errors="coerce")

        if "canonical_setup_key" not in out.columns:
            out["canonical_setup_key"] = ""

        if out.empty:
            return out

        sym_series = out["symbol"] if "symbol" in out.columns else pd.Series(symbol or "", index=out.index)
        model_series = out["model"] if "model" in out.columns else pd.Series("", index=out.index)
        side_series = out["side"] if "side" in out.columns else pd.Series("", index=out.index)

        for idx in out.index:
            key = str(out.at[idx, "canonical_setup_key"] or "")
            if key:
                continue
            out.at[idx, "canonical_setup_key"] = _build_canonical_setup_key(
                sym_series.loc[idx],
                model_series.loc[idx],
                side_series.loc[idx],
                out.at[idx, "setup_created_ts"],
            )
        return out

    if isinstance(obj, dict):
        out = dict(obj)
        setup_created_ts = _get_setup_created_ts(out)
        out["setup_created_ts"] = setup_created_ts
        if not out.get("canonical_setup_key"):
            out["canonical_setup_key"] = _build_canonical_setup_key(
                out.get("symbol", symbol or ""),
                out.get("model", ""),
                out.get("side", ""),
                setup_created_ts,
            )
        return out

    return obj


def _load_canonical_keys_from_csv(path: Path) -> Set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    try:
        df = pd.read_csv(path)
    except Exception:
        return set()
    if df.empty:
        return set()
    if "canonical_setup_key" in df.columns:
        keys = {str(x) for x in df["canonical_setup_key"].dropna().astype(str) if str(x)}
        if keys:
            return keys
    if {"symbol", "model", "side", "setup_created_ts"}.issubset(df.columns):
        keys = set()
        for _, row in df.iterrows():
            key = _build_canonical_setup_key(row.get("symbol"), row.get("model"), row.get("side"), row.get("setup_created_ts"))
            if key:
                keys.add(key)
        if keys:
            return keys
    if "setup_id" in df.columns:
        print(f"[IDENTITY_WARN] {path} missing canonical_setup_key/setup_created_ts; falling back to legacy setup_id")
        return {str(x) for x in df["setup_id"].dropna().astype(str) if str(x)}
    return set()


def _load_terminal_canonical_keys(path: Path) -> Set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    try:
        df = pd.read_csv(path)
    except Exception:
        return set()
    if df.empty:
        return set()
    if "lifecycle_state" in df.columns:
        df = df[df["lifecycle_state"].astype(str).str.upper().isin(TERMINAL_LIFECYCLE_STATES)]
    elif "terminal_stage" in df.columns:
        df = df[df["terminal_stage"].astype(str).str.upper().isin(TERMINAL_LIFECYCLE_STATES)]
    return _load_canonical_keys_from_frame(df)


def _load_canonical_keys_from_frame(df: pd.DataFrame) -> Set[str]:
    if df is None or df.empty:
        return set()
    if "canonical_setup_key" in df.columns:
        keys = {str(x) for x in df["canonical_setup_key"].dropna().astype(str) if str(x)}
        if keys:
            return keys
    if {"symbol", "model", "side", "setup_created_ts"}.issubset(df.columns):
        keys = set()
        for _, row in df.iterrows():
            key = _build_canonical_setup_key(row.get("symbol"), row.get("model"), row.get("side"), row.get("setup_created_ts"))
            if key:
                keys.add(key)
        return keys
    if "setup_id" in df.columns:
        return {str(x) for x in df["setup_id"].dropna().astype(str) if str(x)}
    return set()


def _append_terminal_lifecycle_row(
    path: Path,
    *,
    row: object,
    terminal_stage: str,
    terminal_ts: object,
    reason: str = "",
) -> None:
    lifecycle_state = str(terminal_stage).upper()
    if lifecycle_state not in TERMINAL_LIFECYCLE_STATES:
        print(f"[TERMINAL][SKIP_NON_TERMINAL] stage={terminal_stage} reason=not_terminal")
        return

    if isinstance(row, pd.Series):
        source = row.to_dict()
    elif isinstance(row, dict):
        source = dict(row)
    else:
        source = {}

    source = _ensure_canonical_setup_key(source)
    canonical = str(source.get("canonical_setup_key", "") or "")
    if not canonical:
        print(f"[IDENTITY_WARN] terminal row missing canonical key; fallback setup_id={source.get('setup_id', '')}")
        canonical = str(source.get("setup_id", "") or "")

    out = {
        "canonical_setup_key": canonical,
        "setup_id": source.get("setup_id", ""),
        "symbol": source.get("symbol", ""),
        "model": source.get("model", ""),
        "side": str(source.get("side", "") or "").upper(),
        "setup_created_ts": pd.to_datetime(source.get("setup_created_ts"), utc=True, errors="coerce"),
        "terminal_stage": lifecycle_state,
        "lifecycle_state": lifecycle_state,
        "terminal_ts": pd.to_datetime(terminal_ts, utc=True, errors="coerce"),
        "reason": reason,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([out], columns=TERMINAL_REGISTRY_COLUMNS)
    if not path.exists() or path.stat().st_size == 0:
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode="a", header=False, index=False)
    print(f"[TERMINAL] canonical={canonical} state={lifecycle_state}")
