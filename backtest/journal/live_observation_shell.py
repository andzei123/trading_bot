from __future__ import annotations

"""
Minimal live observation shell with bounded observability logging.

STRICT bounded step:
    1) load live candles
    2) assemble upstream ctx
    3) call pipeline_core.run_pipeline_once(...)
    4) only then perform live-specific emission/logging

Authority:
    offline_live_runner_backtest.py -> pipeline_core.py

Non-goals:
    - no extra phase routing outside pipeline_core
    - no macro/news/liquidity integration
    - no second filter/risk/invalidation path
    - no strategy redesign

Observability additions in this version:
    - flow logging only
    - per-symbol/per-cycle counters for where setups die
    - no changes to pipeline, wait, stale, idempotency, or emit logic
"""

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import requests

from backtest.journal.position_closer import close_symbol_if_hit
from backtest.live_pipeline.pipeline_core import run_pipeline_once
from backtest.portfolio.portfolio_exposure import load_portfolio_exposure
from backtest.utils.wait_confirmation import apply_wait_confirmation
from backtest.journal.live_emit_guard import filter_live_emit_candidates, select_newest_live_candidate
from backtest.journal.identity import (
    LIFECYCLE_FIRED,
    LIFECYCLE_OPENED,
    LIFECYCLE_STALE,
    LIFECYCLE_WAIT_REJECTED,
    _append_terminal_lifecycle_row,
    _ensure_canonical_setup_key,
    _load_canonical_keys_from_csv,
    _load_terminal_canonical_keys,
)

BYBIT_REST = "https://api.bybit.com"

FLOW_LOG_COLUMNS = [
    "cycle_ts",
    "symbol",
    "latest_ts",
    "setup_id",
    "canonical_setup_key",
    "lifecycle_state",
    "model",
    "side",
    "setup_created_ts",
    "wait_confirm_ts",
    "wait_context_source",
    "visible_ts",
    "entry_anchor_ts",
    "first_seen_ts",
    "age_candles_at_first_seen",
    "intended_entry_ts",
    "entry_window_expires_ts",
    "entry_delay_minutes",
    "entry_timing_valid",
    "execution_ts_source",
    "trigger_refresh_candidate",
    "trigger_refresh_reason",
    "old_canonical_setup_key",
    "original_setup_created_ts",
    "candidate_latest_ts",
    "range_retest_score",
    "trigger_refresh_applied",
    "age_minutes_at_first_seen",
    "phase",
    "model_summary_raw",
    "model_summary_after_wait",
    "after_freshness_count",
    "model_summary_after_stale",
    "model_summary_after_idempotency",
    "model_summary_after_per_cycle_guard",
    "sub_label_summary_raw",
    "skipped_open_position",
    "pipeline_zero_rows",
    "raw_entries_count",
    "after_wait_count",
    "after_stale_count",
    "after_idempotency_count",
    "after_per_cycle_guard_count",
    "emitted_count",
    "notes",
    "death_stage",
    "death_reason",
]



VISIBLE_TS_PATH = Path("backtest/journal/visible_ts_cache.csv")


def _load_visible_ts_cache() -> Dict[str, pd.Timestamp]:
    if not VISIBLE_TS_PATH.exists():
        return {}

    try:
        df = pd.read_csv(VISIBLE_TS_PATH)
    except Exception:
        return {}

    if df.empty or "setup_id" not in df.columns or "visible_ts" not in df.columns:
        return {}

    out: Dict[str, pd.Timestamp] = {}
    for _, row in df.iterrows():
        sid = str(row.get("setup_id", "") or "")
        ts = pd.to_datetime(row.get("visible_ts"), utc=True, errors="coerce")
        if sid and pd.notna(ts):
            out[sid] = ts
    return out


def _save_visible_ts_cache(cache: Dict[str, pd.Timestamp]) -> None:
    global VISIBLE_TS_CACHE

    if len(VISIBLE_TS_CACHE) > MAX_CACHE_SIZE:
        VISIBLE_TS_CACHE = dict(
            sorted(
                VISIBLE_TS_CACHE.items(),
                key=lambda x: x[1],
                reverse=True
            )[:MAX_CACHE_SIZE]
        )

    VISIBLE_TS_PATH.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {"setup_id": k, "visible_ts": pd.to_datetime(v, utc=True, errors="coerce")}
            for k, v in VISIBLE_TS_CACHE.items()
        ]
    )

    df.to_csv(VISIBLE_TS_PATH, index=False)


VISIBLE_TS_CACHE = _load_visible_ts_cache()
MAX_CACHE_SIZE = 50000

FETCH_STATUS_OK = "ok"
FETCH_STATUS_EMPTY = "empty"
FETCH_STATUS_DNS_ERROR = "dns_error"
FETCH_STATUS_CONNECTION_ERROR = "connection_error"
FETCH_STATUS_TIMEOUT = "timeout"
FETCH_STATUS_HTTP_ERROR = "http_error"
FETCH_STATUS_API_ERROR = "api_error"

LAST_BYBIT_FETCH_STATUS: Dict[str, str] = {}




def _parse_symbols(s: str) -> List[str]:
    return [x.strip().upper() for x in str(s).split(",") if x.strip()]


def _read_state(state_path: Path) -> Optional[pd.Timestamp]:
    if not state_path.exists():
        return None
    try:
        raw = state_path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        return pd.to_datetime(raw, utc=True, errors="coerce")
    except Exception:
        return None
def _normalize_ts_ns(s):
    return pd.to_datetime(s, utc=True, errors="coerce").dt.as_unit("ns")

def _write_state(state_path: Path, ts: pd.Timestamp) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(str(pd.Timestamp(ts).tz_convert("UTC")), encoding="utf-8")


def _bybit_get_kline(
    category: str,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> tuple[pd.DataFrame, str]:
    url = f"{BYBIT_REST}/v5/market/kline"
    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "start": int(start_ms),
        "end": int(end_ms),
        "limit": int(limit),
    }

    last_error: Optional[str] = None
    last_status = FETCH_STATUS_EMPTY

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            j = r.json()
            if j.get("retCode") == 0:
                rows = []
                for it in (j.get("result", {}).get("list") or []):
                    rows.append(
                        {
                            "timestamp": pd.to_datetime(int(it[0]), unit="ms", utc=True),
                            "open": float(it[1]),
                            "high": float(it[2]),
                            "low": float(it[3]),
                            "close": float(it[4]),
                            "volume": float(it[5]),
                        }
                    )

                df = pd.DataFrame(rows)
                if df.empty:
                    LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = FETCH_STATUS_EMPTY
                    return df, FETCH_STATUS_EMPTY

                df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)

                try:
                    high_low = df["high"] - df["low"]
                    high_close = (df["high"] - df["close"].shift()).abs()
                    low_close = (df["low"] - df["close"].shift()).abs()
                    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                    atr = tr.rolling(14).mean()
                    df["atr"] = atr
                    df["atr_pct"] = atr / df["close"]
                except Exception:
                    df["atr"] = 0.0
                    df["atr_pct"] = 0.0

                LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = FETCH_STATUS_OK
                return df, FETCH_STATUS_OK

            last_error = f"Bybit error for {symbol}: {j}"
            ret_code = int(j.get("retCode", -1))
            last_status = FETCH_STATUS_API_ERROR

            if ret_code == 10006 and attempt < 2:
                print(f"[NETWORK_WARN] symbol={symbol} type=api_rate_limit attempt={attempt + 1}")
                time.sleep(1.0 + attempt)
                continue

            print(f"[NETWORK_WARN] symbol={symbol} type=api_error details={last_error}")
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

        except requests.exceptions.Timeout as e:
            last_error = f"Timeout error for {symbol}: {e}"
            last_status = FETCH_STATUS_TIMEOUT
            print(f"[NETWORK_WARN] symbol={symbol} type=timeout attempt={attempt + 1}")
            if attempt < 2:
                time.sleep(min(4.0, 1.0 * (2 ** attempt)))
                continue
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

        except requests.exceptions.ConnectionError as e:
            msg = str(e)
            if "NameResolutionError" in msg or "Failed to resolve" in msg or "Temporary failure in name resolution" in msg:
                last_status = FETCH_STATUS_DNS_ERROR
                print(f"[NETWORK_WARN] symbol={symbol} type=dns_error attempt={attempt + 1}")
            else:
                last_status = FETCH_STATUS_CONNECTION_ERROR
                print(f"[NETWORK_WARN] symbol={symbol} type=connection_error attempt={attempt + 1}")
            last_error = f"Connection error for {symbol}: {e}"
            if attempt < 2:
                time.sleep(min(4.0, 1.0 * (2 ** attempt)))
                continue
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

        except requests.exceptions.HTTPError as e:
            last_error = f"HTTP error for {symbol}: {e}"
            last_status = FETCH_STATUS_HTTP_ERROR
            print(f"[NETWORK_WARN] symbol={symbol} type=http_error attempt={attempt + 1}")
            if attempt < 2:
                time.sleep(min(4.0, 1.0 * (2 ** attempt)))
                continue
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

        except requests.RequestException as e:
            last_error = f"Request error for {symbol}: {e}"
            last_status = FETCH_STATUS_CONNECTION_ERROR
            print(f"[NETWORK_WARN] symbol={symbol} type=request_error attempt={attempt + 1}")
            if attempt < 2:
                time.sleep(min(4.0, 1.0 * (2 ** attempt)))
                continue
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

    print(f"[BYBIT][{symbol}] {last_error or 'unknown error'}")
    LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
    return pd.DataFrame(), last_status


def load_bybit_latest(
    category: str,
    symbol: str,
    interval: str,
    candles: int,
) -> pd.DataFrame:
    end = int(pd.Timestamp.now("UTC").timestamp() * 1000)
    ms_per_bar = {
        "1": 60_000,
        "3": 180_000,
        "5": 300_000,
        "15": 900_000,
        "30": 1_800_000,
        "60": 3_600_000,
        "120": 7_200_000,
        "240": 14_400_000,
        "D": 86_400_000,
    }.get(str(interval), 60_000)

    start = end - int(ms_per_bar * max(10, candles + 10))
    df, fetch_status = _bybit_get_kline(category, symbol, interval, start, end, limit=1000)
    LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = fetch_status
    if df.empty:
        return df

    if len(df) > candles:
        df = df.iloc[-candles:].reset_index(drop=True)
    return df


def _is_transport_fetch_status(status: str) -> bool:
    return status in {
        FETCH_STATUS_DNS_ERROR,
        FETCH_STATUS_CONNECTION_ERROR,
        FETCH_STATUS_TIMEOUT,
        FETCH_STATUS_HTTP_ERROR,
    }


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_output_csv(path: Path) -> None:
    _ensure_parent(path)


def _load_open_positions(position_state_csv: Path) -> Set[str]:
    if not position_state_csv.exists():
        return set()
    try:
        df = pd.read_csv(position_state_csv)
    except Exception:
        return set()
    if df.empty or "symbol" not in df.columns:
        return set()
    if "status" in df.columns:
        df = df[df["status"].astype(str).str.upper() == "OPEN"]
    return {str(x).upper() for x in df["symbol"].dropna().astype(str)}


def _position_is_open(symbol: str, position_state_csv: Path) -> bool:
    return str(symbol).upper() in _load_open_positions(position_state_csv)


def _position_overlaps_at_open(symbol: str, intended_open_ts, position_state_csv: Path) -> bool:
    """
    Return True if intended_open_ts falls inside any persisted same-symbol
    position window using half-open interval semantics:
        opened_ts <= intended_open_ts < closed_ts

    Null closed_ts is treated as open-ended.
    """
    if not position_state_csv.exists() or position_state_csv.stat().st_size == 0:
        return False

    intended_open_ts = pd.to_datetime(intended_open_ts, utc=True, errors="coerce")
    if pd.isna(intended_open_ts):
        return False

    try:
        df = pd.read_csv(position_state_csv)
    except Exception:
        return False

    if df.empty or "symbol" not in df.columns or "opened_ts" not in df.columns:
        return False

    work = df.copy()
    work["symbol"] = work["symbol"].astype(str).str.upper()
    work = work.loc[work["symbol"] == str(symbol).upper()].copy()
    if work.empty:
        return False

    work["opened_ts"] = pd.to_datetime(work["opened_ts"], utc=True, errors="coerce")
    if "closed_ts" in work.columns:
        work["closed_ts"] = pd.to_datetime(work["closed_ts"], utc=True, errors="coerce")
    else:
        work["closed_ts"] = pd.NaT

    overlap_mask = (
        work["opened_ts"].notna()
        & (work["opened_ts"] <= intended_open_ts)
        & (
            work["closed_ts"].isna()
            | (intended_open_ts < work["closed_ts"])
        )
    )
    return bool(overlap_mask.any())


def _pending_position_overlaps_at_open(
    symbol: str,
    intended_open_ts,
    pending_open_intervals: Dict[str, List[tuple]],
) -> bool:
    """Return True if candidate open interval overlaps accepted rows in this batch."""
    intended_open_ts = pd.to_datetime(intended_open_ts, utc=True, errors="coerce")
    if pd.isna(intended_open_ts):
        return False

    candidate_symbol = str(symbol).upper()
    candidate_close_ts = pd.NaT

    for opened_ts, closed_ts in pending_open_intervals.get(candidate_symbol, []):
        opened_ts = pd.to_datetime(opened_ts, utc=True, errors="coerce")
        closed_ts = pd.to_datetime(closed_ts, utc=True, errors="coerce")
        if pd.isna(opened_ts):
            continue

        pending_ends_after_candidate_opens = (
            pd.isna(closed_ts)
            or intended_open_ts < closed_ts
        )
        candidate_ends_after_pending_opens = (
            pd.isna(candidate_close_ts)
            or opened_ts < candidate_close_ts
        )

        if pending_ends_after_candidate_opens and candidate_ends_after_pending_opens:
            return True

    return False


def _candidate_intended_open_ts(row: pd.Series):
    for ts_col in ("trade_open_ts", "signal_ts", "opened_ts", "timestamp"):
        if ts_col in row.index:
            ts = pd.to_datetime(row.get(ts_col), utc=True, errors="coerce")
            if pd.notna(ts):
                return ts
    return pd.NaT


def _filter_position_overlap_candidates(
    *,
    out_df: pd.DataFrame,
    symbol: str,
    position_state_csv: Path,
    flow_log_csv: Path,
    flow_row: Dict[str, object],
) -> tuple[pd.DataFrame, int]:
    if out_df is None or out_df.empty:
        return out_df, 0

    accepted_indices = []
    skipped_count = 0
    pending_open_intervals: Dict[str, List[tuple]] = {}

    for row_idx, candidate_row in out_df.iterrows():
        candidate_symbol = str(candidate_row.get("symbol", symbol)).upper()
        intended_open_ts = _candidate_intended_open_ts(candidate_row)

        existing_overlap = _position_overlaps_at_open(
            candidate_symbol,
            intended_open_ts,
            position_state_csv,
        )
        pending_overlap = _pending_position_overlaps_at_open(
            candidate_symbol,
            intended_open_ts,
            pending_open_intervals,
        )

        if existing_overlap or pending_overlap:
            skipped_count += 1
            print(
                f"[POSITION_GATE] symbol={candidate_symbol} "
                f"canonical={candidate_row.get('canonical_setup_key', '')} "
                f"skipped_open_position=True "
                f"intended_open_ts={intended_open_ts} "
                f"reason=open_position_exists_at_intended_open_ts"
            )

            skipped_flow_row = dict(flow_row)
            skipped_flow_row["symbol"] = candidate_symbol
            skipped_flow_row["skipped_open_position"] = True
            skipped_flow_row["death_stage"] = "position_gate"
            skipped_flow_row["death_reason"] = "open_position_exists_at_intended_open_ts"
            skipped_flow_row["notes"] = "blocked_by_open_position_at_intended_open_ts"
            skipped_flow_row["emitted_count"] = 0

            for col in (
                "setup_id",
                "canonical_setup_key",
                "lifecycle_state",
                "model",
                "side",
                "setup_created_ts",
                "wait_confirm_ts",
                "wait_context_source",
                "visible_ts",
                "entry_anchor_ts",
                "first_seen_ts",
                "timestamp",
                "signal_ts",
                "trade_open_ts",
                "opened_ts",
            ):
                if col in out_df.columns:
                    skipped_flow_row[col] = candidate_row.get(col)

            _append_flow_row(flow_log_csv, skipped_flow_row)
            continue

        accepted_indices.append(row_idx)
        if pd.notna(intended_open_ts):
            pending_open_intervals.setdefault(candidate_symbol, []).append(
                (intended_open_ts, pd.NaT)
            )

    if not accepted_indices:
        return out_df.iloc[0:0].copy(), skipped_count

    return out_df.loc[accepted_indices].copy(), skipped_count

def _mark_position_open(
    symbol: str,
    setup_id: str,
    opened_ts: pd.Timestamp,
    position_state_csv: Path,
    canonical_setup_key: str = "",
    setup_created_ts=pd.NaT,
    signal_ts=pd.NaT,
    wait_confirm_ts=pd.NaT,
    wait_context_source: str = "",
) -> None:
    _ensure_parent(position_state_csv)

    row = pd.DataFrame([
        {
            "symbol": str(symbol).upper(),
            "canonical_setup_key": str(canonical_setup_key or ""),
            "setup_id": str(setup_id),
            "setup_created_ts": pd.to_datetime(setup_created_ts, utc=True, errors="coerce"),
            "wait_confirm_ts": pd.to_datetime(wait_confirm_ts, utc=True, errors="coerce"),
            "wait_context_source": str(wait_context_source or ""),
            "signal_ts": pd.to_datetime(signal_ts, utc=True, errors="coerce"),
            "opened_ts": pd.to_datetime(opened_ts, utc=True, errors="coerce"),
            "status": "OPEN",
            "closed_ts": pd.NA,
            "close_reason": pd.NA,
            "lifecycle_state": LIFECYCLE_OPENED,
        }
    ])

    cols = [
        "symbol",
        "canonical_setup_key",
        "setup_id",
        "setup_created_ts",
        "wait_confirm_ts",
        "wait_context_source",
        "signal_ts",
        "opened_ts",
        "status",
        "closed_ts",
        "close_reason",
        "lifecycle_state",
    ]

    if not position_state_csv.exists() or position_state_csv.stat().st_size == 0:
        row[cols].to_csv(position_state_csv, index=False)
        return

    try:
        existing = pd.read_csv(position_state_csv)
        if existing.empty:
            existing = pd.DataFrame(columns=cols)
        else:
            for c in cols:
                if c not in existing.columns:
                    existing[c] = pd.NA
            existing = existing[cols]
    except Exception:
        existing = pd.DataFrame(columns=cols)

    if not existing.empty and "symbol" in existing.columns and "status" in existing.columns:
        mask = (
            existing["symbol"].astype(str).str.upper() == str(symbol).upper()
        ) & (
            existing["status"].astype(str).str.upper() == "OPEN"
        )
        existing = existing.loc[~mask].copy()

    combined = pd.concat([existing, row[cols]], ignore_index=True)
    combined.to_csv(position_state_csv, index=False)

def _load_fired_setup_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path)
    except Exception:
        return set()
    if df.empty or "setup_id" not in df.columns:
        return set()
    return {str(x) for x in df["setup_id"].dropna().astype(str)}


def _append_fired_setup_ids(path: Path, rows: pd.DataFrame) -> None:
    if rows is None or rows.empty:
        return

    rows = _ensure_canonical_setup_key(rows)

    def _created_from_canonical(k):
        try:
            return str(k).split("|", 3)[3]
        except Exception:
            return pd.NaT

    canonical_created = rows["canonical_setup_key"].apply(_created_from_canonical)

    # Preserve ORIGINAL structural setup timestamp
    rows["setup_created_ts"] = canonical_created

    signal_fallback = rows.get("signal_ts", rows.get("timestamp"))

    if "trade_open_ts" not in rows.columns:
        rows["trade_open_ts"] = signal_fallback
    else:
        rows["trade_open_ts"] = rows["trade_open_ts"].fillna(signal_fallback)
        rows.loc[
            rows["trade_open_ts"].astype(str).str.strip().eq(""),
            "trade_open_ts"
        ] = signal_fallback

    if "opened_ts" not in rows.columns:
        rows["opened_ts"] = signal_fallback
    else:
        rows["opened_ts"] = rows["opened_ts"].fillna(signal_fallback)
        rows.loc[
            rows["opened_ts"].astype(str).str.strip().eq(""),
            "opened_ts"
        ] = signal_fallback

    if "execution_ts_source" not in rows.columns:
        rows["execution_ts_source"] = (
            "signal_ts"
            if "signal_ts" in rows.columns
            else "timestamp"
        )
    else:
        fallback_source = (
            "signal_ts"
            if "signal_ts" in rows.columns
            else "timestamp"
        )

        rows["execution_ts_source"] = (
            rows["execution_ts_source"].fillna(fallback_source)
        )

        rows.loc[
            rows["execution_ts_source"].astype(str).str.strip().eq(""),
            "execution_ts_source"
        ] = fallback_source

    # Wait confirmation metadata
    if "wait_confirm_ts" not in rows.columns:
        rows["wait_confirm_ts"] = (
            pd.to_datetime(
                rows["setup_created_ts"],
                utc=True,
                errors="coerce",
            ) + pd.Timedelta(minutes=15)
        )
    else:
        fallback_wait = (
            pd.to_datetime(
                rows["setup_created_ts"],
                utc=True,
                errors="coerce",
            ) + pd.Timedelta(minutes=15)
        )

        rows["wait_confirm_ts"] = (
            rows["wait_confirm_ts"].fillna(fallback_wait)
        )

        rows.loc[
            rows["wait_confirm_ts"].astype(str).str.strip().eq(""),
            "wait_confirm_ts"
        ] = fallback_wait

    if "wait_context_source" not in rows.columns:
        rows["wait_context_source"] = (
            "setup_created_ts_plus_1_candle"
        )
    else:
        rows["wait_context_source"] = (
            rows["wait_context_source"]
            .fillna("setup_created_ts_plus_1_candle")
        )

        rows.loc[
            rows["wait_context_source"]
            .astype(str)
            .str.strip()
            .eq(""),
            "wait_context_source"
        ] = "setup_created_ts_plus_1_candle"

    use_cols = [
        c for c in (
            "canonical_setup_key",
            "setup_id",
            "symbol",
            "timestamp",
            "setup_created_ts",
            "wait_confirm_ts",
            "wait_context_source",
            "model",
            "side",
            "signal_ts",
            "observed_ts",
            "trade_open_ts",
            "opened_ts",
            "execution_ts_source",
            "lifecycle_state",
        )
        if c in rows.columns
    ]

    if not use_cols:
        return

    out = rows[use_cols].copy()

    _ensure_parent(path)

    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


def _build_setup_id(symbol: str, timestamp, model: str, side: str) -> str:
    ts = pd.to_datetime(timestamp, utc=True, errors="coerce")
    return f"{str(symbol).upper()}|{ts}|{str(model)}|{str(side).upper()}"


def _build_runtime_setup_ids(df: pd.DataFrame, symbol: str) -> pd.Series:
    ts = pd.to_datetime(df.get("timestamp"), utc=True, errors="coerce").astype(str)
    model = df.get("model", pd.Series("", index=df.index)).astype(str)
    side = df.get("side", pd.Series("", index=df.index)).astype(str)
    sym = pd.Series(str(symbol).upper(), index=df.index)
    return sym + "|" + ts + "|" + model + "|" + side


def _append_df(path: Path, df: pd.DataFrame) -> None:
    _ensure_parent(path)
    if df is None:
        return
    if not path.exists() or path.stat().st_size == 0:
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode="a", header=False, index=False)


def _emit_observation_rows(
    *,
    out_csv: Path,
    symbol: str,
    latest_ts: pd.Timestamp,
    df_e: pd.DataFrame,
) -> int:
    if df_e is None or df_e.empty:
        return 0

    out = df_e.copy()
    out["observed_ts"] = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    out["symbol"] = symbol

    # Canonical execution/open clock for emitted live rows is the emitted
    # post-wait timestamp. Do not use structural setup_created_ts as open time.
    execution_ts = pd.to_datetime(out.get("timestamp"), utc=True, errors="coerce")
    if "signal_ts" not in out.columns or out["signal_ts"].isna().all():
        out["signal_ts"] = execution_ts
    out["trade_open_ts"] = execution_ts
    out["opened_ts"] = execution_ts
    out["execution_ts_source"] = "timestamp"
    out["position_gate_reason"] = ""
    out["skipped_open_position"] = False
    out["lifecycle_state"] = LIFECYCLE_OPENED

    _append_df(out_csv, out)
    return int(len(out))


def _series_summary(df: Optional[pd.DataFrame], col: str) -> str:
    if df is None or df.empty or col not in df.columns:
        return ""
    try:
        s = df[col].dropna().astype(str)
        if s.empty:
            return ""
        vc = s.value_counts()
        return ";".join([f"{idx}:{int(val)}" for idx, val in vc.items()])
    except Exception:
        return ""


def _derive_phase(df_e: Optional[pd.DataFrame], ctx: Dict[str, object]) -> str:
    if df_e is not None and not df_e.empty and "phase" in df_e.columns:
        try:
            s = df_e["phase"].dropna().astype(str)
            if not s.empty:
                return str(s.iloc[0])
        except Exception:
            pass
    try:
        return str(ctx.get("phase", "") or "")
    except Exception:
        return ""


def _append_flow_row(flow_log_csv: Path, row: Dict[str, object]) -> None:
    _ensure_parent(flow_log_csv)
    out = pd.DataFrame([{c: row.get(c, "") for c in FLOW_LOG_COLUMNS}])
    if not flow_log_csv.exists() or flow_log_csv.stat().st_size == 0:
        out.to_csv(flow_log_csv, index=False)
    else:
        out.to_csv(flow_log_csv, mode="a", header=False, index=False)


def _make_flow_row(*, cycle_ts: pd.Timestamp, symbol: str, latest_ts: Optional[pd.Timestamp]) -> Dict[str, object]:
    return {
        "cycle_ts": pd.to_datetime(cycle_ts, utc=True, errors="coerce"),
        "symbol": str(symbol).upper(),
        "latest_ts": pd.to_datetime(latest_ts, utc=True, errors="coerce") if latest_ts is not None else pd.NaT,

        # setup identity
        "setup_id": "",
        "canonical_setup_key": "",
        "lifecycle_state": "",
        "model": "",
        "side": "",
        "setup_created_ts": pd.NaT,
        "wait_confirm_ts": pd.NaT,
        "wait_context_source": "",

        # execution-visible semantics
        "visible_ts": pd.NaT,
        "entry_anchor_ts": pd.NaT,
        "first_seen_ts": pd.NaT,
        "age_candles_at_first_seen": 0,
        "intended_entry_ts": pd.NaT,
        "entry_window_expires_ts": pd.NaT,
        "entry_delay_minutes": 0.0,
        "entry_timing_valid": False,
        "execution_ts_source": "",
        "trigger_refresh_candidate": False,
        "trigger_refresh_reason": "",
        "old_canonical_setup_key": "",
        "original_setup_created_ts": "",
        "candidate_latest_ts": "",
        "range_retest_score": 0.0,
        "trigger_refresh_applied": False,
        "age_minutes_at_first_seen": 0.0,

        # summaries / diagnostics
        "phase": "",
        "model_summary_raw": "",
        "model_summary_after_wait": "",
        "model_summary_after_stale": "",
        "model_summary_after_idempotency": "",
        "model_summary_after_per_cycle_guard": "",
        "sub_label_summary_raw": "",

        # gates / counters
        "skipped_open_position": False,
        "pipeline_zero_rows": False,
        "raw_entries_count": 0,
        "after_wait_count": 0,
        "after_stale_count": 0,
        "after_idempotency_count": 0,
        "after_per_cycle_guard_count": 0,
        "emitted_count": 0,
        "after_freshness_count": 0,

        # outcome
        "notes": "",
        "death_stage": "no_raw",
        "death_reason": "",
    }

def _time_alignment_audit(
    *,
    symbol: str,
    candles_df: pd.DataFrame,
    latest_ts: pd.Timestamp,
    raw_df: Optional[pd.DataFrame] = None,
    visible_ts: Optional[pd.Timestamp] = None,
    pipeline_visible_ts: Optional[pd.Timestamp] = None,
) -> None:
    try:
        now_utc = pd.Timestamp.now(tz="UTC")

        c = candles_df.copy()
        c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
        c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

        latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")

        print(f"\n[TIME_AUDIT][{symbol}] =====================================")
        print(f"[TIME_AUDIT][{symbol}] now_utc={now_utc}")
        print(f"[TIME_AUDIT][{symbol}] latest_ts={latest_ts}")

        print(f"[TIME_AUDIT][{symbol}] last_5_candles:")
        print(c[["timestamp", "open", "high", "low", "close"]].tail(5).to_string(index=False))

        last_row_ts = c["timestamp"].iloc[-1] if len(c) else pd.NaT

        visible_ts = pd.to_datetime(visible_ts, utc=True, errors="coerce")
        pipeline_visible_ts = pd.to_datetime(pipeline_visible_ts, utc=True, errors="coerce")

        entry_anchor_ts = latest_ts
        if pd.notna(visible_ts):
            entry_anchor_ts = visible_ts
        elif pd.notna(pipeline_visible_ts):
            entry_anchor_ts = pipeline_visible_ts

        last_le_latest = c.loc[c["timestamp"] <= entry_anchor_ts, "timestamp"].max() if len(c) else pd.NaT

        print(f"[TIME_AUDIT][{symbol}] last_row_ts={last_row_ts}")
        print(f"[TIME_AUDIT][{symbol}] entry_anchor_ts={entry_anchor_ts}")
        print(f"[TIME_AUDIT][{symbol}] last_ts_le_anchor={last_le_latest}")
        print(f"[TIME_AUDIT][{symbol}] last_row_eq_last_le_anchor={bool(last_row_ts == last_le_latest)}")

        sliced = c.loc[c["timestamp"] <= entry_anchor_ts].copy()
        removed = int(len(c) - len(sliced))

        print(f"[TIME_AUDIT][{symbol}] rows_before_slice={len(c)}")
        print(f"[TIME_AUDIT][{symbol}] rows_removed_by_slice={removed}")
        print(f"[TIME_AUDIT][{symbol}] max_ts_after_slice={sliced['timestamp'].max() if len(sliced) else pd.NaT}")
        print(f"[TIME_AUDIT][{symbol}] last_3_after_slice:")
        if len(sliced):
            print(sliced[["timestamp", "open", "high", "low", "close"]].tail(3).to_string(index=False))
        else:
            print("(empty)")

        if raw_df is not None and not raw_df.empty:
            r = raw_df.copy()
            if "timestamp" in r.columns:
                r["timestamp"] = pd.to_datetime(r["timestamp"], utc=True, errors="coerce")
                first_setup_ts = pd.to_datetime(r["timestamp"].iloc[0], utc=True, errors="coerce")
                print(f"[TIME_AUDIT][{symbol}] first_setup_ts={first_setup_ts}")

                match_idx = sliced.index[sliced["timestamp"] == first_setup_ts].tolist()
                if match_idx:
                    idx = match_idx[0]
                    pos = sliced.index.get_loc(idx)
                    candles_after = len(sliced) - pos - 1
                    diff_min = (latest_ts - first_setup_ts).total_seconds() / 60.0 if pd.notna(first_setup_ts) else None
                    print(f"[TIME_AUDIT][{symbol}] setup_index_in_sliced={pos}")
                    print(f"[TIME_AUDIT][{symbol}] candles_after_setup_until_latest={candles_after}")
                    print(f"[TIME_AUDIT][{symbol}] setup_to_latest_minutes={diff_min}")
                else:
                    print(f"[TIME_AUDIT][{symbol}] first_setup_ts_not_found_in_sliced_window")

            if "ctx_sub_label" in r.columns:
                tdp_rows = r.loc[r["ctx_sub_label"].isin(["TDP_TOP", "TDP_BOT"])].copy()
            elif "sub_label" in r.columns:
                tdp_rows = r.loc[r["sub_label"].isin(["TDP_TOP", "TDP_BOT"])].copy()
            else:
                tdp_rows = pd.DataFrame()

            if not tdp_rows.empty:
                print(f"[TIME_AUDIT][{symbol}] last_6_for_tdp:")
                cols = [cname for cname in ["timestamp", "dev_count", "ctx_sub_label", "sub_label"] if cname in sliced.columns]
                if cols:
                    print(sliced[cols].tail(6).to_string(index=False))

                last_tdp_ts = pd.to_datetime(tdp_rows["timestamp"].iloc[-1], utc=True, errors="coerce")
                tdp_on_last = bool(len(sliced) and last_tdp_ts == sliced["timestamp"].iloc[-1])
                print(f"[TIME_AUDIT][{symbol}] last_tdp_label_ts={last_tdp_ts}")
                print(f"[TIME_AUDIT][{symbol}] tdp_label_on_last_row={tdp_on_last}")

        print(f"[TIME_AUDIT][{symbol}] =====================================\n")

    except Exception as e:
        print(f"[TIME_AUDIT_ERROR][{symbol}] {type(e).__name__}: {e}")

def _apply_execution_window_guard(
    *,
    wait_checked: pd.DataFrame,
    latest_ts: pd.Timestamp,
    symbol: str,
    flow_log_csv: Path,
    flow_row: Dict[str, object],
) -> pd.DataFrame:
    """
    Enforce entry timing semantics after wait confirmation accepts.

    setup_created_ts / wait_confirm_ts remain immutable identity/confirmation
    timestamps. Execution is only allowed at wait_confirm_ts within a small
    model-specific window. The live/latest cycle timestamp is observability
    only and must not become signal_ts/opened_ts for an old setup.
    """
    if wait_checked is None or wait_checked.empty:
        return wait_checked

    out = wait_checked.copy()

    out["setup_created_ts"] = pd.to_datetime(
        out.get("setup_created_ts", out.get("timestamp", pd.NaT)),
        utc=True,
        errors="coerce",
    )

    if "wait_confirm_ts" not in out.columns:
        out["wait_confirm_ts"] = pd.NaT

    out["wait_confirm_ts"] = pd.to_datetime(
        out["wait_confirm_ts"],
        utc=True,
        errors="coerce",
    )

    fallback_wait_confirm_ts = out["setup_created_ts"] + pd.Timedelta(minutes=15)
    out["wait_confirm_ts"] = out["wait_confirm_ts"].fillna(fallback_wait_confirm_ts)

    if "wait_context_source" not in out.columns:
        out["wait_context_source"] = "setup_created_ts_plus_1_candle"
    else:
        out["wait_context_source"] = (
            out["wait_context_source"]
            .fillna("setup_created_ts_plus_1_candle")
        )
        out.loc[
            out["wait_context_source"].astype(str).str.strip().eq(""),
            "wait_context_source",
        ] = "setup_created_ts_plus_1_candle"

    model_window_candles = (
        out.get("model", pd.Series("", index=out.index))
        .astype(str)
        .map({
            "RANGE_TOP_SHORT_V2": 4,
            "TDP_REENTRY": 16,
        })
        .fillna(1)
        .astype(int)
    )

    out["intended_entry_ts"] = out["wait_confirm_ts"]
    out["entry_window_expires_ts"] = (
        out["wait_confirm_ts"]
        + pd.to_timedelta(model_window_candles * 15, unit="m")
    )

    current_latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    out["entry_delay_minutes"] = (
        (current_latest_ts - out["wait_confirm_ts"]).dt.total_seconds() / 60.0
    )

    out["signal_ts"] = out["wait_confirm_ts"]
    out["timestamp"] = out["wait_confirm_ts"]
    out["trade_open_ts"] = out["wait_confirm_ts"]
    out["opened_ts"] = out["wait_confirm_ts"]
    out["execution_ts_source"] = "wait_confirm_ts"

    timing_valid_mask = current_latest_ts <= out["entry_window_expires_ts"]
    out["entry_timing_valid"] = timing_valid_mask

    stale_exec_df = out.loc[~timing_valid_mask].copy()
    if not stale_exec_df.empty:
        for _, stale_exec_row in stale_exec_df.iterrows():
            stale_flow_row = dict(flow_row)
            stale_flow_row["symbol"] = str(stale_exec_row.get("symbol", symbol)).upper()
            stale_flow_row["setup_id"] = stale_exec_row.get("setup_id", stale_flow_row.get("setup_id", ""))
            stale_flow_row["canonical_setup_key"] = stale_exec_row.get("canonical_setup_key", stale_flow_row.get("canonical_setup_key", ""))
            stale_flow_row["model"] = stale_exec_row.get("model", stale_flow_row.get("model", ""))
            stale_flow_row["side"] = str(stale_exec_row.get("side", stale_flow_row.get("side", ""))).upper()
            stale_flow_row["setup_created_ts"] = stale_exec_row.get("setup_created_ts", pd.NaT)
            stale_flow_row["wait_confirm_ts"] = stale_exec_row.get("wait_confirm_ts", pd.NaT)
            stale_flow_row["wait_context_source"] = stale_exec_row.get("wait_context_source", "setup_created_ts_plus_1_candle")
            stale_flow_row["visible_ts"] = stale_exec_row.get("visible_ts", pd.NaT)
            stale_flow_row["entry_anchor_ts"] = stale_exec_row.get("entry_anchor_ts", pd.NaT)
            stale_flow_row["first_seen_ts"] = stale_exec_row.get("visible_ts", pd.NaT)
            stale_flow_row["intended_entry_ts"] = stale_exec_row.get("intended_entry_ts", pd.NaT)
            stale_flow_row["entry_window_expires_ts"] = stale_exec_row.get("entry_window_expires_ts", pd.NaT)
            stale_flow_row["entry_delay_minutes"] = float(stale_exec_row.get("entry_delay_minutes", 0.0) or 0.0)
            stale_flow_row["entry_timing_valid"] = False
            stale_flow_row["execution_ts_source"] = "wait_confirm_ts"
            stale_flow_row["trigger_refresh_candidate"] = bool(stale_exec_row.get("trigger_refresh_candidate", False))
            stale_flow_row["trigger_refresh_reason"] = str(stale_exec_row.get("trigger_refresh_reason", ""))
            stale_flow_row["old_canonical_setup_key"] = str(stale_exec_row.get("old_canonical_setup_key", ""))
            stale_flow_row["original_setup_created_ts"] = stale_exec_row.get("original_setup_created_ts", pd.NaT)
            stale_flow_row["candidate_latest_ts"] = stale_exec_row.get("candidate_latest_ts", pd.NaT)
            stale_flow_row["range_retest_score"] = float(stale_exec_row.get("range_retest_score", 0.0) or 0.0)
            stale_flow_row["trigger_refresh_applied"] = bool(stale_exec_row.get("trigger_refresh_applied", False))
            stale_flow_row["after_wait_count"] = 0
            stale_flow_row["after_freshness_count"] = 0
            stale_flow_row["after_idempotency_count"] = 0
            stale_flow_row["after_stale_count"] = 0
            stale_flow_row["after_per_cycle_guard_count"] = 0
            stale_flow_row["emitted_count"] = 0
            stale_flow_row["notes"] = "entry_window_expired"
            stale_flow_row["death_stage"] = "stale"
            stale_flow_row["death_reason"] = "stale_execution_window"
            _append_flow_row(flow_log_csv, stale_flow_row)

    return out.loc[timing_valid_mask].copy()

def _canonical_created_ts_for_diagnostics(canonical_setup_key: object):
    try:
        return pd.to_datetime(
            str(canonical_setup_key).split("|", 3)[3],
            utc=True,
            errors="coerce",
        )
    except Exception:
        return pd.NaT



def _range_trigger_current_candle_has_fresh_risk_anchor(
    *,
    row: pd.Series,
    candles_df: pd.DataFrame,
    candidate_latest_ts: pd.Timestamp,
) -> bool:
    """
    Return True only when the existing entry/sl/tp on the candidate row
    are plausibly based on the fresh trigger candle context.

    This is intentionally conservative. A coincidental old entry inside the
    fresh candle is not enough. The entry must be inside the candidate candle,
    close enough to that candle close, directionally valid, and either the row
    timestamps are already near candidate_latest_ts or the diagnostics explicitly
    identified candidate_latest_ts as the fresh RANGE retest trigger.
    """
    if candles_df is None or candles_df.empty or "timestamp" not in candles_df.columns:
        return False

    candidate_latest_ts = pd.to_datetime(candidate_latest_ts, utc=True, errors="coerce")
    if pd.isna(candidate_latest_ts):
        return False

    c = candles_df.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp")
    hit = c.loc[c["timestamp"] == candidate_latest_ts]
    if hit.empty:
        return False

    candle = hit.iloc[-1]
    try:
        high = float(candle.get("high"))
        low = float(candle.get("low"))
        close = float(candle.get("close"))
        candle_range = high - low
        entry = float(pd.to_numeric(pd.Series([row.get("entry")]), errors="coerce").iloc[0])
        sl = float(pd.to_numeric(pd.Series([row.get("sl")]), errors="coerce").iloc[0])
        tp = float(pd.to_numeric(pd.Series([row.get("tp")]), errors="coerce").iloc[0])
    except Exception:
        return False

    if candle_range <= 0:
        return False

    if not (low <= entry <= high):
        return False

    entry_close_distance = abs(entry - close) / candle_range
    if entry_close_distance > 0.75:
        return False

    side = str(row.get("side", "")).upper()
    if side == "SHORT":
        direction_valid = bool(sl > entry and tp < entry)
    elif side == "LONG":
        direction_valid = bool(sl < entry and tp > entry)
    else:
        direction_valid = False

    if not direction_valid:
        return False

    near_candidate_ts = False
    for ts_col in ("setup_created_ts", "timestamp", "entry_anchor_ts", "visible_ts"):
        ts = pd.to_datetime(row.get(ts_col), utc=True, errors="coerce")
        if pd.notna(ts) and abs((ts - candidate_latest_ts).total_seconds()) <= 900:
            near_candidate_ts = True
            break

    try:
        score = float(row.get("range_retest_score", 0.0) or 0.0)
    except Exception:
        score = 0.0

    diagnostics_prove_fresh_trigger = (
        bool(row.get("trigger_refresh_candidate", False))
        and score >= 3.0
        and str(row.get("trigger_refresh_reason", ""))
        == "possible_fresh_range_retest_collapsed_to_old_canonical"
        and pd.notna(candidate_latest_ts)
    )

    return bool(near_candidate_ts or diagnostics_prove_fresh_trigger)


def _maybe_refresh_range_trigger_instance(
    *,
    wait_checked: pd.DataFrame,
    candles_df: pd.DataFrame,
    latest_ts: pd.Timestamp,
) -> pd.DataFrame:
    """
    RANGE_TOP_SHORT_V2 only.

    Convert a diagnostics-proven fresh range retest collision into a new
    setup instance before the execution-window guard runs. This does not
    touch TDP, lifecycle architecture, overlap gate, closer, or TP/SL logic.
    """
    if wait_checked is None or wait_checked.empty:
        return wait_checked

    out = wait_checked.copy()
    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if "trigger_refresh_applied" not in out.columns:
        out["trigger_refresh_applied"] = False

    for idx, row in out.iterrows():
        if str(row.get("model", "")) != "RANGE_TOP_SHORT_V2":
            continue
        if not bool(row.get("trigger_refresh_candidate", False)):
            continue

        try:
            score = float(row.get("range_retest_score", 0.0) or 0.0)
        except Exception:
            score = 0.0
        if score < 3.0:
            continue

        candidate_latest_ts = pd.to_datetime(
            row.get("candidate_latest_ts"),
            utc=True,
            errors="coerce",
        )
        if pd.isna(candidate_latest_ts):
            continue

        refreshed_wait_confirm_ts = candidate_latest_ts + pd.Timedelta(minutes=15)
        if pd.isna(latest_ts) or latest_ts < refreshed_wait_confirm_ts:
            out.at[idx, "trigger_refresh_reason"] = "refreshed_wait_not_confirmed_yet"
            out.at[idx, "trigger_refresh_applied"] = False
            continue

        old_wait_confirm_ts = pd.to_datetime(
            row.get("wait_confirm_ts"),
            utc=True,
            errors="coerce",
        )
        if pd.isna(old_wait_confirm_ts):
            old_wait_confirm_ts = (
                pd.to_datetime(row.get("setup_created_ts"), utc=True, errors="coerce")
                + pd.Timedelta(minutes=15)
            )
        if pd.isna(old_wait_confirm_ts):
            continue

        old_expiry_ts = old_wait_confirm_ts + pd.Timedelta(minutes=60)
        if candidate_latest_ts <= old_expiry_ts:
            continue

        if not _range_trigger_current_candle_has_fresh_risk_anchor(
            row=row,
            candles_df=candles_df,
            candidate_latest_ts=candidate_latest_ts,
        ):
            out.at[idx, "trigger_refresh_reason"] = "risk_anchor_not_fresh"
            out.at[idx, "trigger_refresh_applied"] = False
            continue

        side = str(row.get("side", "")).upper()
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            setup_id = str(row.get("setup_id", ""))
            symbol = setup_id.split("|", 1)[0].upper() if "|" in setup_id else ""
        if not symbol or not side:
            continue

        out.at[idx, "old_canonical_setup_key"] = str(row.get("canonical_setup_key", ""))
        out.at[idx, "setup_created_ts"] = candidate_latest_ts
        out.at[idx, "timestamp"] = candidate_latest_ts
        out.at[idx, "canonical_setup_key"] = f"{symbol}|RANGE_TOP_SHORT_V2|{side}|{candidate_latest_ts}"
        out.at[idx, "setup_id"] = f"{symbol}|{candidate_latest_ts}|RANGE_TOP_SHORT_V2|{side}"
        out.at[idx, "wait_confirm_ts"] = refreshed_wait_confirm_ts
        out.at[idx, "wait_context_source"] = "setup_created_ts_plus_1_candle"
        out.at[idx, "trigger_refresh_applied"] = True
        out.at[idx, "trigger_refresh_reason"] = "fresh_range_retest_new_instance"

    return out

def _add_range_trigger_refresh_diagnostics(
    *,
    df: pd.DataFrame,
    candles_df: pd.DataFrame,
    latest_ts: pd.Timestamp,
) -> pd.DataFrame:
    """
    Diagnostics only.

    This must not mutate canonical_setup_key, setup_id, setup_created_ts,
    timestamp, visible_ts, entry_anchor_ts, or execution timing. It only
    annotates RANGE_TOP_SHORT_V2 rows that look like possible fresh retests
    collapsed onto an older canonical identity.
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")

    defaults = {
        "trigger_refresh_candidate": False,
        "trigger_refresh_reason": "",
        "old_canonical_setup_key": "",
        "original_setup_created_ts": "",
        "candidate_latest_ts": "",
        "range_retest_score": 0.0,
    }
    for col, default in defaults.items():
        if col not in out.columns:
            out[col] = default

    if candles_df is None or candles_df.empty or pd.isna(latest_ts):
        return out
    if "timestamp" not in candles_df.columns:
        return out

    c = candles_df.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if c.empty:
        return out

    hit = c.loc[c["timestamp"] == latest_ts]
    if hit.empty:
        hit = c.loc[c["timestamp"] <= latest_ts].tail(1)
    if hit.empty:
        return out

    latest_idx = int(hit.index[-1])
    latest_candle = hit.iloc[-1]
    prior = c.iloc[max(0, latest_idx - 8):latest_idx]
    if prior.empty:
        return out

    try:
        high = float(latest_candle.get("high"))
        low = float(latest_candle.get("low"))
        open_ = float(latest_candle.get("open"))
        close = float(latest_candle.get("close"))
        candle_range = high - low
        local_top = bool(high >= float(prior["high"].max()) * 0.9995)
        upper_wick = high - max(open_, close)
        rejection = bool(candle_range > 0 and (upper_wick >= candle_range * 0.15 or close < open_))
    except Exception:
        return out

    for idx, row in out.iterrows():
        if str(row.get("model", "")) != "RANGE_TOP_SHORT_V2":
            continue
        if str(row.get("side", "")).upper() != "SHORT":
            continue

        canonical_key = str(row.get("canonical_setup_key", "") or "")
        original_setup_ts = _canonical_created_ts_for_diagnostics(canonical_key)
        if pd.isna(original_setup_ts):
            original_setup_ts = pd.to_datetime(
                row.get("setup_created_ts", row.get("timestamp", pd.NaT)),
                utc=True,
                errors="coerce",
            )

        old_identity = pd.notna(original_setup_ts) and latest_ts > (original_setup_ts + pd.Timedelta(minutes=30))
        entry = pd.to_numeric(pd.Series([row.get("entry")]), errors="coerce").iloc[0]
        entry_in_candle = bool(pd.notna(entry) and low <= float(entry) <= high)

        score = 0.0
        if old_identity:
            score += 1.0
        if local_top:
            score += 1.0
        if rejection:
            score += 1.0
        if entry_in_candle:
            score += 1.0

        candidate = bool(old_identity and local_top and rejection)
        reason = ""
        if candidate:
            reason = "possible_fresh_range_retest_collapsed_to_old_canonical"
        elif old_identity:
            reason = "old_range_identity_no_fresh_retest_proven"

        out.at[idx, "trigger_refresh_candidate"] = candidate
        out.at[idx, "trigger_refresh_reason"] = reason
        out.at[idx, "old_canonical_setup_key"] = canonical_key
        out.at[idx, "original_setup_created_ts"] = (
            "" if pd.isna(original_setup_ts) else str(original_setup_ts)
        )
        out.at[idx, "candidate_latest_ts"] = (
            "" if pd.isna(latest_ts) else str(latest_ts)
        )
        out.at[idx, "range_retest_score"] = float(score)

    return out


def model_freshness_filter(df: pd.DataFrame, latest_ts: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df

    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if pd.isna(latest_ts):
        return df

    work = df.copy()

    # normalize all possible time sources
    for col in ("timestamp", "visible_ts", "pipeline_visible_ts", "entry_anchor_ts"):
        if col in work.columns:
            work[col] = pd.to_datetime(work[col], utc=True, errors="coerce")
        else:
            work[col] = pd.NaT

    rows = []

    for _, row in work.iterrows():
        model = row["model"]

        # LIVE freshness authority:
        # entry_anchor_ts -> visible_ts -> pipeline_visible_ts -> timestamp
        setup_ts = row.get("entry_anchor_ts", pd.NaT)
        if pd.isna(setup_ts):
            setup_ts = row.get("visible_ts", pd.NaT)
        if pd.isna(setup_ts):
            setup_ts = row.get("pipeline_visible_ts", pd.NaT)
        if pd.isna(setup_ts):
            setup_ts = row.get("timestamp", pd.NaT)

        if pd.isna(setup_ts):
            print(f"[FRESHNESS] model={model} age=NA keep=False reason=missing_anchor_ts")
            continue

        age_candles = int((latest_ts - setup_ts).total_seconds() / 900)

        keep = False
        reason = ""

        ALLOWED_EXTRA_BARS = 0

        if model == "RANGE_TOP_SHORT_V2":
            keep = age_candles <= 4
            reason = "range_fresh" if keep else "range_too_old"

        elif model == "TDP_REENTRY":
            keep = age_candles <= 16
            reason = "tdp_fresh" if keep else "tdp_too_old"

        else:
            keep = True
            reason = "no_filter"

        print(
            f"[FRESHNESS] model={model} "
            f"anchor_ts={setup_ts} latest_ts={latest_ts} "
            f"age={age_candles} keep={keep} reason={reason}"
        )

        if keep:
            rows.append(row)

    return pd.DataFrame(rows)

def run_symbol_once(
    *,
    cycle_ts: pd.Timestamp,
    symbol: str,
    category: str,
    interval: str,
    candles_n: int,
    window_n: int,
    portfolio_state_path: Path,
    out_csv: Path,
    state_dir: Path,
    position_state_csv: Path,
    fired_setups_csv: Path,
    terminal_lifecycle_registry_csv: Path,
    flow_log_csv: Path,
    debug: bool,
    debug_force_entries: bool,
    use_wait_confirmation: bool,
    candidate_pressure_csv: str,
    cluster_score_mode: Optional[str],
    cluster_max_per_group: Optional[int],
    cluster_rank_signal_score: bool,
    rr: float,
    sl_atr_buffer: float,
    require_impulse_before_tdp: bool,
    impulse_lookback: int,
    impulse_size_atr: float,
    tdp_dev_lookback: int,
    tts_retest_lookback: int,
) -> int:
    candles_df = load_bybit_latest(category, symbol, interval, candles_n)
    fetch_status = LAST_BYBIT_FETCH_STATUS.get(str(symbol).upper(), FETCH_STATUS_EMPTY)
    if candles_df is None or candles_df.empty:
        print(f"[OBSERVE][{symbol}] no candles fetch_status={fetch_status}")
        flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
        flow_row["notes"] = f"no_candles:{fetch_status}"
        flow_row["pipeline_zero_rows"] = True
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    candles_df = candles_df.copy()
    candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"], utc=True, errors="coerce").dt.as_unit("us")
    candles_df = candles_df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    debug_candles_dir = Path("backtest/journal/exports_live/candles_debug")
    debug_candles_dir.mkdir(parents=True, exist_ok=True)

    candles_df.to_csv(
        debug_candles_dir / f"{symbol}_raw_live_candles.csv",
        index=False,
    )
    if candles_df.empty:
        print(f"[OBSERVE][{symbol}] candles empty after normalization fetch_status={fetch_status}")
        flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
        flow_row["notes"] = f"candles_empty_after_normalization:{fetch_status}"
        flow_row["pipeline_zero_rows"] = True
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    # paskutinė žvakė laikoma formuojama, todėl jos nenaudojam kaip latest closed bar
    if candles_df is None or len(candles_df) < 2:
        print(f"[OBSERVE][{symbol}] insufficient candles rows={0 if candles_df is None else len(candles_df)}")
        return 0

    latest_ts = pd.to_datetime(
        candles_df["timestamp"].iloc[-2],
        utc=True,
        errors="coerce",
    )
    candles_df = candles_df.iloc[:-1].copy().reset_index(drop=True)
    candles_df.to_csv(
        debug_candles_dir / f"{symbol}_closed_live_candles.csv",
        index=False,
    )
    flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=latest_ts)
    if debug:
        _time_alignment_audit(
            symbol=symbol,
            candles_df=candles_df,
            latest_ts=latest_ts,
            raw_df=None,
            visible_ts=None,
            pipeline_visible_ts=None,
        )
    state_path = state_dir / f"{symbol}_{interval}.txt"
    last_seen = _read_state(state_path)

    # Integration point: closer runs every cycle before position gate / signal generation.
    close_symbol_if_hit(
        symbol=symbol,
        candles_df=candles_df,
        position_state_csv=position_state_csv,
        out_csv=out_csv,
    )

    if _position_is_open(symbol, position_state_csv):
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] skipped open position latest_ts={latest_ts}")
        print(f"[POSITION_GATE] symbol={symbol} canonical= skipped_open_position=True reason=open_position_exists")
        flow_row["skipped_open_position"] = True
        flow_row["death_stage"] = "position_gate"
        flow_row["death_reason"] = "open_position_exists"
        flow_row["notes"] = "blocked_by_open_position"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    if last_seen is not None and latest_ts <= last_seen:
        print(f"[OBSERVE][{symbol}] no new candle latest_ts={latest_ts}")
        flow_row["notes"] = "no_new_candle"
        flow_row["pipeline_zero_rows"] = True
        flow_row["death_stage"] = "no_raw"
        flow_row["death_reason"] = "no_new_candle"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    window = candles_df.tail(int(window_n)).copy().reset_index(drop=True)
    window["timestamp"] = pd.to_datetime(window["timestamp"], utc=True, errors="coerce").dt.as_unit("us")
    portfolio_state = load_portfolio_exposure(portfolio_state_path)

    ctx: Dict[str, object] = {
        "latest_ts": latest_ts,
        "bybit_interval": int(interval) if str(interval).isdigit() else interval,
        "macro_bias": "NEUTRAL",
        "debug": bool(debug),
        "use_wait_confirmation": bool(use_wait_confirmation),
        "candidate_pressure_csv": candidate_pressure_csv,
        "rr": float(rr),
        "sl_atr_buffer": float(sl_atr_buffer),
        "require_impulse_before_tdp": bool(require_impulse_before_tdp),
        "impulse_lookback": int(impulse_lookback),
        "impulse_size_atr": float(impulse_size_atr),
        "tdp_dev_lookback": int(tdp_dev_lookback),
        "tts_retest_lookback": int(tts_retest_lookback),
        "disable_invalidation": True,
        "debug_force_entries": bool(debug_force_entries),
        "force_entries": bool(debug_force_entries),
        "debug_entry_force": bool(debug_force_entries),
        "DEBUG_FORCE_ENTRIES": bool(debug_force_entries),
    }

    if cluster_score_mode is not None:
        ctx["cluster_score_mode"] = cluster_score_mode
        if cluster_score_mode == "SIGNAL_SCORE":
            ctx["cluster_rank_signal_score"] = True

    if cluster_rank_signal_score:
        ctx["cluster_rank_signal_score"] = True

    if cluster_max_per_group is not None:
        ctx["cluster_max_per_group"] = int(cluster_max_per_group)

    df_e = run_pipeline_once(
        symbol=symbol,
        candles_df=window,
        ctx=ctx,
        portfolio_state=portfolio_state,
        debug=bool(debug),
    )

    if df_e is not None and not df_e.empty:
        df_e = _ensure_canonical_setup_key(df_e.copy(), symbol=symbol)

        df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")

        setup_ids = _build_runtime_setup_ids(df_e, symbol)
        df_e["setup_id"] = setup_ids
        for _, identity_row in df_e.iterrows():
            print(
                f"[IDENTITY] canonical={identity_row.get('canonical_setup_key', '')} "
                f"setup_id={identity_row.get('setup_id', '')}"
            )

        df_e = _add_range_trigger_refresh_diagnostics(
            df=df_e,
            candles_df=candles_df,
            latest_ts=latest_ts,
        )

        stable_visible = []

        cache_updated = False
        for sid in setup_ids:
            if sid not in VISIBLE_TS_CACHE:
                VISIBLE_TS_CACHE[sid] = latest_ts
                cache_updated = True
            stable_visible.append(VISIBLE_TS_CACHE[sid])

        df_e["visible_ts"] = stable_visible
        df_e["pipeline_visible_ts"] = stable_visible

        if cache_updated:
            _save_visible_ts_cache(VISIBLE_TS_CACHE)

    visible_ts = pd.NaT
    pipeline_visible_ts = pd.NaT
    entry_anchor_ts = pd.NaT
    # ==============================
    # DISCOVERY GATE (NEW SIGNALS ONLY)
    # ==============================
    if df_e is not None and not df_e.empty:
        df_e = df_e.copy()

        try:
            df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
        except Exception:
            pass

        # --- ENTRY ANCHOR NORMALIZATION (MOVE BEFORE GATE) ---
        if "visible_ts" in df_e.columns:
            df_e["visible_ts"] = pd.to_datetime(df_e["visible_ts"], utc=True, errors="coerce")
        else:
            df_e["visible_ts"] = pd.NaT

        if "pipeline_visible_ts" in df_e.columns:
            df_e["pipeline_visible_ts"] = pd.to_datetime(df_e["pipeline_visible_ts"], utc=True, errors="coerce")
        else:
            df_e["pipeline_visible_ts"] = pd.NaT

        # fallback chain:
        # visible_ts -> pipeline_visible_ts -> timestamp
        anchor = df_e["visible_ts"].copy()

        m = anchor.isna()
        anchor.loc[m] = df_e.loc[m, "pipeline_visible_ts"]

        m = anchor.isna()
        anchor.loc[m] = df_e.loc[m, "timestamp"]

        df_e["entry_anchor_ts"] = pd.to_datetime(anchor, utc=True, errors="coerce")

        current_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
        entry_window = pd.Timedelta(minutes=30)
        min_ts = current_ts - entry_window

        before = len(df_e)

        print(
            f"[DISCOVERY_GATE_DEBUG][{symbol}] "
            f"timestamp={df_e['timestamp'].astype(str).tolist()} "
            f"visible_ts={df_e['visible_ts'].astype(str).tolist()} "
            f"anchor={df_e['entry_anchor_ts'].astype(str).tolist()}"
        )

        df_e = df_e[
            (df_e["entry_anchor_ts"] > min_ts) &
            (df_e["entry_anchor_ts"] <= current_ts)
            ].copy()

        after = len(df_e)

        if after == 0:
            print(f"[POST_DROP][{symbol}] stage=DISCOVERY_GATE latest_ts={latest_ts}")

        if debug:
            print(
                f"[DISCOVERY_GATE][{symbol}] "
                f"window=({min_ts}, {current_ts}] before={before} after={after}"
            )

    if df_e is not None and not df_e.empty:
        df_e = df_e.copy()
        df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")

        if "visible_ts" not in df_e.columns:
            df_e["visible_ts"] = pd.NaT
        if "pipeline_visible_ts" not in df_e.columns:
            df_e["pipeline_visible_ts"] = pd.NaT

        df_e["visible_ts"] = pd.to_datetime(df_e["visible_ts"], utc=True, errors="coerce")
        df_e["pipeline_visible_ts"] = pd.to_datetime(df_e["pipeline_visible_ts"], utc=True, errors="coerce")

        first_row = df_e.iloc[0]
        setup_created_ts = pd.to_datetime(first_row.get("timestamp"), utc=True, errors="coerce")
        visible_ts = pd.to_datetime(first_row.get("visible_ts"), utc=True, errors="coerce")
        pipeline_visible_ts = pd.to_datetime(first_row.get("pipeline_visible_ts"), utc=True, errors="coerce")

        entry_anchor_ts = visible_ts
        if pd.isna(entry_anchor_ts):
            entry_anchor_ts = pipeline_visible_ts
        if pd.isna(entry_anchor_ts):
            entry_anchor_ts = setup_created_ts

        df_e = _ensure_canonical_setup_key(df_e, symbol=symbol)
        setup_ids = _build_runtime_setup_ids(df_e, symbol)
        df_e["setup_id"] = setup_ids
        flow_row["setup_id"] = str(setup_ids.iloc[0])
        flow_row["canonical_setup_key"] = str(first_row.get("canonical_setup_key", ""))

        print(
            f"[VISIBLE_DEBUG] "
            f"timestamp={first_row['timestamp']} "
            f"visible_ts={first_row['visible_ts']} "
            f"pipeline_visible_ts={first_row['pipeline_visible_ts']}"
        )

        flow_row["model"] = str(first_row.get("model", ""))
        flow_row["side"] = str(first_row.get("side", "")).upper()
        flow_row["setup_created_ts"] = setup_created_ts
        flow_row["visible_ts"] = visible_ts
        flow_row["entry_anchor_ts"] = entry_anchor_ts
        flow_row["first_seen_ts"] = visible_ts
        flow_row["age_candles_at_first_seen"] = 0
        flow_row["age_minutes_at_first_seen"] = 0.0
        flow_row["trigger_refresh_candidate"] = bool(first_row.get("trigger_refresh_candidate", False))
        flow_row["trigger_refresh_reason"] = str(first_row.get("trigger_refresh_reason", ""))
        flow_row["old_canonical_setup_key"] = str(first_row.get("old_canonical_setup_key", ""))
        flow_row["original_setup_created_ts"] = pd.to_datetime(first_row.get("original_setup_created_ts"), utc=True, errors="coerce")
        flow_row["candidate_latest_ts"] = pd.to_datetime(first_row.get("candidate_latest_ts"), utc=True, errors="coerce")
        flow_row["range_retest_score"] = float(first_row.get("range_retest_score", 0.0) or 0.0)
        flow_row["trigger_refresh_applied"] = bool(first_row.get("trigger_refresh_applied", False))

    flow_row["phase"] = _derive_phase(df_e, ctx)
    flow_row["raw_entries_count"] = int(0 if df_e is None else len(df_e))
    flow_row["model_summary_raw"] = _series_summary(df_e, "model")
    flow_row["sub_label_summary_raw"] = _series_summary(df_e, "ctx_sub_label")
    if debug:
        _time_alignment_audit(
            symbol=symbol,
            candles_df=window,
            latest_ts=latest_ts,
            raw_df=df_e,
            visible_ts=visible_ts,
            pipeline_visible_ts=pipeline_visible_ts,
        )
    if df_e is None or df_e.empty:
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] pipeline rows=0 latest_ts={latest_ts}")
        flow_row["pipeline_zero_rows"] = True
        flow_row["notes"] = "pipeline_rows_0"
        flow_row["death_stage"] = "no_raw"
        flow_row["death_reason"] = "pipeline_rows_0"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    df_wait = df_e.copy()

    # preserve original structure origin
    df_wait["setup_created_ts"] = pd.to_datetime(
        df_wait["timestamp"], utc=True, errors="coerce"
    )

    # Wait confirmation is an EARLY STRUCTURAL VALIDATION.
    # It must evaluate setup_created_ts + one candle, not the late live
    # visible/entry-anchor/cycle timestamp.
    df_wait["cycle_ts"] = pd.to_datetime(cycle_ts, utc=True, errors="coerce")
    df_wait["pre_wait_live_execution_ts"] = pd.to_datetime(
        df_wait["entry_anchor_ts"], utc=True, errors="coerce"
    )
    df_wait["timestamp"] = pd.to_datetime(
        df_wait["setup_created_ts"], utc=True, errors="coerce"
    )

    entries = df_wait.to_dict("records")

    if bool(use_wait_confirmation):
        before_wait = len(entries)
        entries_after_wait = apply_wait_confirmation(entries, window)
        after_wait = len(entries_after_wait)

        if entries_after_wait:
            wait_checked = pd.DataFrame(entries_after_wait)
            if "wait_confirm_ts" not in wait_checked.columns:
                wait_checked["wait_confirm_ts"] = wait_checked.get("timestamp", pd.NaT)
            if "wait_context_source" not in wait_checked.columns:
                wait_checked["wait_context_source"] = "setup_created_ts_plus_1_candle"

            wait_checked = _maybe_refresh_range_trigger_instance(
                wait_checked=wait_checked,
                candles_df=candles_df,
                latest_ts=latest_ts,
            )

            wait_checked = _apply_execution_window_guard(
                wait_checked=wait_checked,
                latest_ts=latest_ts,
                symbol=symbol,
                flow_log_csv=flow_log_csv,
                flow_row=flow_row,
            )

            if wait_checked.empty:
                entries_after_wait = []
                after_wait = 0
                _write_state(state_path, latest_ts)
                print(f"[POST_DROP][{symbol}] stage=STALE_EXECUTION_WINDOW latest_ts={latest_ts}")
                flow_row["after_wait_count"] = 0
                flow_row["after_freshness_count"] = 0
                flow_row["after_idempotency_count"] = 0
                flow_row["after_stale_count"] = 0
                flow_row["after_per_cycle_guard_count"] = 0
                flow_row["emitted_count"] = 0
                flow_row["notes"] = "entry_window_expired"
                flow_row["death_stage"] = "stale"
                flow_row["death_reason"] = "stale_execution_window"
                flow_row["execution_ts_source"] = "wait_confirm_ts"
                flow_row["entry_timing_valid"] = False
                _append_flow_row(flow_log_csv, flow_row)
                return 0

            after_wait = len(wait_checked)

            diag = wait_checked.iloc[0]
            flow_row["wait_confirm_ts"] = pd.to_datetime(diag.get("wait_confirm_ts"), utc=True, errors="coerce")
            flow_row["wait_context_source"] = str(diag.get("wait_context_source", "setup_created_ts_plus_1_candle"))
            flow_row["intended_entry_ts"] = pd.to_datetime(diag.get("intended_entry_ts"), utc=True, errors="coerce")
            flow_row["entry_window_expires_ts"] = pd.to_datetime(diag.get("entry_window_expires_ts"), utc=True, errors="coerce")
            flow_row["entry_delay_minutes"] = float(diag.get("entry_delay_minutes", 0.0) or 0.0)
            flow_row["entry_timing_valid"] = bool(diag.get("entry_timing_valid", False))
            flow_row["execution_ts_source"] = str(diag.get("execution_ts_source", "wait_confirm_ts"))
            print(
                f"[WAIT_CONTEXT_ACCEPTED][{symbol}] "
                f"canonical={diag.get('canonical_setup_key', '')} "
                f"setup_created_ts={diag.get('setup_created_ts', '')} "
                f"wait_confirm_ts={diag.get('wait_confirm_ts', '')} "
                f"visible_ts={diag.get('visible_ts', '')} "
                f"pipeline_visible_ts={diag.get('pipeline_visible_ts', '')} "
                f"signal_ts={diag.get('signal_ts', '')} "
                f"cycle_ts={cycle_ts} "
                f"wait_context_source={diag.get('wait_context_source', 'setup_created_ts_plus_1_candle')}"
            )
            entries_after_wait = wait_checked.to_dict("records")

        if after_wait < before_wait:
            print(f"[DROP][{symbol}] stage=WAIT_CONFIRM before={before_wait} after={after_wait}")

        # strict gate semantics: no fallback to pre-wait entries
        entries = entries_after_wait

        if after_wait == 0:
            _write_state(state_path, latest_ts)
            print(f"[POST_DROP][{symbol}] stage=WAIT_CONFIRMATION latest_ts={latest_ts}")
            print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after wait confirmation latest_ts={latest_ts}")
            flow_row["after_wait_count"] = 0
            flow_row["model_summary_after_wait"] = ""
            flow_row["after_freshness_count"] = 0
            flow_row["after_idempotency_count"] = 0
            flow_row["after_stale_count"] = 0
            flow_row["after_per_cycle_guard_count"] = 0
            flow_row["notes"] = "wait_confirmation_rejected_non_terminal"
            flow_row["wait_context_source"] = "setup_created_ts_plus_1_candle"
            flow_row["lifecycle_state"] = LIFECYCLE_WAIT_REJECTED
            flow_row["death_stage"] = "wait_confirmation"
            flow_row["death_reason"] = "apply_wait_confirmation"
            print(f"[TERMINAL][SKIP_NON_TERMINAL] stage=wait_confirmation reason=not_terminal")
            _append_flow_row(flow_log_csv, flow_row)
            return 0


    wait_df = pd.DataFrame(entries)
    flow_row["after_wait_count"] = int(len(wait_df))
    flow_row["model_summary_after_wait"] = _series_summary(wait_df, "model")

    out_df = pd.DataFrame(entries)
    out_df = model_freshness_filter(out_df, latest_ts)

    flow_row["after_freshness_count"] = int(len(out_df))

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=FRESHNESS latest_ts={latest_ts}")
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after freshness latest_ts={latest_ts}")
        flow_row["notes"] = "died_in_freshness"
        flow_row["death_stage"] = "freshness"
        flow_row["death_reason"] = "model_freshness_filter"
        _append_flow_row(flow_log_csv, flow_row)
        return 0


    out_df = _ensure_canonical_setup_key(out_df, symbol=symbol)
    out_df["setup_id"] = _build_runtime_setup_ids(out_df, symbol)

    fired_ids = _load_canonical_keys_from_csv(fired_setups_csv)
    terminal_ids = _load_terminal_canonical_keys(terminal_lifecycle_registry_csv)
    blocked_ids = fired_ids | terminal_ids
    blocked_mask = out_df["canonical_setup_key"].astype(str).isin(blocked_ids)
    for blocked_key in out_df.loc[blocked_mask, "canonical_setup_key"].astype(str).tolist():
        print(f"[DEDUPE] blocked canonical_key={blocked_key} reason=terminal_or_fired")
        print(f"[TERMINAL_BLOCK] canonical={blocked_key}")
    out_df = out_df.loc[~blocked_mask].copy()
    flow_row["after_idempotency_count"] = int(len(out_df))
    flow_row["model_summary_after_idempotency"] = _series_summary(out_df, "model")

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=IDEMPOTENCY latest_ts={latest_ts}")
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after idempotency latest_ts={latest_ts}")
        flow_row["after_stale_count"] = flow_row["after_wait_count"]
        flow_row["model_summary_after_stale"] = flow_row["model_summary_after_wait"]
        flow_row["notes"] = "died_in_idempotency"
        flow_row["death_stage"] = "idempotency"
        flow_row["death_reason"] = "already_fired"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    out_df["observed_ts"] = latest_ts

    before_stale = len(out_df)
    before_stale_df = out_df.copy()
    out_df = filter_live_emit_candidates(out_df, candles_df, latest_ts)
    after_stale = len(out_df)
    flow_row["after_stale_count"] = int(len(out_df))

    if after_stale < before_stale:
        print(f"[DROP][{symbol}] stage=STALE before={before_stale} after={after_stale}")
    flow_row["model_summary_after_stale"] = _series_summary(out_df, "model")

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=STALE latest_ts={latest_ts}")
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after stale-hit filter latest_ts={latest_ts}")
        for _, stale_row in before_stale_df.iterrows():
            _append_terminal_lifecycle_row(
                terminal_lifecycle_registry_csv,
                row=stale_row,
                terminal_stage=LIFECYCLE_STALE,
                terminal_ts=latest_ts,
                reason="filter_live_emit_candidates",
            )
        flow_row["notes"] = "died_in_stale"
        flow_row["lifecycle_state"] = LIFECYCLE_STALE
        flow_row["death_stage"] = "stale"
        flow_row["death_reason"] = "filter_live_emit_candidates"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    out_df = select_newest_live_candidate(out_df)
    flow_row["after_per_cycle_guard_count"] = int(len(out_df))
    flow_row["model_summary_after_per_cycle_guard"] = _series_summary(out_df, "model")
    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=FINAL_SELECTION latest_ts={latest_ts}")
        flow_row["notes"] = "died_in_final_selection"
        flow_row["death_stage"] = "final_selection"
        flow_row["death_reason"] = "select_newest_live_candidate"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    out_df, position_gate_skipped = _filter_position_overlap_candidates(
        out_df=out_df,
        symbol=symbol,
        position_state_csv=position_state_csv,
        flow_log_csv=flow_log_csv,
        flow_row=flow_row,
    )
    flow_row["after_per_cycle_guard_count"] = int(len(out_df))
    flow_row["model_summary_after_per_cycle_guard"] = _series_summary(out_df, "model")
    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=POSITION_GATE latest_ts={latest_ts}")
        flow_row["skipped_open_position"] = True
        flow_row["notes"] = "blocked_by_open_position_at_intended_open_ts"
        flow_row["death_stage"] = "position_gate"
        flow_row["death_reason"] = "open_position_exists_at_intended_open_ts"
        flow_row["emitted_count"] = 0
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    if position_gate_skipped > 0:
        flow_row["notes"] = f"position_gate_skipped={int(position_gate_skipped)}"

    # Keep flow_log aligned with the exact final emitted row.
    if out_df is not None and not out_df.empty:
        final_row = out_df.iloc[0]
        if "setup_id" in out_df.columns:
            flow_row["setup_id"] = str(final_row.get("setup_id", ""))
        flow_row["canonical_setup_key"] = str(final_row.get("canonical_setup_key", ""))
        flow_row["model"] = str(final_row.get("model", ""))
        flow_row["side"] = str(final_row.get("side", "")).upper()
        flow_row["setup_created_ts"] = pd.to_datetime(final_row.get("setup_created_ts", final_row.get("timestamp")), utc=True, errors="coerce")
        flow_row["visible_ts"] = pd.to_datetime(final_row.get("visible_ts"), utc=True, errors="coerce")
        flow_row["entry_anchor_ts"] = pd.to_datetime(final_row.get("entry_anchor_ts"), utc=True, errors="coerce")
        flow_row["first_seen_ts"] = flow_row["visible_ts"]
        flow_row["trigger_refresh_candidate"] = bool(final_row.get("trigger_refresh_candidate", False))
        flow_row["trigger_refresh_reason"] = str(final_row.get("trigger_refresh_reason", ""))
        flow_row["old_canonical_setup_key"] = str(final_row.get("old_canonical_setup_key", ""))
        flow_row["original_setup_created_ts"] = pd.to_datetime(final_row.get("original_setup_created_ts"), utc=True, errors="coerce")
        flow_row["candidate_latest_ts"] = pd.to_datetime(final_row.get("candidate_latest_ts"), utc=True, errors="coerce")
        flow_row["range_retest_score"] = float(final_row.get("range_retest_score", 0.0) or 0.0)
        flow_row["trigger_refresh_applied"] = bool(final_row.get("trigger_refresh_applied", False))

    written = _emit_observation_rows(
        out_csv=out_csv,
        symbol=symbol,
        latest_ts=latest_ts,
        df_e=out_df,
    )
    flow_row["emitted_count"] = int(written)
    if written > 0:
        out_df["lifecycle_state"] = LIFECYCLE_FIRED
    _append_fired_setup_ids(fired_setups_csv, out_df)

    if written > 0:
        first = out_df.iloc[0]
        first_setup_id = str(first.get("setup_id", ""))
        first_canonical_key = str(first.get("canonical_setup_key", ""))
        _append_terminal_lifecycle_row(
            terminal_lifecycle_registry_csv,
            row=first,
            terminal_stage=LIFECYCLE_FIRED,
            terminal_ts=latest_ts,
            reason="emitted_fired",
        )

        entry_ts = pd.to_datetime(out_df["timestamp"].iloc[0], utc=True, errors="coerce")

        if pd.isna(entry_ts):
            print(f"[OPEN_TS_ERROR][{symbol}] missing timestamp setup_id={first_setup_id}")
            return written

        print(
            f"[EXEC_CLOCK] canonical={first_canonical_key} "
            f"setup_created={first.get('setup_created_ts', pd.NaT)} "
            f"signal={entry_ts} trade_open={entry_ts} source=timestamp"
        )

        _mark_position_open(
            symbol,
            first_setup_id,
            entry_ts,
            position_state_csv,
            canonical_setup_key=first_canonical_key,
            setup_created_ts=first.get("setup_created_ts", pd.NaT),
            signal_ts=first.get("signal_ts", pd.NaT),
            wait_confirm_ts=first.get("wait_confirm_ts", pd.NaT),
            wait_context_source=first.get("wait_context_source", ""),
        )
        _append_terminal_lifecycle_row(
            terminal_lifecycle_registry_csv,
            row=first,
            terminal_stage=LIFECYCLE_OPENED,
            terminal_ts=entry_ts,
            reason="position_opened",
        )

    _write_state(state_path, latest_ts)

    if written > 0:
        flow_row["notes"] = "emitted_successfully"
        flow_row["lifecycle_state"] = LIFECYCLE_OPENED
        flow_row["death_stage"] = "emitted"
        flow_row["death_reason"] = "passed_all_filters"
    else:
        flow_row["notes"] = "post_guard_no_emit"
        flow_row["death_stage"] = "stale"
        flow_row["death_reason"] = "post_guard_no_emit"

    _append_flow_row(flow_log_csv, flow_row)

    try:
        preview = out_df[
            [c for c in ("timestamp", "signal_ts", "model", "side", "entry", "sl", "tp", "rr", "phase") if c in out_df.columns]
        ].copy()
        print(f"[OBSERVE][{symbol}] wrote={written} latest_ts={latest_ts}")
        print(preview.tail(min(len(preview), 5)).to_string(index=False))
    except Exception:
        print(f"[OBSERVE][{symbol}] wrote={written} latest_ts={latest_ts}")

    return written


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Minimal live observation shell using pipeline_core as decision authority.")

    ap.add_argument("--symbols", default="BTCUSDT", help="Comma-separated symbols")
    ap.add_argument("--bybit_category", default="linear", help="Bybit category")
    ap.add_argument("--bybit_interval", default="15", help="Bybit candle interval")
    ap.add_argument("--bybit_candles", type=int, default=260, help="How many live candles to fetch")
    ap.add_argument("--window", type=int, default=200, help="Window length passed to pipeline_core")
    ap.add_argument("--portfolio_state", default="backtest/journal/exports_live/portfolio_state.json")
    ap.add_argument("--out_csv", default="backtest/journal/exports_live/live_observation_entries.csv")
    ap.add_argument("--state_dir", default="backtest/journal/exports_live/live_observation_state")
    ap.add_argument("--position_state_csv", default="backtest/journal/exports_live/position_state.csv")
    ap.add_argument("--fired_setups_csv", default="backtest/journal/fired_setups.csv")
    ap.add_argument("--terminal_lifecycle_registry_csv", default="backtest/journal/terminal_lifecycle_registry.csv")
    ap.add_argument("--flow_log_csv", default="backtest/journal/exports_live/flow_log.csv")
    ap.add_argument("--poll_seconds", type=int, default=30, help="Loop sleep when not using --once")
    ap.add_argument("--max_global_no_candles_cycles", type=int, default=20)
    ap.add_argument("--max_global_network_error_cycles", type=int, default=10)
    ap.add_argument("--once", action="store_true", help="Run one cycle only")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug_force_entries", action="store_true")
    ap.add_argument("--use_wait_confirmation", action="store_true")
    ap.add_argument("--candidate_pressure_csv", default="backtest/journal/exports_live/candidate_pressure.csv")

    ap.add_argument("--cluster_score_mode", choices=("LEGACY", "SIGNAL_SCORE"), default=None)
    ap.add_argument("--cluster_max_per_group", type=int, choices=(1, 2, 3), default=None)
    ap.add_argument("--cluster_rank_signal_score", action="store_true")

    ap.add_argument("--rr", type=float, default=2.0)
    ap.add_argument("--sl_atr_buffer", type=float, default=0.15)
    ap.add_argument("--require_impulse_before_tdp", action="store_true")
    ap.add_argument("--impulse_lookback", type=int, default=10)
    ap.add_argument("--impulse_size_atr", type=float, default=1.0)
    ap.add_argument("--tdp_dev_lookback", type=int, default=8)
    ap.add_argument("--tts_retest_lookback", type=int, default=24)

    args = ap.parse_args(argv)

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("--symbols empty")

    out_csv = Path(args.out_csv)
    state_dir = Path(args.state_dir)
    portfolio_state_path = Path(args.portfolio_state)
    position_state_csv = Path(args.position_state_csv)
    fired_setups_csv = Path(args.fired_setups_csv)
    terminal_lifecycle_registry_csv = Path(args.terminal_lifecycle_registry_csv)
    flow_log_csv = Path(args.flow_log_csv)

    _ensure_output_csv(out_csv)
    _ensure_parent(flow_log_csv)

    total_written = 0
    consecutive_global_no_candles_cycles = 0
    consecutive_global_network_error_cycles = 0
    last_successful_data_ts: Optional[pd.Timestamp] = None

    while True:
        cycle_written = 0
        cycle_ts = pd.Timestamp.now("UTC")
        cycle_symbol_had_data = []
        cycle_symbol_network_error = []

        for i, symbol in enumerate(symbols):
            try:
                written_for_symbol = run_symbol_once(
                    cycle_ts=cycle_ts,
                    symbol=symbol,
                    category=str(args.bybit_category),
                    interval=str(args.bybit_interval),
                    candles_n=int(args.bybit_candles),
                    window_n=int(args.window),
                    portfolio_state_path=portfolio_state_path,
                    out_csv=out_csv,
                    state_dir=state_dir,
                    position_state_csv=position_state_csv,
                    fired_setups_csv=fired_setups_csv,
                    terminal_lifecycle_registry_csv=terminal_lifecycle_registry_csv,
                    flow_log_csv=flow_log_csv,
                    debug=bool(args.debug),
                    debug_force_entries=bool(args.debug_force_entries),
                    use_wait_confirmation=bool(args.use_wait_confirmation),
                    candidate_pressure_csv=str(args.candidate_pressure_csv),
                    cluster_score_mode=args.cluster_score_mode,
                    cluster_max_per_group=args.cluster_max_per_group,
                    cluster_rank_signal_score=bool(args.cluster_rank_signal_score),
                    rr=float(args.rr),
                    sl_atr_buffer=float(args.sl_atr_buffer),
                    require_impulse_before_tdp=bool(args.require_impulse_before_tdp),
                    impulse_lookback=int(args.impulse_lookback),
                    impulse_size_atr=float(args.impulse_size_atr),
                    tdp_dev_lookback=int(args.tdp_dev_lookback),
                    tts_retest_lookback=int(args.tts_retest_lookback),
                )
                cycle_written += written_for_symbol

                fetch_status = LAST_BYBIT_FETCH_STATUS.get(str(symbol).upper(), FETCH_STATUS_EMPTY)
                had_data = fetch_status == FETCH_STATUS_OK
                is_network_error = _is_transport_fetch_status(fetch_status)

                cycle_symbol_had_data.append(had_data)
                cycle_symbol_network_error.append(is_network_error)

                if had_data:
                    last_successful_data_ts = cycle_ts

            except Exception as e:
                print(f"[OBSERVE][{symbol}] error={type(e).__name__}: {e}")
                err_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
                err_row["notes"] = f"symbol_exception:{type(e).__name__}:{e}"
                err_row["pipeline_zero_rows"] = True
                err_row["death_stage"] = "no_raw"
                err_row["death_reason"] = f"symbol_exception:{type(e).__name__}"
                _append_flow_row(flow_log_csv, err_row)
                cycle_symbol_had_data.append(False)
                cycle_symbol_network_error.append(True)

            if i < len(symbols) - 1:
                time.sleep(0.3)

        total_written += cycle_written

        if cycle_symbol_had_data and all(not x for x in cycle_symbol_had_data):
            consecutive_global_no_candles_cycles += 1
            print(f"[HEALTH] no_candles_all_symbols consecutive={consecutive_global_no_candles_cycles}")
        else:
            consecutive_global_no_candles_cycles = 0

        if cycle_symbol_network_error and all(cycle_symbol_network_error):
            consecutive_global_network_error_cycles += 1
            print(f"[HEALTH] network_error_all_symbols consecutive={consecutive_global_network_error_cycles}")
        else:
            consecutive_global_network_error_cycles = 0

        print(
            json.dumps(
                {
                    "event": "cycle_done",
                    "cycle_ts": str(cycle_ts),
                    "written_this_cycle": int(cycle_written),
                    "written_total": int(total_written),
                    "symbols": symbols,
                    "consecutive_global_no_candles_cycles": int(consecutive_global_no_candles_cycles),
                    "consecutive_global_network_error_cycles": int(consecutive_global_network_error_cycles),
                    "last_successful_data_ts": str(last_successful_data_ts) if last_successful_data_ts is not None else None,
                },
                ensure_ascii=False,
            )
        )

        if consecutive_global_no_candles_cycles >= int(args.max_global_no_candles_cycles):
            print(
                f"[HEALTH_EXIT] reason=global_no_candles threshold_hit "
                f"consecutive={consecutive_global_no_candles_cycles}"
            )
            raise SystemExit(2)

        if consecutive_global_network_error_cycles >= int(args.max_global_network_error_cycles):
            print(
                f"[HEALTH_EXIT] reason=global_network_error threshold_hit "
                f"consecutive={consecutive_global_network_error_cycles}"
            )
            raise SystemExit(3)

        if args.once:
            break

        time.sleep(max(1, int(args.poll_seconds)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
