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

BYBIT_REST = "https://api.bybit.com"

FLOW_LOG_COLUMNS = [
    "cycle_ts",
    "symbol",
    "latest_ts",
    "setup_id",
    "model",
    "side",
    "setup_created_ts",
    "visible_ts",
    "first_seen_ts",
    "age_candles_at_first_seen",
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
) -> pd.DataFrame:
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
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            j = r.json()
            if j.get("retCode") == 0:
                break

            last_error = f"Bybit error for {symbol}: {j}"
            if int(j.get("retCode", -1)) == 10006 and attempt < 2:
                time.sleep(1.0 + attempt)
                continue

            print(f"[BYBIT][{symbol}] {last_error}")
            return pd.DataFrame()
        except requests.RequestException as e:
            last_error = f"HTTP error for {symbol}: {e}"
            if attempt < 2:
                time.sleep(1.0 + attempt)
                continue
            print(f"[BYBIT][{symbol}] {last_error}")
            return pd.DataFrame()
    else:
        print(f"[BYBIT][{symbol}] {last_error or 'unknown error'}")
        return pd.DataFrame()

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
        return df

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

    return df


def load_bybit_latest(
    category: str,
    symbol: str,
    interval: str,
    candles: int,
) -> pd.DataFrame:
    end = int(pd.Timestamp.utcnow().timestamp() * 1000)
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
    df = _bybit_get_kline(category, symbol, interval, start, end, limit=1000)
    if df.empty:
        return df

    if len(df) > candles:
        df = df.iloc[-candles:].reset_index(drop=True)
    return df


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


def _mark_position_open(symbol: str, setup_id: str, opened_ts: pd.Timestamp, position_state_csv: Path) -> None:
    _ensure_parent(position_state_csv)
    row = pd.DataFrame([
        {
            "symbol": str(symbol).upper(),
            "setup_id": str(setup_id),
            "opened_ts": pd.to_datetime(opened_ts, utc=True, errors="coerce"),
            "status": "OPEN",
            "closed_ts": pd.NA,
            "close_reason": pd.NA,
        }
    ])
    if not position_state_csv.exists() or position_state_csv.stat().st_size == 0:
        row.to_csv(position_state_csv, index=False)
        return

    cols = ["symbol", "setup_id", "opened_ts", "status", "closed_ts", "close_reason"]
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
            existing["status"].astype(str).str.upper() == "OPEN")
        existing = existing.loc[~mask].copy()
    combined = pd.concat([existing, row], ignore_index=True)
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
    use_cols = [c for c in ("setup_id", "symbol", "timestamp", "model", "side", "signal_ts", "observed_ts") if c in rows.columns]
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


def _build_setup_ids(df: pd.DataFrame, symbol: str) -> pd.Series:
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

    if "signal_ts" not in out.columns or out["signal_ts"].isna().all():
        out["signal_ts"] = pd.to_datetime(latest_ts, utc=True, errors="coerce")

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
        "model": "",
        "side": "",
        "setup_created_ts": pd.NaT,

        # execution-visible semantics
        "visible_ts": pd.NaT,
        "first_seen_ts": pd.NaT,
        "age_candles_at_first_seen": 0,
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
        last_le_latest = c.loc[c["timestamp"] <= latest_ts, "timestamp"].max() if len(c) else pd.NaT

        print(f"[TIME_AUDIT][{symbol}] last_row_ts={last_row_ts}")
        print(f"[TIME_AUDIT][{symbol}] last_ts_le_latest={last_le_latest}")
        print(f"[TIME_AUDIT][{symbol}] last_row_eq_last_le_latest={bool(last_row_ts == last_le_latest)}")

        sliced = c.loc[c["timestamp"] <= latest_ts].copy()
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

def model_freshness_filter(df: pd.DataFrame, latest_ts: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df

    rows = []

    for _, row in df.iterrows():
        model = row["model"]
        ts = pd.to_datetime(row["timestamp"], utc=True, errors="coerce")

        age_candles = int((latest_ts - ts).total_seconds() / 900)

        keep = False
        reason = ""

        ALLOWED_EXTRA_BARS = 1

        if model == "RANGE_TOP_SHORT_V2":
            keep = age_candles <= (1 + ALLOWED_EXTRA_BARS)
            reason = "range_fresh" if keep else "range_too_old"

        elif model == "TDP_REENTRY":
            keep = age_candles <= (3 + ALLOWED_EXTRA_BARS)
            reason = "tdp_fresh" if keep else "tdp_too_old"

        else:
            keep = True
            reason = "no_filter"

        print(f"[FRESHNESS] model={model} age={age_candles} keep={keep} reason={reason}")

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
    if candles_df is None or candles_df.empty:
        print(f"[OBSERVE][{symbol}] no candles")
        flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
        flow_row["notes"] = "no_candles"
        flow_row["pipeline_zero_rows"] = True
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    candles_df = candles_df.copy()
    candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"], utc=True, errors="coerce")
    candles_df = candles_df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if candles_df.empty:
        print(f"[OBSERVE][{symbol}] candles empty after normalization")
        flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
        flow_row["notes"] = "candles_empty_after_normalization"
        flow_row["pipeline_zero_rows"] = True
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    latest_ts = pd.to_datetime(candles_df["timestamp"].iloc[-1], utc=True, errors="coerce")
    flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=latest_ts)
    if debug:
        _time_alignment_audit(
            symbol=symbol,
            candles_df=candles_df,
            latest_ts=latest_ts,
            raw_df=None,
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

    # ==============================
    # DISCOVERY GATE (NEW SIGNALS ONLY)
    # ==============================
    if df_e is not None and not df_e.empty:
        try:
            df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
        except Exception:
            pass

        prev_ts = None
        if state_path.exists():
            try:
                prev_ts = pd.to_datetime(_read_state(state_path), utc=True, errors="coerce")
            except Exception:
                prev_ts = None

        current_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")

        ENTRY_WINDOW = pd.Timedelta(minutes=30)  # same bar + 1 extra 15m bar

        if df_e is not None and not df_e.empty:
            try:
                df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
            except Exception:
                pass

            current_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
            entry_window = pd.Timedelta(minutes=30)
            min_ts = current_ts - entry_window

            before = len(df_e)
            df_e = df_e[
                (df_e["timestamp"] > min_ts) &
                (df_e["timestamp"] <= current_ts)
                ].copy()
            after = len(df_e)

            if debug:
                print(
                    f"[DISCOVERY_GATE][{symbol}] "
                    f"window=({min_ts}, {current_ts}] before={before} after={after}"
                )

    if df_e is not None and not df_e.empty:
        df_e = df_e.copy()
        df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
        df_e["visible_ts"] = pd.Timestamp(latest_ts)

        first_row = df_e.iloc[0]
        setup_created_ts = pd.to_datetime(first_row.get("timestamp"), utc=True, errors="coerce")
        visible_ts = pd.to_datetime(first_row.get("visible_ts", latest_ts), utc=True, errors="coerce")

        flow_row["setup_id"] = _build_setup_id(
            symbol,
            setup_created_ts,
            str(first_row.get("model", "")),
            str(first_row.get("side", "")),
        )
        flow_row["model"] = str(first_row.get("model", ""))
        flow_row["side"] = str(first_row.get("side", "")).upper()
        flow_row["setup_created_ts"] = setup_created_ts
        flow_row["visible_ts"] = visible_ts
        flow_row["first_seen_ts"] = visible_ts
        flow_row["age_candles_at_first_seen"] = 0
        flow_row["age_minutes_at_first_seen"] = 0.0

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

    entries = df_e.to_dict("records")

    if bool(use_wait_confirmation):
        entries = apply_wait_confirmation(entries, window)

    entries = df_e.to_dict("records")

    if bool(use_wait_confirmation):
        entries = apply_wait_confirmation(entries, window)

    if not entries:
        entries = df_e.to_dict("records")

    wait_df = pd.DataFrame(entries)
    flow_row["after_wait_count"] = int(len(wait_df))
    flow_row["model_summary_after_wait"] = _series_summary(wait_df, "model")

    out_df = pd.DataFrame(entries)
    out_df = model_freshness_filter(out_df, latest_ts)

    flow_row["after_freshness_count"] = int(len(out_df))

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after freshness latest_ts={latest_ts}")
        flow_row["notes"] = "died_in_freshness"
        flow_row["death_stage"] = "freshness"
        flow_row["death_reason"] = "model_freshness_filter"
        _append_flow_row(flow_log_csv, flow_row)
        return 0


    out_df["setup_id"] = _build_setup_ids(out_df, symbol)

    fired_ids = _load_fired_setup_ids(fired_setups_csv)
    out_df = out_df.loc[~out_df["setup_id"].isin(fired_ids)].copy()
    flow_row["after_idempotency_count"] = int(len(out_df))
    flow_row["model_summary_after_idempotency"] = _series_summary(out_df, "model")

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after idempotency latest_ts={latest_ts}")
        flow_row["after_stale_count"] = flow_row["after_wait_count"]
        flow_row["model_summary_after_stale"] = flow_row["model_summary_after_wait"]
        flow_row["notes"] = "died_in_idempotency"
        flow_row["death_stage"] = "idempotency"
        flow_row["death_reason"] = "already_fired"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    out_df["observed_ts"] = latest_ts

    out_df = filter_live_emit_candidates(out_df, candles_df, latest_ts)
    flow_row["after_stale_count"] = int(len(out_df))
    flow_row["model_summary_after_stale"] = _series_summary(out_df, "model")

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after stale-hit filter latest_ts={latest_ts}")
        flow_row["notes"] = "died_in_stale"
        flow_row["death_stage"] = "stale"
        flow_row["death_reason"] = "filter_live_emit_candidates"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    out_df = select_newest_live_candidate(out_df)
    flow_row["after_per_cycle_guard_count"] = int(len(out_df))
    flow_row["model_summary_after_per_cycle_guard"] = _series_summary(out_df, "model")

    written = _emit_observation_rows(
        out_csv=out_csv,
        symbol=symbol,
        latest_ts=latest_ts,
        df_e=out_df,
    )
    flow_row["emitted_count"] = int(written)
    _append_fired_setup_ids(fired_setups_csv, out_df)

    if written > 0:
        first_setup_id = str(out_df["setup_id"].iloc[0])
        entry_ts = pd.to_datetime(
            out_df["confirm_ts"].iloc[0] if "confirm_ts" in out_df.columns and pd.notna(out_df["confirm_ts"].iloc[0])
            else out_df["timestamp"].iloc[0],
            utc=True,
            errors="coerce",
        )
        _mark_position_open(symbol, first_setup_id, entry_ts, position_state_csv)

    _write_state(state_path, latest_ts)

    if written > 0:
        flow_row["notes"] = "emitted_successfully"
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
    ap.add_argument("--flow_log_csv", default="backtest/journal/exports_live/flow_log.csv")
    ap.add_argument("--poll_seconds", type=int, default=30, help="Loop sleep when not using --once")
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
    flow_log_csv = Path(args.flow_log_csv)

    _ensure_output_csv(out_csv)
    _ensure_parent(flow_log_csv)

    total_written = 0

    while True:
        cycle_written = 0
        cycle_ts = pd.Timestamp.utcnow()

        for i, symbol in enumerate(symbols):
            try:
                cycle_written += run_symbol_once(
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
            except Exception as e:
                print(f"[OBSERVE][{symbol}] error={type(e).__name__}: {e}")
                err_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
                err_row["notes"] = f"symbol_exception:{type(e).__name__}:{e}"
                err_row["pipeline_zero_rows"] = True
                err_row["death_stage"] = "no_raw"
                err_row["death_reason"] = f"symbol_exception:{type(e).__name__}"
                _append_flow_row(flow_log_csv, err_row)

            if i < len(symbols) - 1:
                time.sleep(0.3)

        total_written += cycle_written
        print(
            json.dumps(
                {
                    "event": "cycle_done",
                    "cycle_ts": str(cycle_ts),
                    "written_this_cycle": int(cycle_written),
                    "written_total": int(total_written),
                    "symbols": symbols,
                },
                ensure_ascii=False,
            )
        )

        if args.once:
            break

        time.sleep(max(1, int(args.poll_seconds)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
