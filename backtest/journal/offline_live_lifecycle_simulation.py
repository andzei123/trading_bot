from __future__ import annotations

"""Offline full lifecycle simulation runner.

Goal:
    Replay historical candles bar-by-bar using the same production decision path
    as live, while instrumenting the full setup/trade lifecycle.

Non-goals:
    - no strategy changes
    - no threshold / RR / stale / wait tweaks
    - no alternate signal generation path

This runner intentionally mirrors the observable live sequence as closely as
possible:

    candles
    -> pipeline_core.run_pipeline_once(...)
    -> wait confirmation
    -> freshness
    -> idempotency
    -> stale/live emit guard
    -> per-symbol position gate
    -> emit
    -> open trade
    -> close trade
    -> log full lifecycle

Primary outputs:
    - trades_full_simulation.csv
    - setup_lifecycle_log.csv
    - simulation_summary.csv
"""

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import pandas as pd

from backtest.engine.execution import ExecutionSimulator
from backtest.live_pipeline.pipeline_core import run_pipeline_once
from backtest.metrics.equity_curve_tracker import update_equity_curve_from_trades
from backtest.metrics.symbol_performance_tracker import update_symbol_performance
from backtest.portfolio.portfolio_exposure import load_portfolio_exposure
from backtest.utils.wait_confirmation import apply_wait_confirmation

try:
    from backtest.journal.live_emit_guard import filter_live_emit_candidates, select_newest_live_candidate
except Exception:
    from live_emit_guard import filter_live_emit_candidates, select_newest_live_candidate  # type: ignore


RANGE_SHORT_REENTRY_GAP = 5

# -----------------------------------------------------------------------------
# Shared helpers
# -----------------------------------------------------------------------------


def _parse_dt(s: str) -> pd.Timestamp:
    ts = pd.to_datetime(s, utc=True, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Bad datetime: {s}")
    return ts



def _minutes(a: Optional[pd.Timestamp], b: Optional[pd.Timestamp]) -> Optional[float]:
    if a is None or b is None or pd.isna(a) or pd.isna(b):
        return None
    return float((pd.Timestamp(b) - pd.Timestamp(a)).total_seconds() / 60.0)


def _candles_between(df: pd.DataFrame, start_ts: Optional[pd.Timestamp], end_ts: Optional[pd.Timestamp]) -> Optional[int]:
    if start_ts is None or end_ts is None or pd.isna(start_ts) or pd.isna(end_ts):
        return None
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    try:
        return int(((ts > pd.Timestamp(start_ts)) & (ts <= pd.Timestamp(end_ts))).sum())
    except Exception:
        return None

def _should_allow_idempotency_override(
    *,
    df: pd.DataFrame,
    symbol: str,
    model: str,
    side: str,
    current_ts: pd.Timestamp,
    last_fired_ts_map: Dict[tuple, pd.Timestamp],
    min_gap_candles: int,
) -> bool:
    """
    Allow model-specific re-entry only for RANGE_TOP_SHORT_V2 SHORT.

    Returns True if candidate should bypass normal fired_setup_ids blocking.
    """
    sym = str(symbol).upper()
    mdl = str(model).upper()
    sd = str(side).upper()

    if not (mdl == "RANGE_TOP_SHORT_V2" and sd == "SHORT"):
        return False

    key = (sym, mdl, sd)
    last_ts = last_fired_ts_map.get(key)
    if last_ts is None or pd.isna(last_ts):
        return True

    gap = _candles_between(df, pd.Timestamp(last_ts), pd.Timestamp(current_ts))
    if gap is None:
        return False

    return int(gap) >= int(min_gap_candles)

def _build_setup_id(symbol: str, timestamp, model: str, side: str) -> str:
    ts = pd.to_datetime(timestamp, utc=True, errors="coerce")
    return f"{str(symbol).upper()}|{ts}|{str(model).upper()}|{str(side).upper()}"



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



def _append_fired_setup(path: Path, row: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = {
        "setup_id": row.get("setup_id"),
        "symbol": row.get("symbol"),
        "timestamp": row.get("timestamp"),
        "model": row.get("model"),
        "side": row.get("side"),
        "signal_ts": row.get("signal_ts"),
        "observed_ts": row.get("observed_ts"),
    }
    df = pd.DataFrame([out])
    if not path.exists() or path.stat().st_size == 0:
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode="a", header=False, index=False)



def _load_symbol_candles(candles_dir: Path, symbol: str) -> pd.DataFrame:
    candidates = [
        candles_dir / f"{symbol}.csv",
        candles_dir / f"{symbol}_15m.csv",
        candles_dir / f"{symbol}_15.csv",
    ]
    p = None
    for c in candidates:
        if c.exists():
            p = c
            break
    if p is None:
        raise FileNotFoundError(f"No candles CSV for {symbol} in {candles_dir}")

    df = pd.read_csv(p)

    if "timestamp" not in df.columns:
        for alt in ("time", "date", "ts"):
            if alt in df.columns:
                df = df.rename(columns={alt: "timestamp"})
                break

    required = {"timestamp", "open", "high", "low", "close"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{p} missing columns: {missing}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

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


# -----------------------------------------------------------------------------
# Live-shell-equivalent freshness logic
# -----------------------------------------------------------------------------


def model_freshness_filter(df: pd.DataFrame, latest_ts: pd.Timestamp) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[] if df is None else list(df.columns))

    rows = []
    for _, row in df.iterrows():
        model = str(row.get("model", "")).upper()
        ts = pd.to_datetime(row.get("timestamp"), utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        age_candles = int((pd.Timestamp(latest_ts) - ts).total_seconds() / 900)

        keep = False
        if model == "RANGE_TOP_SHORT_V2":
            keep = age_candles <= 1
        elif model == "TDP_REENTRY":
            keep = age_candles <= 3
        else:
            keep = True

        if keep:
            rows.append(row)

    return pd.DataFrame(rows, columns=df.columns)


# -----------------------------------------------------------------------------
# Output schemas
# -----------------------------------------------------------------------------

SETUP_LIFECYCLE_COLUMNS = [
    "setup_id",
    "symbol",
    "model",
    "side",
    "setup_created_ts",
    "visible_ts",
    "first_seen_ts",
    "phase",
    "regime",
    "sub_label",
    "rr_planned",
    "age_candles_at_first_seen",
    "age_minutes_at_first_seen",
    "wait_input_ts",
    "wait_confirm_ts",
    "freshness_check_ts",
    "emit_ts",
    "setup_age_candles_at_emit",
    "setup_age_minutes_at_emit",
    "age_candles_at_death",
    "age_minutes_at_death",
    "death_stage",
    "death_reason",
    "trade_open_ts",
    "trade_close_ts",
    "trade_lifetime_candles",
    "trade_lifetime_minutes",
    "exit_reason",
    "r_realized",
    "notes",
]

TRADES_FULL_COLUMNS = [
    "idx",
    "setup_id",
    "symbol",
    "model",
    "side",
    "setup_created_ts",
    "visible_ts",
    "first_seen_ts",
    "emit_ts",
    "trade_open_ts",
    "trade_close_ts",
    "trade_lifetime_candles",
    "trade_lifetime_minutes",
    "entry",
    "sl",
    "tp",
    "rr_planned",
    "r_realized",
    "phase",
    "regime",
    "sub_label",
    "exit_reason",
    "outcome",
    "exit_price",
    "exit_idx",
    "bars_held",
    "setup_age_candles_at_emit",
    "setup_age_minutes_at_emit",
    "wait_input_ts",
    "wait_confirm_ts",
    "freshness_check_ts",
    "notes",
]



def _ensure_csv(path: Path, columns: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(list(columns))



def _append_row(path: Path, fieldnames: List[str], row: Dict) -> None:
    file_exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


# -----------------------------------------------------------------------------
# Lifecycle structures
# -----------------------------------------------------------------------------

@dataclass
class ActivePosition:
    setup_id: str
    symbol: str
    open_ts: pd.Timestamp
    close_ts: Optional[pd.Timestamp]
    exit_idx: Optional[int]



# -----------------------------------------------------------------------------
# Main simulation
# -----------------------------------------------------------------------------


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Offline full lifecycle simulation using the production decision pipeline."
    )
    ap.add_argument("--symbols", default="BTCUSDT", help="Comma-separated symbols")
    ap.add_argument("--from", dest="dt_from", required=False)
    ap.add_argument("--start", dest="dt_from", required=False)
    ap.add_argument("--to", dest="dt_to", required=False)
    ap.add_argument("--end", dest="dt_to", required=False)
    ap.add_argument("--candles_dir", default="backtest/data")
    ap.add_argument("--portfolio_state", default="backtest/journal/exports_live/portfolio_state.json")
    ap.add_argument("--fired_setups_csv", default="backtest/journal/fired_setups.csv")
    ap.add_argument("--out_trades_full", default="backtest/journal/trades_full_simulation.csv")
    ap.add_argument("--out_setup_lifecycle", default="backtest/journal/setup_lifecycle_log.csv")
    ap.add_argument("--out_summary", default="backtest/journal/simulation_summary.csv")
    ap.add_argument("--out_trades_compat", default="backtest/journal/trades.csv")
    ap.add_argument("--use_wait_confirmation", action="store_true")
    ap.add_argument("--cluster_score_mode", choices=("LEGACY", "SIGNAL_SCORE"), default=None)
    ap.add_argument("--cluster_max_per_group", type=int, choices=(1, 2, 3), default=None)
    ap.add_argument("--cluster_rank_signal_score", action="store_true")
    ap.add_argument("--window", type=int, default=200)
    ap.add_argument("--bybit_interval", type=int, default=15)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug_force_entries", action="store_true")
    ap.add_argument("--candidate_pressure_csv", default="backtest/journal/exports_live/candidate_pressure.csv")
    ap.add_argument("--summary_initial_equity", type=float, default=10_000.0)
    args = ap.parse_args(argv)

    if not args.dt_from or not args.dt_to:
        ap.error("Missing date range: provide --from/--to or --start/--end")

    dt_from = _parse_dt(args.dt_from)
    dt_to = _parse_dt(args.dt_to)
    if dt_to <= dt_from:
        raise SystemExit("--to/--end must be > --from/--start")

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    if not symbols:
        raise SystemExit("--symbols empty")

    cluster_score_mode = args.cluster_score_mode
    if args.cluster_rank_signal_score:
        if cluster_score_mode is not None and cluster_score_mode != "SIGNAL_SCORE":
            ap.error("--cluster_rank_signal_score conflicts with --cluster_score_mode LEGACY")
        cluster_score_mode = "SIGNAL_SCORE"

    candles_dir = Path(args.candles_dir)
    pf_path = Path(args.portfolio_state)
    fired_setups_path = Path(args.fired_setups_csv)
    out_trades_full = Path(args.out_trades_full)
    out_setup_lifecycle = Path(args.out_setup_lifecycle)
    out_summary = Path(args.out_summary)
    out_trades_compat = Path(args.out_trades_compat)

    _ensure_csv(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS)
    _ensure_csv(out_trades_full, TRADES_FULL_COLUMNS)

    fired_setup_ids = _load_fired_setup_ids(fired_setups_path)
    candles_by_symbol: Dict[str, pd.DataFrame] = {s: _load_symbol_candles(candles_dir, s) for s in symbols}

    timelines = []
    for s, df in candles_by_symbol.items():
        sub = df[(df["timestamp"] >= dt_from) & (df["timestamp"] <= dt_to)].copy()
        timelines.append(sub["timestamp"])
        candles_by_symbol[s] = df.reset_index(drop=True)

    if not timelines or all(t.empty for t in timelines):
        print("[SIM] no candles in requested range")
        return 0

    all_ts = pd.Index(sorted(set().union(*[set(t.tolist()) for t in timelines if not t.empty])))

    active_positions: Dict[str, ActivePosition] = {}
    trade_idx = 0
    seen_trades = set()

    # replay discovery state: "new" means first time visible in pipeline output
    seen_setup_ids: Set[str] = set()
    first_seen_cycle_by_setup_id: Dict[str, str] = {}
    last_fired_ts_map: Dict[tuple, pd.Timestamp] = {}
    death_counts: Dict[str, int] = {}
    model_counts: Dict[str, int] = {}
    emitted_count = 0
    opened_count = 0
    closed_count = 0

    for ts in all_ts:
        portfolio_state = load_portfolio_exposure(pf_path)

        for sym in symbols:
            df_full = candles_by_symbol[sym]

            cutoff = df_full["timestamp"].searchsorted(ts, side="right")
            if cutoff <= 0:
                continue

            df_up_to = df_full.iloc[:cutoff]
            window = df_up_to.tail(int(args.window)).copy().reset_index(drop=True)
            latest_ts = pd.to_datetime(ts, utc=True, errors="coerce")

            force_flag = bool(args.debug_force_entries)
            ctx = {
                "latest_ts": latest_ts,
                "bybit_interval": int(args.bybit_interval),
                "macro_bias": "NEUTRAL",
                "debug": bool(args.debug),
                "use_wait_confirmation": bool(args.use_wait_confirmation),
                "candidate_pressure_csv": args.candidate_pressure_csv,
                **({"cluster_score_mode": cluster_score_mode} if cluster_score_mode is not None else {}),
                **({"cluster_rank_signal_score": True} if cluster_score_mode == "SIGNAL_SCORE" else {}),
                **({"cluster_max_per_group": int(args.cluster_max_per_group)} if args.cluster_max_per_group is not None else {}),
                "rr": 2.0,
                "sl_atr_buffer": 0.15,
                "require_impulse_before_tdp": False,
                "impulse_lookback": 10,
                "impulse_size_atr": 1.0,
                "tdp_dev_lookback": 8,
                "tts_retest_lookback": 24,
                "disable_invalidation": True,
                "debug_force_entries": force_flag,
                "force_entries": force_flag,
                "debug_entry_force": force_flag,
                "DEBUG_FORCE_ENTRIES": force_flag,
            }

            df_e = run_pipeline_once(
                symbol=sym,
                candles_df=window,
                ctx=ctx,
                portfolio_state=portfolio_state,
                debug=bool(args.debug),
            )

            if df_e is None or df_e.empty:
                death_counts["no_raw"] = death_counts.get("no_raw", 0) + 1
                continue

            # execution-layer semantics:
            # keep structural/origin timestamp intact, but attach pipeline visibility time
            df_e = df_e.copy()
            df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
            df_e["structural_ts"] = df_e["timestamp"]
            df_e["visible_ts"] = pd.Timestamp(latest_ts)

            raw_rows = []
            for _, r in df_e.iterrows():
                setup_created_ts = pd.to_datetime(r.get("structural_ts", r.get("timestamp")), utc=True, errors="coerce")
                if pd.isna(setup_created_ts):
                    continue
                side = str(r.get("side", "")).upper()
                model = str(r.get("model", "")).upper()
                setup_id = _build_setup_id(sym, setup_created_ts, model, side)
                if setup_id in seen_setup_ids:
                    continue
                seen_setup_ids.add(setup_id)
                first_seen_cycle_by_setup_id[setup_id] = pd.Timestamp(latest_ts).isoformat()
                raw_rows.append(dict(r))

            if not raw_rows:
                death_counts["no_raw"] = death_counts.get("no_raw", 0) + 1
                continue

            for raw in raw_rows:
                setup_created_ts = pd.to_datetime(raw.get("structural_ts", raw.get("timestamp")), utc=True, errors="coerce")
                side = str(raw.get("side", "")).upper()
                model = str(raw.get("model", "")).upper()
                setup_id = _build_setup_id(sym, setup_created_ts, model, side)
                sub_label = raw.get("ctx_sub_label") or raw.get("sub_label") or ""
                visible_ts = pd.to_datetime(raw.get("visible_ts", latest_ts), utc=True, errors="coerce")
                first_seen_ts = pd.to_datetime(
                    first_seen_cycle_by_setup_id.get(setup_id, visible_ts.isoformat()),
                    utc=True,
                    errors="coerce",
                )

                base_lifecycle = {
                    "setup_id": setup_id,
                    "symbol": sym,
                    "model": model,
                    "side": side,
                    "setup_created_ts": setup_created_ts.isoformat() if pd.notna(setup_created_ts) else "",
                    "visible_ts": visible_ts.isoformat() if pd.notna(visible_ts) else "",
                    "first_seen_ts": first_seen_ts.isoformat() if pd.notna(first_seen_ts) else "",
                    "phase": raw.get("phase", ""),
                    "regime": raw.get("regime", ""),
                    "sub_label": sub_label,
                    "rr_planned": raw.get("rr", ""),
                    "age_candles_at_first_seen": _candles_between(df_full, visible_ts, visible_ts),
                    "age_minutes_at_first_seen": _minutes(visible_ts, visible_ts),
                    "wait_input_ts": visible_ts.isoformat() if pd.notna(visible_ts) else "",
                    "wait_confirm_ts": "",
                    "freshness_check_ts": "",
                    "emit_ts": "",
                    "setup_age_candles_at_emit": "",
                    "setup_age_minutes_at_emit": "",
                    "age_candles_at_death": "",
                    "age_minutes_at_death": "",
                    "death_stage": "no_raw",
                    "death_reason": "",
                    "trade_open_ts": "",
                    "trade_close_ts": "",
                    "trade_lifetime_candles": "",
                    "trade_lifetime_minutes": "",
                    "exit_reason": "",
                    "r_realized": "",
                    "notes": "raw_candidate_detected",
                }
                model_counts[model] = model_counts.get(model, 0) + 1

                wait_seed = dict(raw)
                wait_seed["timestamp"] = pd.Timestamp(setup_created_ts)
                wait_entries = [wait_seed]

                # do not pass full history into wait confirmation;
                # replay should only see candles available up to current cycle
                wait_candles = df_up_to.tail(max(int(args.window), 64)).copy().reset_index(drop=True)

                if bool(ctx.get("use_wait_confirmation", False)):
                    wait_entries = apply_wait_confirmation(wait_entries, wait_candles)
                if not wait_entries:
                    base_lifecycle["death_stage"] = "wait"
                    base_lifecycle["death_reason"] = "wait_confirmation_failed"
                    base_lifecycle["age_candles_at_death"] = _candles_between(df_full, visible_ts, visible_ts)
                    base_lifecycle["age_minutes_at_death"] = _minutes(visible_ts, visible_ts)
                    death_counts["wait"] = death_counts.get("wait", 0) + 1
                    _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)
                    continue

                wait_row = dict(wait_entries[0])
                wait_confirm_ts = pd.to_datetime(wait_row.get("timestamp"), utc=True, errors="coerce")
                base_lifecycle["wait_confirm_ts"] = wait_confirm_ts.isoformat() if pd.notna(wait_confirm_ts) else ""
                base_lifecycle["freshness_check_ts"] = visible_ts.isoformat()

                fresh_df = model_freshness_filter(pd.DataFrame([wait_row]), visible_ts)
                if fresh_df.empty:
                    base_lifecycle["death_stage"] = "freshness"
                    base_lifecycle["death_reason"] = "model_freshness_filter"
                    base_lifecycle["age_candles_at_death"] = _candles_between(df_full, visible_ts, visible_ts)
                    base_lifecycle["age_minutes_at_death"] = _minutes(visible_ts, visible_ts)
                    death_counts["freshness"] = death_counts.get("freshness", 0) + 1
                    _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)
                    continue

                fresh_row = fresh_df.iloc[0].to_dict()
                fresh_row["setup_id"] = setup_id

                allow_range_short_reentry = _should_allow_idempotency_override(
                    df=df_full,
                    symbol=sym,
                    model=model,
                    side=side,
                    current_ts=wait_confirm_ts,
                    last_fired_ts_map=last_fired_ts_map,
                    min_gap_candles=RANGE_SHORT_REENTRY_GAP,
                )

                if setup_id in fired_setup_ids and not allow_range_short_reentry:
                    base_lifecycle["death_stage"] = "idempotency"
                    base_lifecycle["death_reason"] = "already_fired"
                    base_lifecycle["age_candles_at_death"] = _candles_between(df_full, visible_ts, visible_ts)
                    base_lifecycle["age_minutes_at_death"] = _minutes(visible_ts, visible_ts)
                    death_counts["idempotency"] = death_counts.get("idempotency", 0) + 1
                    _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)
                    continue

                stale_df = filter_live_emit_candidates(pd.DataFrame([fresh_row]), df_up_to, latest_ts)
                if stale_df.empty:
                    base_lifecycle["death_stage"] = "stale"
                    base_lifecycle["death_reason"] = "filter_live_emit_candidates"
                    base_lifecycle["age_candles_at_death"] = _candles_between(df_full, visible_ts, visible_ts)
                    base_lifecycle["age_minutes_at_death"] = _minutes(visible_ts, visible_ts)
                    death_counts["stale"] = death_counts.get("stale", 0) + 1
                    _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)
                    continue

                gated_df = select_newest_live_candidate(stale_df)
                if gated_df.empty:
                    base_lifecycle["death_stage"] = "stale"
                    base_lifecycle["death_reason"] = "post_guard_empty"
                    base_lifecycle["age_candles_at_death"] = _candles_between(df_full, visible_ts, visible_ts)
                    base_lifecycle["age_minutes_at_death"] = _minutes(visible_ts, visible_ts)
                    death_counts["stale"] = death_counts.get("stale", 0) + 1
                    _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)
                    continue

                if sym in active_positions:
                    active = active_positions[sym]
                    if active.close_ts is None or latest_ts <= active.close_ts:
                        base_lifecycle["death_stage"] = "position_gate"
                        base_lifecycle["death_reason"] = "open_position_exists"
                        base_lifecycle["age_candles_at_death"] = _candles_between(df_full, visible_ts, visible_ts)
                        base_lifecycle["age_minutes_at_death"] = _minutes(visible_ts, visible_ts)
                        death_counts["position_gate"] = death_counts.get("position_gate", 0) + 1
                        _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)
                        continue
                    active_positions.pop(sym, None)

                emit_row = gated_df.iloc[0].to_dict()
                emit_ts = visible_ts
                base_lifecycle["emit_ts"] = emit_ts.isoformat()
                base_lifecycle["setup_age_candles_at_emit"] = _candles_between(df_full, visible_ts, emit_ts)
                base_lifecycle["setup_age_minutes_at_emit"] = _minutes(visible_ts, emit_ts)
                base_lifecycle["death_stage"] = "emitted"
                emitted_count += 1

                entry_exec_ts = pd.to_datetime(emit_row.get("timestamp", visible_ts), utc=True, errors="coerce")
                try:
                    entry_idx = int(df_full[df_full["timestamp"] <= entry_exec_ts].index.max())
                except Exception:
                    entry_idx = None
                if entry_idx is None or entry_idx < 0:
                    base_lifecycle["death_stage"] = "emitted"
                    _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)
                    continue

                side = str(emit_row.get("side", "")).upper()
                entry = float(emit_row.get("entry"))
                sl = float(emit_row.get("sl"))
                tp = float(emit_row.get("tp"))
                trade_key = (str(setup_id), sym, side, entry, sl, tp, str(emit_ts))
                if trade_key in seen_trades:
                    base_lifecycle["death_stage"] = "idempotency"
                    death_counts["idempotency"] = death_counts.get("idempotency", 0) + 1
                    _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)
                    continue
                seen_trades.add(trade_key)

                sim = ExecutionSimulator(df_full)
                res = sim.simulate(entry_idx=entry_idx, side=side, entry=entry, sl=sl, tp=tp)

                trade_open_ts = pd.to_datetime(df_full["timestamp"].iloc[entry_idx], utc=True, errors="coerce")
                trade_close_ts = None
                if res is not None and getattr(res, "exit_idx", None) is not None:
                    exi = int(getattr(res, "exit_idx"))
                    if 0 <= exi < len(df_full):
                        trade_close_ts = pd.to_datetime(df_full["timestamp"].iloc[exi], utc=True, errors="coerce")

                risk = (entry - sl) if side == "LONG" else (sl - entry)
                if risk == 0:
                    r_realized = 0.0
                else:
                    exit_price = float(getattr(res, "exit_price", entry)) if res is not None else entry
                    if side == "LONG":
                        r_realized = (exit_price - entry) / risk
                    else:
                        r_realized = (entry - exit_price) / risk

                bars_held = getattr(res, "bars_held", "") if res is not None else ""
                exit_reason = str(getattr(res, "outcome", "")) if res is not None else ""
                outcome = exit_reason
                exit_idx = getattr(res, "exit_idx", "") if res is not None else ""
                exit_price = getattr(res, "exit_price", "") if res is not None else ""

                fired_setup_ids.add(setup_id)
                _append_fired_setup(
                    fired_setups_path,
                    {
                        "setup_id": setup_id,
                        "symbol": sym,
                        "timestamp": setup_created_ts.isoformat() if pd.notna(setup_created_ts) else "",
                        "model": model,
                        "side": side,
                        "signal_ts": setup_created_ts.isoformat() if pd.notna(setup_created_ts) else "",
                        "observed_ts": emit_ts.isoformat(),
                    },
                )
                last_fired_ts_map[(str(sym).upper(), str(model).upper(), str(side).upper())] = pd.Timestamp(wait_confirm_ts)
                base_lifecycle["death_stage"] = "closed" if trade_close_ts is not None else "opened"
                base_lifecycle["death_reason"] = "sim_closed" if trade_close_ts is not None else "sim_opened"
                base_lifecycle["trade_open_ts"] = trade_open_ts.isoformat() if pd.notna(trade_open_ts) else ""
                base_lifecycle["trade_close_ts"] = trade_close_ts.isoformat() if pd.notna(trade_close_ts) else ""
                base_lifecycle["trade_lifetime_candles"] = _candles_between(df_full, trade_open_ts, trade_close_ts)
                base_lifecycle["trade_lifetime_minutes"] = _minutes(trade_open_ts, trade_close_ts)
                if trade_close_ts is not None:
                    base_lifecycle["age_candles_at_death"] = _candles_between(df_full, visible_ts, trade_close_ts)
                    base_lifecycle["age_minutes_at_death"] = _minutes(visible_ts, trade_close_ts)
                else:
                    base_lifecycle["age_candles_at_death"] = _candles_between(df_full, visible_ts, trade_open_ts)
                    base_lifecycle["age_minutes_at_death"] = _minutes(visible_ts, trade_open_ts)
                base_lifecycle["exit_reason"] = exit_reason
                base_lifecycle["r_realized"] = r_realized
                base_lifecycle["notes"] = "offline_full_lifecycle_simulation"
                _append_row(out_setup_lifecycle, SETUP_LIFECYCLE_COLUMNS, base_lifecycle)

                if trade_close_ts is not None:
                    active_positions[sym] = ActivePosition(
                        setup_id=setup_id,
                        symbol=sym,
                        open_ts=trade_open_ts,
                        close_ts=trade_close_ts,
                        exit_idx=int(exit_idx) if exit_idx != "" else None,
                    )
                    closed_count += 1
                else:
                    active_positions[sym] = ActivePosition(
                        setup_id=setup_id,
                        symbol=sym,
                        open_ts=trade_open_ts,
                        close_ts=None,
                        exit_idx=None,
                    )
                opened_count += 1

                _append_row(
                    out_trades_full,
                    TRADES_FULL_COLUMNS,
                    {
                        "idx": trade_idx,
                        "setup_id": setup_id,
                        "symbol": sym,
                        "model": model,
                        "side": side,
                        "setup_created_ts": setup_created_ts.isoformat() if pd.notna(setup_created_ts) else "",
                        "visible_ts": visible_ts.isoformat() if pd.notna(visible_ts) else "",
                        "first_seen_ts": first_seen_ts.isoformat() if pd.notna(first_seen_ts) else "",
                        "emit_ts": emit_ts.isoformat(),
                        "trade_open_ts": trade_open_ts.isoformat() if pd.notna(trade_open_ts) else "",
                        "trade_close_ts": trade_close_ts.isoformat() if pd.notna(trade_close_ts) else "",
                        "trade_lifetime_candles": _candles_between(df_full, trade_open_ts, trade_close_ts),
                        "trade_lifetime_minutes": _minutes(trade_open_ts, trade_close_ts),
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "rr_planned": emit_row.get("rr", ""),
                        "r_realized": r_realized,
                        "phase": emit_row.get("phase", ""),
                        "regime": emit_row.get("regime", ""),
                        "sub_label": emit_row.get("ctx_sub_label") or emit_row.get("sub_label") or "",
                        "exit_reason": exit_reason,
                        "outcome": outcome,
                        "exit_price": exit_price,
                        "exit_idx": exit_idx,
                        "bars_held": bars_held,
                        "setup_age_candles_at_emit": _candles_between(df_full, visible_ts, emit_ts),
                        "setup_age_minutes_at_emit": _minutes(visible_ts, emit_ts),
                        "wait_input_ts": visible_ts.isoformat() if pd.notna(visible_ts) else "",
                        "wait_confirm_ts": wait_confirm_ts.isoformat() if pd.notna(wait_confirm_ts) else "",
                        "freshness_check_ts": visible_ts.isoformat() if pd.notna(visible_ts) else "",
                        "notes": "offline_full_lifecycle_simulation",
                    },
                )
                trade_idx += 1

    # backward-compatible exports from the enriched trade log
    try:
        trades_df = pd.read_csv(out_trades_full)
        if not trades_df.empty:
            compat = pd.DataFrame(
                {
                    "idx": trades_df["idx"],
                    "timestamp": trades_df["setup_created_ts"],
                    "reason": trades_df["model"],
                    "side": trades_df["side"],
                    "entry": trades_df["entry"],
                    "sl": trades_df["sl"],
                    "tp": trades_df["tp"],
                    "rr": trades_df["rr_planned"],
                    "R": trades_df["r_realized"],
                    "phase": trades_df["phase"],
                    "regime": trades_df["regime"],
                    "score": "",
                    "notes": trades_df["notes"],
                    "outcome": trades_df["outcome"],
                    "exit_price": trades_df["exit_price"],
                    "exit_idx": trades_df["exit_idx"],
                    "bars_held": trades_df["bars_held"],
                    "exit_timestamp": trades_df["trade_close_ts"],
                    "symbol": trades_df["symbol"],
                    "setup_id": trades_df["setup_id"],
                }
            )
            compat.to_csv(out_trades_compat, index=False)
            update_equity_curve_from_trades(
                trades_csv=str(out_trades_compat),
                out_csv="backtest/journal/exports_live/equity_curve.csv",
                initial_equity=float(args.summary_initial_equity),
                window_trades=60,
            )
            update_symbol_performance(
                trades_csv=str(out_trades_compat),
                out_csv="backtest/journal/exports_live/symbol_performance.csv",
            )
    except Exception as e:
        print(f"[SIM][EXPORT_WARN] {repr(e)}")

    summary_rows = [
        {"metric": "raw_setups", "value": sum(model_counts.values())},
        {"metric": "emitted", "value": emitted_count},
        {"metric": "opened", "value": opened_count},
        {"metric": "closed", "value": closed_count},
    ]
    for k, v in sorted(death_counts.items()):
        summary_rows.append({"metric": f"death_stage::{k}", "value": v})
    for k, v in sorted(model_counts.items()):
        summary_rows.append({"metric": f"model_raw::{k}", "value": v})
    pd.DataFrame(summary_rows).to_csv(out_summary, index=False)

    print(
        f"[SIM] done raw_setups={sum(model_counts.values())} emitted={emitted_count} "
        f"opened={opened_count} closed={closed_count}"
    )
    print(f"[SIM] setup log -> {out_setup_lifecycle}")
    print(f"[SIM] trades    -> {out_trades_full}")
    print(f"[SIM] summary   -> {out_summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
