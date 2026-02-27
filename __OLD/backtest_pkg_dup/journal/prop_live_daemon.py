"""Prop Live Daemon (Step 14)

Runs live_signal_runner_auto in a loop (prop-safe defaults), with optional report refresh.

PowerShell example:
  python -m backtest.journal.prop_live_daemon `
    --loop_minutes 5 `
    --emit_last_candles 50 `
    --top_n 5 `
    --metric total_R `
    --bad_month_r -10 `
    --bad_month_min_trades 20 `
    --maxdd_threshold -25 `
    --killswitch_r -10 `
    --killswitch_window_days 7 `
    --monthly_action neutral `
    --bybit_interval 30 --bybit_candles 1500 `
    --debug_regime
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_TRADES = "backtest/journal/exports_trades/trades_simulated.csv"
DEFAULT_REPORTS_DIR = "backtest/journal/exports_reports"
DEFAULT_LEADERBOARD = f"{DEFAULT_REPORTS_DIR}/summary_by_symbol.csv"
DEFAULT_MONTHLY_RISK = f"{DEFAULT_REPORTS_DIR}/monthly_risk_by_symbol.csv"

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def log_line(log_path: str, msg: str) -> None:
    _ensure_parent(log_path)
    line = f"[{_utcnow_iso()}] {msg}"
    print(line, flush=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def write_heartbeat(path: str, payload: dict) -> None:
    _ensure_parent(path)
    payload = dict(payload)
    payload["ts"] = _utcnow_iso()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

def run_cmd(cmd: list[str], log_path: str, tag: str) -> int:
    log_line(log_path, f"{tag} exec: {' '.join(cmd)}")
    try:
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.stdout:
            for ln in p.stdout.splitlines():
                log_line(log_path, f"{tag} {ln}")
        if p.stderr:
            for ln in p.stderr.splitlines():
                log_line(log_path, f"{tag} [stderr] {ln}")
        log_line(log_path, f"{tag} exit_code={p.returncode}")
        return p.returncode
    except Exception as e:
        log_line(log_path, f"{tag} ERROR: {type(e).__name__}: {e}")
        return 1

def maybe_refresh_reports(python_exe: str, trades_csv: str, reports_dir: str, top_n: int, log_path: str) -> None:
    cmd = [
        python_exe, "-m", "backtest.journal.analyze_trades_multi",
        "--trades", trades_csv,
        "--out_dir", reports_dir,
        "--top_n", str(top_n)
    ]
    run_cmd(cmd, log_path, tag="[REFRESH]")

def build_live_auto_cmd(
    python_exe: str,
    leaderboard_csv: str,
    monthly_risk_csv: str,
    trades_csv: str,
    metric: str,
    top_n: int,
    bad_month_r: float,
    bad_month_min_trades: int,
    monthly_action: str,
    maxdd_threshold: float,
    killswitch_r: float,
    killswitch_window_days: int,
    emit_last_candles: int,
    bybit_interval: int,
    bybit_candles: int,
    source: str,
    bybit_category: str,
    regime_window_months: int,
    regime_min_trades: int,
    debug_regime: bool,
) -> list[str]:
    cmd = [
        python_exe, "-m", "backtest.journal.live_signal_runner_auto",
        "--leaderboard_csv", leaderboard_csv,
        "--metric", metric,
        "--top_n", str(top_n),
        "--monthly_risk_csv", monthly_risk_csv,
        "--bad_month_r", str(bad_month_r),
        "--bad_month_min_trades", str(bad_month_min_trades),
        "--monthly_action", monthly_action,
        "--trades_csv", trades_csv,
        "--maxdd_threshold", str(maxdd_threshold),
        "--killswitch_r", str(killswitch_r),
        "--killswitch_window_days", str(killswitch_window_days),
        "--",
        "--once",
        "--source", source,
        "--bybit_category", bybit_category,
        "--bybit_interval", str(bybit_interval),
        "--bybit_candles", str(bybit_candles),
        "--regime_perf_csv", trades_csv,
        "--regime_window_months", str(regime_window_months),
        "--regime_min_trades", str(regime_min_trades),
        "--regime_per_symbol",
        "--emit_last_candles", str(emit_last_candles),
    ]
    if debug_regime:
        cmd.append("--debug_regime")
    return cmd

def main() -> int:
    ap = argparse.ArgumentParser(description="Step 14: loop around live_signal_runner_auto with prop-safe guards.")
    ap.add_argument("--loop_minutes", type=int, default=5)
    ap.add_argument("--refresh_reports", action="store_true")
    ap.add_argument("--refresh_every", type=int, default=1)
    ap.add_argument("--log", default="reports/prop_live/prop_live.log")
    ap.add_argument("--heartbeat", default="reports/prop_live/heartbeat.json")

    ap.add_argument("--trades_csv", default=DEFAULT_TRADES)
    ap.add_argument("--reports_dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--leaderboard_csv", default=DEFAULT_LEADERBOARD)
    ap.add_argument("--monthly_risk_csv", default=DEFAULT_MONTHLY_RISK)

    ap.add_argument("--metric", default="total_R")
    ap.add_argument("--top_n", type=int, default=5)

    ap.add_argument("--bad_month_r", type=float, default=-10.0)
    ap.add_argument("--bad_month_min_trades", type=int, default=20)
    ap.add_argument("--monthly_action", choices=["off", "neutral"], default="neutral")
    ap.add_argument("--maxdd_threshold", type=float, default=-25.0)
    ap.add_argument("--killswitch_r", type=float, default=-10.0)
    ap.add_argument("--killswitch_window_days", type=int, default=7)

    ap.add_argument("--emit_last_candles", type=int, default=50)
    ap.add_argument("--source", default="bybit", choices=["bybit", "csv"])
    ap.add_argument("--bybit_category", default="linear")
    ap.add_argument("--bybit_interval", type=int, default=30)
    ap.add_argument("--bybit_candles", type=int, default=1500)
    ap.add_argument("--regime_window_months", type=int, default=12)
    ap.add_argument("--regime_min_trades", type=int, default=10)
    ap.add_argument("--debug_regime", action="store_true")

    args = ap.parse_args()
    python_exe = sys.executable

    log_line(args.log, f"[BOOT] python={python_exe}")
    log_line(args.log, f"[BOOT] trades_csv={args.trades_csv}")

    loop_idx = 0
    while True:
        loop_idx += 1
        write_heartbeat(args.heartbeat, {"status": "running", "loop": loop_idx})

        if args.refresh_reports and (loop_idx % max(1, args.refresh_every) == 0):
            log_line(args.log, f"[REFRESH] loop={loop_idx}")
            maybe_refresh_reports(python_exe, args.trades_csv, args.reports_dir, args.top_n, args.log)

        cmd = build_live_auto_cmd(
            python_exe=python_exe,
            leaderboard_csv=args.leaderboard_csv,
            monthly_risk_csv=args.monthly_risk_csv,
            trades_csv=args.trades_csv,
            metric=args.metric,
            top_n=args.top_n,
            bad_month_r=args.bad_month_r,
            bad_month_min_trades=args.bad_month_min_trades,
            monthly_action=args.monthly_action,
            maxdd_threshold=args.maxdd_threshold,
            killswitch_r=args.killswitch_r,
            killswitch_window_days=args.killswitch_window_days,
            emit_last_candles=args.emit_last_candles,
            bybit_interval=args.bybit_interval,
            bybit_candles=args.bybit_candles,
            source=args.source,
            bybit_category=args.bybit_category,
            regime_window_months=args.regime_window_months,
            regime_min_trades=args.regime_min_trades,
            debug_regime=args.debug_regime,
        )
        rc = run_cmd(cmd, args.log, tag="[AUTO_LOOP]")
        write_heartbeat(args.heartbeat, {"status": "sleeping", "loop": loop_idx, "last_rc": rc})
        time.sleep(max(1, args.loop_minutes) * 60)

if __name__ == "__main__":
    raise SystemExit(main())
