import argparse
import subprocess
import sys
from typing import List, Tuple

import pandas as pd

from backtest.live.symbol_selector import load_leaderboard, pick_top_symbols
from backtest.live.risk_guard import monthly_bad_month_guard, maxdd_guard, killswitch_guard


def _split_args(argv: List[str]) -> Tuple[List[str], List[str]]:
    """
    Split argv into (auto_args, runner_args) using '--' as separator.
    Everything after '--' is passed to live_signal_runner.
    """
    if "--" in argv:
        i = argv.index("--")
        return argv[:i], argv[i + 1 :]
    return argv, []


def _map_monthly_action_to_runner(action: str) -> str:
    # live_signal_runner supports: defensive, off, none
    # We expose: neutral, off
    if action == "neutral":
        return "defensive"
    if action == "off":
        return "off"
    return "none"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m backtest.journal.live_signal_runner_auto",
        description="Auto-pick symbols from leaderboard and run live_signal_runner per symbol with guards.",
    )

    # allowlist / leaderboard
    p.add_argument("--leaderboard_csv", required=True)
    p.add_argument("--metric", default="total_R")
    p.add_argument("--top_n", type=int, default=5)

    # monthly guard (per symbol)
    p.add_argument("--monthly_risk_csv", default="")
    p.add_argument("--bad_month_r", type=float, default=-10.0)
    p.add_argument("--bad_month_min_trades", type=int, default=20)
    p.add_argument("--monthly_action", choices=["off", "neutral"], default="off")

    # maxDD guard (per symbol)
    p.add_argument("--trades_csv", default="")
    p.add_argument("--maxdd_threshold", type=float, default=-25.0)

    # killswitch (per symbol, rolling window on recent trades R)
    p.add_argument("--killswitch_r", type=float, default=-10.0)
    p.add_argument("--killswitch_window_days", type=int, default=7)

    p.add_argument("--dry_run", action="store_true")

    return p


def main() -> int:
    auto_argv, runner_argv = _split_args(sys.argv[1:])
    args = build_parser().parse_args(auto_argv)

    lb = load_leaderboard(args.leaderboard_csv)
    symbols = pick_top_symbols(lb, metric=args.metric, top_n=args.top_n)
    if not symbols:
        print("[AUTO] leaderboard top symbols: (none)")
        return 2

    print(f"[AUTO] leaderboard top symbols: {', '.join(symbols)}")

    allow: List[str] = []
    decisions = {}

    for sym in symbols:
        reasons = []
        ok = True

        # monthly guard
        if args.monthly_risk_csv:
            mg_ok, mg_reason = monthly_bad_month_guard(
                monthly_csv=args.monthly_risk_csv,
                symbol=sym,
                bad_month_r=args.bad_month_r,
                min_trades=args.bad_month_min_trades,
            )
            if not mg_ok:
                ok = False
            reasons.append(f"monthly_guard: {mg_reason}")

        # maxDD guard
        if args.trades_csv and args.maxdd_threshold is not None:
            dd_ok, dd_reason = maxdd_guard(
                trades_csv=args.trades_csv,
                symbol=sym,
                maxdd_threshold=args.maxdd_threshold,
            )
            if not dd_ok:
                ok = False
            reasons.append(f"maxdd_guard: {dd_reason}")

        # killswitch guard
        if args.trades_csv and args.killswitch_window_days:
            ks_ok, ks_reason = killswitch_guard(
                trades_csv=args.trades_csv,
                symbol=sym,
                window_days=args.killswitch_window_days,
                threshold_r=args.killswitch_r,
            )
            if not ks_ok:
                ok = False
            reasons.append(f"killswitch: {ks_reason}")

        decisions[sym] = (ok, "; ".join(reasons))
        print(f"[AUTO] {sym}: {'ALLOW' if ok else 'DENY'} | " + "; ".join(reasons))

        if ok:
            allow.append(sym)

    if args.dry_run:
        for sym in allow:
            runner_action = _map_monthly_action_to_runner(args.monthly_action)
            cmd = [
                sys.executable,
                "-m",
                "backtest.journal.live_signal_runner",
                "--symbols",
                sym,
            ]
            if args.monthly_risk_csv:
                cmd += [
                    "--risk_guard_csv",
                    args.monthly_risk_csv,
                    "--risk_guard_bad_month_r",
                    str(args.bad_month_r),
                    "--risk_guard_min_trades",
                    str(args.bad_month_min_trades),
                    "--risk_guard_action",
                    "none" if runner_action == "off" else runner_action,
                ]
            cmd += runner_argv
            print(f"[AUTO] exec({sym}): " + " ".join([repr(x) for x in cmd]))
        print("[AUTO] dry_run -> not executing runner")
        return 0

    # Execute runner per symbol (safer: isolated state per symbol)
    for sym in allow:
        runner_action = _map_monthly_action_to_runner(args.monthly_action)
        cmd = [
            sys.executable,
            "-m",
            "backtest.journal.live_signal_runner",
            "--symbols",
            sym,
        ]
        if args.monthly_risk_csv:
            cmd += [
                "--risk_guard_csv",
                args.monthly_risk_csv,
                "--risk_guard_bad_month_r",
                str(args.bad_month_r),
                "--risk_guard_min_trades",
                str(args.bad_month_min_trades),
                "--risk_guard_action",
                "none" if runner_action == "off" else runner_action,
            ]
        cmd += runner_argv

        print(f"[AUTO] exec({sym}): " + " ".join([repr(x) for x in cmd]))
        proc = subprocess.run(cmd)
        if proc.returncode != 0:
            return proc.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
