"""
Paper executor: reads a snapshot of live signals (signals_live.csv) and maintains
a lightweight paper trades ledger (paper_trades.csv).

Design goals:
- Never crash the live loop.
- Always ensure output CSV exists with stable header.
- Tolerant to empty/missing signals file (creates paper_trades header and exits).
- Uses `signal_ts` if present for trade timestamp; falls back to `timestamp`.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd
from backtest.risk.policy_engine import evaluate_policy_has_open

DEFAULT_SIGNALS = Path("backtest/journal/exports_live/signals_live.csv")
DEFAULT_OUT = Path("backtest/journal/exports_live/paper_trades.csv")

PAPER_COLUMNS = [
    "timestamp",
    "signal_ts",
    "symbol",
    "model",
    "side",
    "entry",
    "sl",
    "tp",
    "rr",
    "ctx_sub_label",
    "phase",
    "regime",
    "trend_dir",
    "status",
]


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def ensure_paper_trades_file(out_path: str | Path = DEFAULT_OUT) -> Path:
    out_p = Path(out_path)
    _ensure_parent(out_p)
    if (not out_p.exists()) or out_p.stat().st_size == 0:
        pd.DataFrame(columns=PAPER_COLUMNS).to_csv(out_p, index=False)
    return out_p


def _read_signals(in_csv: str | Path) -> pd.DataFrame:
    p = Path(in_csv)
    if (not p.exists()) or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, engine="python", on_bad_lines="skip")
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    # Normalize timestamps
    if "signal_ts" in df.columns:
        df["signal_ts"] = pd.to_datetime(df["signal_ts"], utc=True, errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # Sort so newest is last
    if "signal_ts" in df.columns and df["signal_ts"].notna().any():
        df = df.sort_values("signal_ts")
    elif "timestamp" in df.columns and df["timestamp"].notna().any():
        df = df.sort_values("timestamp")

    return df


def run_paper_executor(
    in_csv: str | Path = DEFAULT_SIGNALS,
    out_csv: str | Path = DEFAULT_OUT,
    *,
    allow_multiple: bool = False,
) -> int:
    """
    Update paper_trades.csv from signals_live.csv.

    Returns number of newly opened trades appended.
    """
    out_p = ensure_paper_trades_file(out_csv)

    # ===== PYRAMID telemetry (Sprint-5 DEV2) =====
    try:
        # import tik dėl telemetry / fail-open
        from backtest.risk import pyramiding as _pyr  # noqa: F401
        print("[PYRAMID] executor=paper status=READY reason=BOOT")
    except Exception as _pe:
        print(f"[PYRAMID] executor=paper status=SKIP reason=IMPORT_ERROR err={repr(_pe)}")

    df_sig = _read_signals(in_csv)
    if df_sig.empty:
        # Keep file present; also touch it so dashboards/ops can see the cycle ran.
        try:
            out_p.touch(exist_ok=True)
        except Exception:
            pass
        print("No signals (signals_live.csv missing/empty) -> nothing to open.")
        print("[PYRAMID] executor=paper status=SKIP reason=NO_SIGNALS")
        return 0

    try:
        df_tr = pd.read_csv(out_p, engine="python", on_bad_lines="skip")
    except Exception:
        df_tr = pd.DataFrame(columns=PAPER_COLUMNS)

    if df_tr is None or df_tr.empty:
        df_tr = pd.DataFrame(columns=PAPER_COLUMNS)

    # We open from the latest signal only (snapshot semantics)
    last = df_sig.iloc[-1].to_dict()
    last_symbol = str(last.get("symbol", ""))

    # Consider OPEN rows for the same symbol as active positions
    has_open_decision = evaluate_policy_has_open(
        df_tr,
        symbol=last_symbol,
        allow_multiple=bool(allow_multiple),
    )

    if has_open_decision["block"]:
        # Do not open a duplicate position for the same symbol
        print(f"[PAPER] skip open symbol={last_symbol} reason={has_open_decision['reason']}")
        return 0

    ts = last.get("signal_ts") if "signal_ts" in df_sig.columns else None
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        ts = last.get("timestamp")

    row_out = {
        "timestamp": str(pd.to_datetime(ts, utc=True, errors="coerce")) if ts is not None else "",
        "signal_ts": str(pd.to_datetime(last.get("signal_ts"), utc=True, errors="coerce")) if last.get("signal_ts") is not None else "",
        "symbol": str(last.get("symbol", "")),
        "model": last.get("model", ""),
        "side": last.get("side", ""),
        "entry": last.get("entry", ""),
        "sl": last.get("sl", ""),
        "tp": last.get("tp", ""),
        "rr": last.get("rr", ""),
        "ctx_sub_label": last.get("ctx_sub_label", ""),
        "phase": last.get("phase", ""),
        "regime": last.get("regime", ""),
        "trend_dir": last.get("trend_dir", ""),
        "status": "OPEN",
    }

    df_new = pd.DataFrame([row_out]).reindex(columns=PAPER_COLUMNS)

    # Append (header only if empty)
    write_header = (not out_p.exists()) or out_p.stat().st_size == 0
    df_new.to_csv(out_p, mode="a", header=write_header, index=False)

    return 1


def main(argv: Optional[list[str]] = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", default=str(DEFAULT_SIGNALS))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--allow-multiple", action="store_true")
    args = ap.parse_args(argv)

    run_paper_executor(args.signals, args.out, allow_multiple=bool(args.allow_multiple))


if __name__ == "__main__":
    main()
