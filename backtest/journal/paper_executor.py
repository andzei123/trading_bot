from __future__ import annotations

import argparse
import os
from pathlib import Path
import pandas as pd

DEFAULT_SIGNALS = Path("backtest/journal/exports_live/signals_live.csv")
DEFAULT_OUT = Path("backtest/journal/exports_live/paper_trades.csv")
DEFAULT_STATE = Path("backtest/journal/exports_live/paper_state.json")


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def load_signals(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return df
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df.sort_values("timestamp").reset_index(drop=True)


def load_paper_trades(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=[
            "timestamp", "model", "side", "entry", "sl", "tp",
            "ctx_sub_label", "phase", "regime", "trend_dir",
            "status"
        ])
    df = pd.read_csv(path)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def has_open_position(paper_df: pd.DataFrame) -> bool:
    if paper_df.empty:
        return False
    # status == OPEN reiškia, kad yra aktyvi pozicija
    return (paper_df["status"].astype(str).str.upper() == "OPEN").any()


def append_trade(out_path: Path, trade_row: dict) -> None:
    _ensure_parent(out_path)
    df = load_paper_trades(out_path)
    df = pd.concat([df, pd.DataFrame([trade_row])], ignore_index=True)
    df.to_csv(out_path, index=False)


def run_once(signals_csv: Path, out_csv: Path, allow_multiple: bool = False) -> None:
    sig = load_signals(signals_csv)
    if sig.empty:
        print("No signals file / empty signals.")
        return

    paper = load_paper_trades(out_csv)

    # paimam paskutinį signalą
    last_sig = sig.iloc[-1].to_dict()

    # dedupe: jei tas pats timestamp+side+model jau yra paper_trades -> skip
    if not paper.empty:
        exists = (
            (paper["timestamp"] == pd.to_datetime(last_sig.get("timestamp"))) &
            (paper["side"].astype(str) == str(last_sig.get("side"))) &
            (paper["model"].astype(str) == str(last_sig.get("model")))
        )
        if exists.any():
            print("Latest signal already processed -> skip.")
            return

    if (not allow_multiple) and has_open_position(paper):
        print("OPEN position exists -> skip new entry (set --allow-multiple to override).")
        return

    # suformuojam paper trade
    trade = {
        "timestamp": pd.to_datetime(last_sig.get("timestamp")),
        "model": last_sig.get("model", ""),
        "side": last_sig.get("side", ""),
        "entry": float(last_sig.get("entry", 0.0)),
        "sl": float(last_sig.get("sl", 0.0)),
        "tp": float(last_sig.get("tp", 0.0)),
        "ctx_sub_label": last_sig.get("ctx_sub_label", ""),
        "phase": last_sig.get("phase", ""),
        "regime": last_sig.get("regime", ""),
        "trend_dir": last_sig.get("trend_dir", ""),
        "status": "OPEN",
    }

    append_trade(out_csv, trade)
    print("PAPER TRADE OPENED:")
    print(trade)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", default=str(DEFAULT_SIGNALS))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--allow-multiple", action="store_true")
    args = ap.parse_args()

    run_once(Path(args.signals), Path(args.out), allow_multiple=args.allow_multiple)


if __name__ == "__main__":
    main()
