#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import csv
import datetime as dt
import time
from pathlib import Path
import numpy as np
import pandas as pd

def _utcnow_z():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _file_last_ts_and_rows(p: Path) -> tuple[int, str | None]:
    """Return (rows, last_ts) for a macro CSV (best-effort, no pandas)."""
    rows = 0
    last_ts: str | None = None
    try:
        with p.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None) or []
            # Try to locate a timestamp-like column
            ts_idx = None
            for cand in ("timestamp", "time", "date", "ts"):
                if cand in [h.strip().lower() for h in header]:
                    ts_idx = [h.strip().lower() for h in header].index(cand)
                    break
            for row in reader:
                if not row:
                    continue
                rows += 1
                if ts_idx is not None and ts_idx < len(row):
                    v = str(row[ts_idx]).strip()
                    if v:
                        last_ts = v
    except Exception:
        return 0, None
    return rows, last_ts


def write_meta(macro_dir: str | Path) -> Path:
    """Write data/macro/_meta.json for MacroGate freshness checks."""
    macro_dir = Path(macro_dir)
    macro_dir.mkdir(parents=True, exist_ok=True)
    meta: dict = {"generated_at_utc": _utcnow_z(), "files": {}}
    for p in sorted(macro_dir.glob("*.csv")):
        rows, last_ts = _file_last_ts_and_rows(p)
        meta["files"][p.name] = {"rows": int(rows), "last_ts": last_ts}
    out = macro_dir / "_meta.json"
    out.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


REQUIRED = [
    ("BTC", 48000.0, 1),
    ("ETH", 2600.0, 7),
    ("BTC.D", 52.0, 2),
    ("TOTAL2", 1200.0, 3),
    ("TOTAL3", 900.0, 8),
    ("ETHBTC", 0.055, 4),
    ("USDT.D", 5.5, 5),
    ("USDC.D", 4.2, 6),
    ("DXY", 105.0, 9),
]

MULTI_TF = ["1D", "1W"]

EXTRA_RUNTIME = [
    ("DXY", "4h", 105.0, 29),
    ("BTC.D", "4h", 52.0, 22),
    ("TOTAL3", "4h", 900.0, 23),
    ("ETH", "4h", 2600.0, 30),
    ("ETHBTC", "4h", 0.055, 24),
    ("TOTAL2", "1d", 1200.0, 25),
    ("ETHBTC", "1d", 0.055, 28),
    ("USDT.D", "1d", 5.5, 26),
    ("USDC.D", "1d", 4.2, 27),
]


def _tf_to_freq(tf: str) -> str:
    s = str(tf).strip().lower()
    if s.endswith("h"):
        return f"{int(s[:-1])}h"
    if s.endswith("d"):
        return f"{int(s[:-1])}D"
    if s.endswith("w"):
        return f"{int(s[:-1])}W"
    raise ValueError(tf)


def gen_series(rows, tf, end_dt, start_price, seed):
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0003, 0.015, rows)
    prices = float(start_price) * np.exp(np.cumsum(rets))
    idx = pd.date_range(end=end_dt, periods=rows, freq=_tf_to_freq(tf))
    return pd.DataFrame(
        {"timestamp": idx.strftime("%Y-%m-%d %H:%M:%S"), "close": prices}
    )


def write_csv(out_dir, name, tf, df):
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"{name}_{tf}.csv"
    df.to_csv(p, index=False)
    return p


def write_status(out_dir, name, tf):
    p = out_dir / f"{name}_{tf}.status"
    p.write_text("OK\n")
    return p


def run_feeder(args):
    macro_dir = Path(args.macro_dir)
    # "also_data_root" is treated as the project's data root.
    # Mirror output into <also_data_root>/macro so callers can pass e.g. --also_data_root data
    # without polluting the root with macro files.
    also_root = (Path(args.also_data_root) / "macro") if str(args.also_data_root).strip() else None

    today = dt.datetime.now(dt.UTC).date()
    end_1d = dt.datetime.combine(today - dt.timedelta(days=1), dt.time(), tzinfo=dt.UTC)
    end_4h = dt.datetime.combine(today, dt.time(), tzinfo=dt.UTC)

    rows_1d = max(400, int(args.rows))
    rows_1w = max(200, rows_1d // 3)

    for name, sp, seed in REQUIRED:
        for tf in MULTI_TF:
            rows = rows_1d if tf == "1D" else rows_1w
            df = gen_series(rows, tf, end_1d, sp, seed)
            p = write_csv(macro_dir, name, tf, df)
            if tf == "1D":
                write_status(macro_dir, name, tf)
            if also_root:
                write_csv(also_root, name, tf, df)
            print(f"[FEED] wrote {p.name} rows={len(df)} last_ts={df.timestamp.iloc[-1]}")

    if args.include_extra:
        for name, tf, sp, seed in EXTRA_RUNTIME:
            end_dt = end_4h if tf.endswith("h") else end_1d
            rows = max(rows_1d, 900) if tf.endswith("h") else rows_1d
            df = gen_series(rows, tf, end_dt, sp, seed)
            for out in [macro_dir] + ([also_root] if also_root else []):
                p = write_csv(out, name, tf, df)
            print(f"[FEED] wrote {name}_{tf}.csv rows={len(df)} last_ts={df.timestamp.iloc[-1]}")

    # write freshness meta for MacroGate
    try:
        mp = write_meta(macro_dir)
        print(f"[FEED] wrote {mp.name}")
    except Exception as e:
        print(f"[FEED][WARN] meta write failed: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--macro_dir", default="data/macro")
    ap.add_argument("--also_data_root", default="")
    ap.add_argument("--rows", type=int, default=750)
    ap.add_argument("--include_extra", action="store_true")
    ap.add_argument("--loop", type=int, default=0, help="Run feeder every N seconds (daemon mode)")
    args = ap.parse_args()

    if args.loop <= 0:
        run_feeder(args)
        return

    print(f"[FEED] starting daemon loop interval={args.loop}s")
    while True:
        try:
            run_feeder(args)
        except Exception as e:
            print("[FEED] error:", e)
        time.sleep(int(args.loop))


if __name__ == "__main__":
    main()