"""
SAFE merge + dedupe + gap check for candles CSV

- Merges: existing CSV + downloaded CSV
- Outputs: a NEW merged file (does NOT overwrite unless you set --inplace)
- Checks gaps vs expected interval (e.g. 15m)
- Prints summary and writes gap report

Example:
  python backtest/tools/merge_dedupe_and_check_gaps.py ^
    --existing backtest/data/BTCUSDT_15m.csv ^
    --downloaded backtest/data/history_15m_download/BTCUSDT_15m_downloaded.csv ^
    --interval_m 15 ^
    --out backtest/data/history_15m_merged/BTCUSDT_15m.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _load(p: Path) -> pd.DataFrame:
    df = pd.read_csv(p)
    if "timestamp" not in df.columns:
        raise ValueError(f"{p} missing timestamp")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    # Keep only expected columns if present
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    keep = [c for c in cols if c in df.columns]
    df = df[keep].copy()
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["timestamp"], keep="last")
    return df


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--existing", type=str, required=True)
    p.add_argument("--downloaded", type=str, required=True)
    p.add_argument("--interval_m", type=int, required=True)
    p.add_argument("--out", type=str, required=True)
    args = p.parse_args()

    existing = Path(args.existing)
    downloaded = Path(args.downloaded)
    out = Path(args.out)

    if not existing.exists():
        raise SystemExit(f"existing not found: {existing}")
    if not downloaded.exists():
        raise SystemExit(f"downloaded not found: {downloaded}")

    df1 = _load(existing)
    df2 = _load(downloaded)

    df = pd.concat([df1, df2], ignore_index=True)
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

    start = df["timestamp"].min()
    end = df["timestamp"].max()
    span_days = (end - start).days
    span_years = span_days / 365.25

    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    print(f"[MERGE] out={out}")
    print(f"[MERGE] rows={len(df):,}")
    print(f"[MERGE] start={start} end={end} span_days={span_days} (~{span_years:.2f}y)")

    # Gap check
    expected = pd.date_range(start=start, end=end, freq=f"{int(args.interval_m)}min", tz="UTC")
    have = pd.DatetimeIndex(df["timestamp"])
    missing = expected.difference(have)

    gaps_report = out.parent / f"{out.stem}_gaps_report.csv"
    if len(missing) == 0:
        print("[GAPS] none ✅")
        pd.DataFrame({"missing_timestamp": []}).to_csv(gaps_report, index=False)
        return

    # Summarize missing into runs
    miss = pd.Series(missing).sort_values().reset_index(drop=True)
    step = pd.Timedelta(minutes=int(args.interval_m))
    runs = []
    run_start = miss.iloc[0]
    prev = miss.iloc[0]
    for t in miss.iloc[1:]:
        if t - prev == step:
            prev = t
            continue
        runs.append((run_start, prev))
        run_start = t
        prev = t
    runs.append((run_start, prev))

    rows = []
    for a, b in runs:
        n = int((b - a) / step) + 1
        rows.append({"gap_start": a, "gap_end": b, "missing_bars": n})

    rep = pd.DataFrame(rows).sort_values(["missing_bars", "gap_start"], ascending=[False, True])
    rep.to_csv(gaps_report, index=False)

    print(f"[GAPS] missing_bars_total={len(missing):,} runs={len(rep):,} report={gaps_report}")
    print(rep.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
