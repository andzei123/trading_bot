"""
SAFE Bybit kline downloader (public, no API keys needed)

- Downloads N years of klines per symbol (default 5y)
- Resumable: writes a checkpoint JSON in out_dir
- Writes downloaded raw CSV to out_dir (does NOT overwrite your existing CSV)
- Uses backward pagination with `end` parameter
- Rate-limit safe backoff

Requires:
  pip install pybit pandas

Example:
  python backtest/tools/bybit_download_klines_5y.py ^
    --symbols BTCUSDT,ETHUSDT,XRPUSDT ^
    --interval 15 ^
    --years 5 ^
    --category linear ^
    --out_dir backtest/data/history_15m_download

Then merge with your existing CSV using merge script (below).
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
from datetime import datetime, timezone, timedelta

from pybit.unified_trading import HTTP


def utc_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_checkpoint(p: Path) -> Dict[str, Any]:
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_checkpoint(p: Path, data: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def fetch_page(session: HTTP, category: str, symbol: str, interval: int, limit: int, end_ms: Optional[int]) -> List[List[str]]:
    # Bybit unified trading: get_kline(category, symbol, interval, limit, end)
    kwargs = dict(category=category, symbol=symbol, interval=interval, limit=limit)
    if end_ms is not None:
        kwargs["end"] = end_ms
    res = session.get_kline(**kwargs)
    # Expected: res["result"]["list"] is list of rows as strings
    return res.get("result", {}).get("list", []) or []


def normalize_rows(rows: List[List[str]]) -> pd.DataFrame:
    # Bybit row: [timestamp, open, high, low, close, volume, turnover]
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype("int64"), unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
    return df[["timestamp", "open", "high", "low", "close", "volume"]]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", type=str, required=True, help="Comma-separated, e.g. BTCUSDT,ETHUSDT,XRPUSDT")
    p.add_argument("--interval", type=int, default=15, help="Bybit interval in minutes, e.g. 15, 30, 60")
    p.add_argument("--years", type=int, default=5, help="How many years back to download")
    p.add_argument("--category", type=str, default="linear", help="Bybit category: linear/spot/inverse (you use linear)")
    p.add_argument("--limit", type=int, default=1000, help="Max per request (Bybit limit)")
    p.add_argument("--sleep_s", type=float, default=0.12, help="Base sleep between requests (tune if rate-limited)")
    p.add_argument("--out_dir", type=str, required=True, help="Output folder for downloaded CSVs + checkpoints")
    args = p.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("No symbols")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    session = HTTP(testnet=False)

    # Desired start timestamp (UTC)
    start_dt = now_utc() - timedelta(days=int(args.years * 365.25))
    start_ms = utc_ms(start_dt)

    print(f"[BOOT] symbols={symbols} interval={args.interval}m years={args.years} start={start_dt.isoformat()} out_dir={out_dir}")

    for sym in symbols:
        ckpt_path = out_dir / f"{sym}_{args.interval}m_checkpoint.json"
        out_csv = out_dir / f"{sym}_{args.interval}m_downloaded.csv"

        ckpt = load_checkpoint(ckpt_path)
        end_ms = ckpt.get("end_ms")  # download backwards
        done = bool(ckpt.get("done", False))
        total_rows = int(ckpt.get("total_rows", 0))

        if done and out_csv.exists():
            print(f"[{sym}] already done -> {out_csv}")
            continue

        # We'll append in-memory chunks and flush periodically
        chunks: List[pd.DataFrame] = []
        flush_every_pages = 25

        pages = 0
        backoff = 1.0

        while True:
            try:
                rows = fetch_page(session, args.category, sym, args.interval, args.limit, end_ms)
            except Exception as e:
                # rate-limit/backoff
                print(f"[{sym}] WARN fetch error: {e} -> sleep {backoff:.1f}s")
                time.sleep(backoff)
                backoff = min(60.0, backoff * 1.8)
                continue

            backoff = 1.0

            if not rows:
                print(f"[{sym}] no more rows returned. stopping.")
                done = True
                break

            df = normalize_rows(rows)
            if df.empty:
                print(f"[{sym}] empty normalized page. stopping.")
                done = True
                break

            chunks.append(df)
            pages += 1
            total_rows += len(df)

            # Set next end_ms = oldest candle timestamp - 1ms (because we paginate backwards)
            oldest_ts = int(df["timestamp"].min().timestamp() * 1000)
            end_ms = oldest_ts - 1

            # Progress
            newest = df["timestamp"].max()
            oldest = df["timestamp"].min()
            print(f"[{sym}] page={pages} rows+={len(df)} total~={total_rows} window {oldest} -> {newest}")

            # Stop condition: we reached start_dt (oldest <= start_dt)
            if oldest_ts <= start_ms:
                done = True
                break

            # Periodic flush to disk (safe + resumable)
            if pages % flush_every_pages == 0:
                _flush(out_csv, chunks)
                chunks = []

                save_checkpoint(ckpt_path, {
                    "symbol": sym,
                    "interval_m": args.interval,
                    "end_ms": end_ms,
                    "total_rows": total_rows,
                    "done": False,
                    "updated_utc": now_utc().isoformat(),
                })

            time.sleep(max(0.0, float(args.sleep_s)))

        # Final flush
        _flush(out_csv, chunks)
        chunks = []

        save_checkpoint(ckpt_path, {
            "symbol": sym,
            "interval_m": args.interval,
            "end_ms": end_ms,
            "total_rows": total_rows,
            "done": bool(done),
            "updated_utc": now_utc().isoformat(),
        })

        # After download, we keep only the required window (>= start_dt) just in case
        try:
            df_all = pd.read_csv(out_csv)
            df_all["timestamp"] = pd.to_datetime(df_all["timestamp"], utc=True, errors="coerce")
            df_all = df_all.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
            df_all = df_all[df_all["timestamp"] >= pd.to_datetime(start_dt)]
            df_all.to_csv(out_csv, index=False)
        except Exception as e:
            print(f"[{sym}] WARN post-trim failed: {e}")

        print(f"[{sym}] DONE -> {out_csv}")

    print("ALL DONE.")


def _flush(out_csv: Path, chunks: List[pd.DataFrame]) -> None:
    if not chunks:
        return
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    df = pd.concat(chunks, ignore_index=True)
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")

    if out_csv.exists():
        try:
            old = pd.read_csv(out_csv)
            old["timestamp"] = pd.to_datetime(old["timestamp"], utc=True, errors="coerce")
            old = old.dropna(subset=["timestamp"])
            df = pd.concat([old, df], ignore_index=True)
            df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
        except Exception:
            pass

    df.to_csv(out_csv, index=False)


if __name__ == "__main__":
    main()
