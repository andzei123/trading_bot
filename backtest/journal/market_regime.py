from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from backtest.journal.bybit_loader import load_bybit_latest


def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(n).mean()


def build_market_regime(
    candles: pd.DataFrame,
    htf: str = "4h",
    ema_fast: int = 20,
    ema_slow: int = 50,
    atr_win: int = 14,
    trend_min: float = 0.0015,  # ~0.15% skirtumas tarp EMA
    vol_hi: float = 0.012,      # ATR% high
    vol_lo: float = 0.006,      # ATR% low (kept for compatibility)
) -> pd.DataFrame:
    c = candles.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    h = (
        c.set_index("timestamp")[["open", "high", "low", "close"]]
        .resample(htf)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna()
    )

    h["ema_fast"] = h["close"].ewm(span=ema_fast, adjust=False).mean()
    h["ema_slow"] = h["close"].ewm(span=ema_slow, adjust=False).mean()
    h["atr"] = _atr(h.reset_index(), atr_win).values
    h["atr_pct"] = (h["atr"] / h["close"]).replace([np.inf, -np.inf], np.nan)

    # trend strength proxy
    h["ema_gap"] = (h["ema_fast"] - h["ema_slow"]) / h["close"]
    h["trend_dir"] = np.where(h["ema_gap"] > 0, "UP", "DOWN")
    h["trend_strength"] = h["ema_gap"].abs()

    # regime rules
    regime = np.where(
        h["trend_strength"] >= trend_min,
        np.where(h["trend_dir"] == "UP", "TREND_UP", "TREND_DOWN"),
        np.where(h["atr_pct"] >= vol_hi, "VOLATILE", "RANGE"),
    )
    h["regime"] = regime

    out = h.reset_index()[["timestamp", "regime", "trend_dir", "trend_strength", "atr_pct"]].copy()
    return out


def refresh_market_regime_csv(
    symbol: str = "BTCUSDT",
    category: str = "linear",
    interval: str = "15",
    candles: int = 20000,
    out_path: str = "backtest/journal/exports_trades/market_regime.csv",
    keep_days: int = 180,
    htf: str = "4h",
) -> pd.DataFrame:
    """
    Refresh market_regime.csv from fresh Bybit candles.

    Flow:
    - fetch latest candles from Bybit
    - build regime with existing build_market_regime()
    - append to existing CSV if present
    - drop duplicate timestamps (keep latest)
    - trim to rolling keep_days window
    - save back to out_path
    """
    outp = Path(out_path)
    outp.parent.mkdir(parents=True, exist_ok=True)

    candles_df = load_bybit_latest(
        category=category,
        symbol=symbol,
        interval=interval,
        candles=int(candles),
    )

    if candles_df is None or candles_df.empty:
        raise SystemExit("[MR REFRESH] No candles loaded from Bybit")

    need = {"timestamp", "open", "high", "low", "close"}
    miss = need - set(candles_df.columns)
    if miss:
        raise SystemExit(f"[MR REFRESH] Missing columns in fetched candles: {miss}")

    df_new = build_market_regime(candles_df, htf=htf)
    df_new["timestamp"] = pd.to_datetime(df_new["timestamp"], utc=True, errors="coerce")
    df_new = df_new.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    if outp.exists():
        df_old = pd.read_csv(outp, engine="python", on_bad_lines="skip")
        if "timestamp" in df_old.columns:
            df_old["timestamp"] = pd.to_datetime(df_old["timestamp"], utc=True, errors="coerce")
            df_old = df_old.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_all = df_new.copy()
    else:
        df_all = df_new.copy()

    # normalize
    if "regime" in df_all.columns:
        df_all["regime"] = df_all["regime"].astype(str).str.upper()
    if "trend_dir" in df_all.columns:
        df_all["trend_dir"] = df_all["trend_dir"].astype(str).str.upper()
    if "trend_strength" in df_all.columns:
        df_all["trend_strength"] = pd.to_numeric(df_all["trend_strength"], errors="coerce").fillna(0.0)
    if "atr_pct" in df_all.columns:
        df_all["atr_pct"] = pd.to_numeric(df_all["atr_pct"], errors="coerce").fillna(0.0)

    # dedupe by timestamp
    df_all = (
        df_all.sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"], keep="last")
        .reset_index(drop=True)
    )

    # rolling trim
    if int(keep_days) > 0 and not df_all.empty:
        cutoff = df_all["timestamp"].max() - pd.Timedelta(days=int(keep_days))
        df_all = df_all[df_all["timestamp"] >= cutoff].copy()
        df_all = df_all.sort_values("timestamp").reset_index(drop=True)

    df_all.to_csv(outp, index=False)

    print(
        f"[MR REFRESH] symbol={symbol} interval={interval} htf={htf} "
        f"rows={len(df_all)} "
        f"from={df_all['timestamp'].min()} "
        f"to={df_all['timestamp'].max()}"
    )
    try:
        print("[MR REFRESH] regime_count:", df_all["regime"].value_counts(dropna=False).to_dict())
    except Exception:
        pass

    return df_all


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", type=str, default="backtest/journal/candles_ohlc.csv")
    ap.add_argument("--out", dest="out", type=str, default="backtest/journal/exports_trades/market_regime.csv")
    ap.add_argument("--htf", type=str, default="4h")

    # new refresh mode
    ap.add_argument("--refresh", action="store_true", help="Refresh market_regime.csv from fresh Bybit candles")
    ap.add_argument("--symbol", type=str, default="BTCUSDT")
    ap.add_argument("--bybit_category", type=str, default="linear")
    ap.add_argument("--bybit_interval", type=str, default="15")
    ap.add_argument("--candles", type=int, default=20000)
    ap.add_argument("--keep_days", type=int, default=180)

    args = ap.parse_args()

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    if args.refresh:
        refresh_market_regime_csv(
            symbol=args.symbol,
            category=args.bybit_category,
            interval=args.bybit_interval,
            candles=args.candles,
            out_path=args.out,
            keep_days=args.keep_days,
            htf=args.htf,
        )
        return

    inp = Path(args.inp)
    candles = pd.read_csv(inp, engine="python", on_bad_lines="skip")
    need = {"timestamp", "open", "high", "low", "close"}
    miss = need - set(candles.columns)
    if miss:
        raise SystemExit(f"Missing columns in candles: {miss}")

    mr = build_market_regime(candles, htf=args.htf)
    mr.to_csv(outp, index=False)

    print(f"Rows: {len(mr)} | HTF={args.htf}")
    print("Saved:", outp.resolve())


if __name__ == "__main__":
    main()