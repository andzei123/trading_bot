# backtest/journal/market_regime.py
from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd

def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low).abs(),
         (high - prev_close).abs(),
         (low - prev_close).abs()],
        axis=1
    ).max(axis=1)
    return tr.rolling(n).mean()

def build_market_regime(
    candles: pd.DataFrame,
    htf: str = "4h",
    ema_fast: int = 20,
    ema_slow: int = 50,
    atr_win: int = 14,
    trend_min: float = 0.0015,     # ~0.15% skirtumas tarp EMA (tunable)
    vol_hi: float = 0.012,         # ATR% high (tunable)
    vol_lo: float = 0.006,         # ATR% low  (tunable)
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

    # regime rules (paprastos, bet stabilios)
    # 1) jei trend_strength didelis -> TREND_UP / TREND_DOWN
    # 2) jei trend_strength mažas -> RANGE (jei vol low/med) arba VOLATILE (jei vol high)
    regime = np.where(
        h["trend_strength"] >= trend_min,
        np.where(h["trend_dir"] == "UP", "TREND_UP", "TREND_DOWN"),
        np.where(h["atr_pct"] >= vol_hi, "VOLATILE", "RANGE")
    )
    h["regime"] = regime

    out = h.reset_index()[["timestamp", "regime", "trend_dir", "trend_strength", "atr_pct"]].copy()
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", type=str, default="backtest/journal/candles_ohlc.csv")
    ap.add_argument("--out", dest="out", type=str, default="backtest/journal/exports_trades/market_regime.csv")
    ap.add_argument("--htf", type=str, default="4h")
    args = ap.parse_args()

    inp = Path(args.inp)
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    candles = pd.read_csv(inp, engine="python", on_bad_lines="skip")
    need = {"timestamp","open","high","low","close"}
    miss = need - set(candles.columns)
    if miss:
        raise SystemExit(f"Missing columns in candles: {miss}")

    mr = build_market_regime(candles, htf=args.htf)
    mr.to_csv(outp, index=False)

    print(f"Rows: {len(mr)} | HTF={args.htf}")
    print("Saved:", outp.resolve())

if __name__ == "__main__":
    main()
