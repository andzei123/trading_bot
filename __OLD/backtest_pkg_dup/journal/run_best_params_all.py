from __future__ import annotations

import json
from pathlib import Path
import sys

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
JOURNAL_DIR = THIS_FILE.parent
ENGINE_DIR = PROJECT_ROOT / "backtest" / "engine"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(JOURNAL_DIR))
sys.path.insert(0, str(ENGINE_DIR))


import filter_trades as ft
from backtest.engine import entry_model as em
import pandas as pd

def load_market_regime(path="backtest/journal/exports_trades/market_regime.csv") -> pd.DataFrame:
    mr = pd.read_csv(path, engine="python", on_bad_lines="skip")
    mr["timestamp"] = pd.to_datetime(mr["timestamp"], errors="coerce")
    mr = mr.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return mr

def attach_regime_to_rows(df: pd.DataFrame, mr: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce")
    d = d.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    out = pd.merge_asof(
        d,
        mr[["timestamp","regime","trend_dir","trend_strength","atr_pct"]].sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )
    return out

def pass_smart_gate(row, trend_min: float = 0.0010) -> bool:
    ctx = str(row.get("ctx_sub_label","")).upper()
    side = str(row.get("side","")).upper()
    trend_dir = str(row.get("trend_dir","")).upper()
    ts = row.get("trend_strength", 0.0)
    try:
        ts = float(ts) if ts is not None else 0.0
    except:
        ts = 0.0

    # tavo geras gate:
    if ctx == "TDP_BOT" and side == "LONG" and trend_dir == "DOWN":
        # optional: tik jei tikrai trendas pakankamai stiprus
        if ts >= trend_min:
            return False
        # jei nenori trend_min, tada tiesiog return False be if
        return False

    return True


def load_best_params() -> dict:
    p = ft.EXPORT_DIR / "best_params.json"
    if not p.exists():
        raise SystemExit(f"Missing: {p} (run pick_best_from_wf.py first)")
    raw = json.loads(p.read_text(encoding="utf-8"))

    # convert numpy-y types if they got serialized weirdly
    out = {}
    for k, v in raw.items():
        if isinstance(v, str):
            out[k] = v
        elif isinstance(v, bool) or v is None:
            out[k] = v
        else:
            # numbers
            try:
                out[k] = float(v) if (k in ["RR","SL_ATR_BUFFER","IMPULSE_SIZE_ATR","BE_AFTER_R","PARTIAL_AT_R","PARTIAL_FRAC"]) else int(v)
            except Exception:
                out[k] = v
    return out


def main():
    params = load_best_params()
    mr = load_market_regime()
    candles, _ = ft.load_inputs()
    candles = candles.copy()
    candles["timestamp"] = pd.to_datetime(candles["timestamp"])

    print(f"ALL candles: {len(candles)} | {candles['timestamp'].min()} -> {candles['timestamp'].max()}")
    print("Using best params:", params)

    ctx = ft.build_ctx(candles)

    ctx = ft.merge_market_regime(ctx)  # <-- kad gate turėtų trend_dir

    cache = em.build_candle_cache(candles)

    entries = em.generate_entries_from_ctx(
        ctx,
        rr=float(params["RR"]),
        sl_atr_buffer=float(params["SL_ATR_BUFFER"]),
        tdp_dev_lookback=int(params["TDP_DEV_LOOKBACK"]),
        require_impulse_before_tdp=bool(params["REQUIRE_IMPULSE_BEFORE_TDP"]),
        impulse_lookback=int(params["IMPULSE_LOOKBACK"]),
        impulse_size_atr=float(params["IMPULSE_SIZE_ATR"]),
        tts_retest_lookback=int(params["TTS_RETEST_LOOKBACK"]),
    )

    sim = em.simulate_trades(
        candles=pd.DataFrame(),  # unused when candle_cache is passed
        entries=entries,
        max_hold_bars=int(params["MAX_HOLD_BARS"]),
        be_after_r=float(params["BE_AFTER_R"]),
        partial_at_r=float(params["PARTIAL_AT_R"]),
        partial_frac=float(params["PARTIAL_FRAC"]),
        candle_cache=cache,
    )

    # ---- attach market regime to trades (so CSV shows trend_dir/regime) ----
    sim = ft.merge_market_regime(sim)


    out_csv = ft.EXPORT_DIR / "best_trades_all.csv"
    sim.to_csv(out_csv, index=False)
    print("Saved:", out_csv)
    print("Trades:", len(sim))


if __name__ == "__main__":
    main()
