from __future__ import annotations

from pathlib import Path
import sys
import time
import pandas as pd

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
JOURNAL_DIR = THIS_FILE.parent
ENGINE_DIR = PROJECT_ROOT / "backtest" / "engine"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(JOURNAL_DIR))
sys.path.insert(0, str(ENGINE_DIR))

import filter_trades as ft
from backtest.engine import entry_model as em


def _pick_nonempty_paramset(df: pd.DataFrame) -> dict:
    # Paimam geriausią iš grid_results_expectancy.csv, jeigu yra
    candidates = df.sort_values(["expectancy_R", "trades_total"], ascending=[False, False])
    return candidates.iloc[0].to_dict()


def main():
    # 1) Load + build caches
    candles, _ = ft.load_inputs()
    ctx = ft.label_tts_tdp(candles)
    candle_cache = em.build_candle_cache(candles)

    print(f"Candles: {len(candles)} | CTX: {len(ctx)} | Cache ts: {len(candle_cache.ts)}")
    assert len(candles) == len(ctx) == len(candle_cache.ts), "Len mismatch: candles/ctx/cache must match"

    # 2) Pasiimam vieną parametrų rinkinį (geriausia iš tavo grid exporto)
    grid_path = ft.EXPORT_DIR / "grid_results_expectancy.csv"
    if not grid_path.exists():
        raise FileNotFoundError(f"Missing: {grid_path} (pirmiau paleisk grid_search_expectancy.py)")

    grid = pd.read_csv(grid_path)
    params = _pick_nonempty_paramset(grid)

    # Normalizuojam tipų, nes CSV gali paversti į float/object
    params["RR"] = float(params["RR"])
    params["SL_ATR_BUFFER"] = float(params["SL_ATR_BUFFER"])
    params["TDP_DEV_LOOKBACK"] = int(params["TDP_DEV_LOOKBACK"])
    params["REQUIRE_IMPULSE_BEFORE_TDP"] = bool(params["REQUIRE_IMPULSE_BEFORE_TDP"])
    params["IMPULSE_LOOKBACK"] = int(params["IMPULSE_LOOKBACK"])
    params["IMPULSE_SIZE_ATR"] = float(params["IMPULSE_SIZE_ATR"])
    params["TTS_RETEST_LOOKBACK"] = int(params["TTS_RETEST_LOOKBACK"])
    params["MAX_HOLD_BARS"] = int(params["MAX_HOLD_BARS"])
    params["BE_AFTER_R"] = float(params["BE_AFTER_R"])
    params["PARTIAL_AT_R"] = float(params["PARTIAL_AT_R"])
    params["PARTIAL_FRAC"] = float(params["PARTIAL_FRAC"])

    print("Using paramset:", {k: params[k] for k in [
        "RR", "SL_ATR_BUFFER", "TDP_DEV_LOOKBACK", "IMPULSE_LOOKBACK", "IMPULSE_SIZE_ATR"
    ]})

    # 3) Entries
    entries = em.generate_entries_from_ctx(
        ctx,
        rr=params["RR"],
        sl_atr_buffer=params["SL_ATR_BUFFER"],
        tdp_dev_lookback=params["TDP_DEV_LOOKBACK"],
        require_impulse_before_tdp=params["REQUIRE_IMPULSE_BEFORE_TDP"],
        impulse_lookback=params["IMPULSE_LOOKBACK"],
        impulse_size_atr=params["IMPULSE_SIZE_ATR"],
        tts_retest_lookback=params["TTS_RETEST_LOOKBACK"],
    )
    print("Entries:", len(entries))
    assert len(entries) > 0, "No entries generated -> something off in ctx/entry logic"

    # 4) Simulate WITH cache
    t0 = time.perf_counter()
    sim_cache = em.simulate_trades(
        candles=pd.DataFrame(),  # ignored when candle_cache passed
        entries=entries,
        max_hold_bars=params["MAX_HOLD_BARS"],
        be_after_r=params["BE_AFTER_R"],
        partial_at_r=params["PARTIAL_AT_R"],
        partial_frac=params["PARTIAL_FRAC"],
        candle_cache=candle_cache,
    )
    dt_cache = (time.perf_counter() - t0) * 1000.0

    print(f"Sim (cache): {len(sim_cache)} trades | {dt_cache:.1f} ms")
    assert "R" in sim_cache.columns, "simulate_trades output missing 'R' column"
    assert len(sim_cache) > 0, "No trades simulated -> check simulate_trades"

    # 5) Simulate WITHOUT cache (control)
    t1 = time.perf_counter()
    sim_raw = em.simulate_trades(
        candles=candles,  # now candles used
        entries=entries,
        max_hold_bars=params["MAX_HOLD_BARS"],
        be_after_r=params["BE_AFTER_R"],
        partial_at_r=params["PARTIAL_AT_R"],
        partial_frac=params["PARTIAL_FRAC"],
        candle_cache=None,
    )
    dt_raw = (time.perf_counter() - t1) * 1000.0

    print(f"Sim (raw):   {len(sim_raw)} trades | {dt_raw:.1f} ms")

    # 6) Sanity: rezultatai turi sutapti (bent jau trade count). Jei nesutaps — iškart žinosim kur skirtumas.
    if len(sim_cache) != len(sim_raw):
        print("WARNING: trade counts differ cache vs raw!")
        print("cache:", len(sim_cache), "raw:", len(sim_raw))
    else:
        print("OK: trade counts match (cache vs raw)")

    # Minimal check: mean R should be very close
    r_cache = pd.to_numeric(sim_cache["R"], errors="coerce").dropna()
    r_raw = pd.to_numeric(sim_raw["R"], errors="coerce").dropna()
    if not r_cache.empty and not r_raw.empty:
        diff = abs(r_cache.mean() - r_raw.mean())
        print(f"Mean R cache={r_cache.mean():.6f} raw={r_raw.mean():.6f} | diff={diff:.6g}")
        if diff > 1e-9:
            print("NOTE: Mean R differs slightly (could be rounding/order). If big diff -> bug.")
    else:
        print("NOTE: R series empty after numeric coercion")

    print("DONE.")


if __name__ == "__main__":
    main()
