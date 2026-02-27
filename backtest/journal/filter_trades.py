from __future__ import annotations

import json
import time
import random
import os
from pathlib import Path
from typing import Tuple, Optional

import pandas as pd
import numpy as np
import requests

# ============ PATHS ============
CANDLES_PATH = Path("backtest/journal/candles_ohlc.csv")
TRADES_PATH  = Path("backtest/journal/trades.csv")

EXPORT_DIR = Path("backtest/journal/exports_trades")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# ============ TUNING ============
HTF = "4h"   # lowercase -> no FutureWarning
CTX_WIN = 80
ATR_WIN = 14

# TTS
IMPULSE_ATR_MIN = 1.0
RANGE_ATR_MAX   = 10.0
DEV_MAX         = 2
BREAKOUT_ATR_MIN = 0.2

# TDP
EXTREME_Q = 0.2
DEV_MIN_TDP = 4
TDP_RANGE_ATR_MAX = 14.0
TDP_IMPULSE_MIN = 1.0

EMA_FAST = 20
EMA_SLOW = 50

# FILTER FLAGS
REQUIRE_TREND_FOR_TTS = True
REQUIRE_TREND_FOR_TDP = False
REQUIRE_EXTREME_FOR_TDP = True


# ========= JSON I/O (fix diagnose_best_params.py error) =========
def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: Path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=float)


def _to_dt(df: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    # Greita ir stabili (grid search metu labai svarbu)
    df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d %H:%M:%S", cache=True)
    return df.dropna(subset=[col])


# ============================================================
# ✅ Bybit loader built-in (for load_inputs(source="bybit"))
# ============================================================

BYBIT_REST = "https://api.bybit.com"

# ============================================================
# A2: Rate-limit backoff helpers (Bybit retCode 10006)
# ============================================================

class BybitRateLimitError(RuntimeError):
    pass


def _is_bybit_rate_limit_payload(j: dict) -> bool:
    try:
        code = int(j.get("retCode", -1))
    except Exception:
        code = -1
    msg = str(j.get("retMsg", "")).lower()
    return (code == 10006) or ("too many visits" in msg) or ("rate limit" in msg)


def _bybit_get_kline(
    category: str,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Bybit v5 Get Kline:
      GET /v5/market/kline?category=linear&symbol=BTCUSDT&interval=30&start=..&end=..&limit=..
    Returns list rows: [startTime, open, high, low, close, volume, turnover]
    """
    url = f"{BYBIT_REST}/v5/market/kline"
    params = {
        "category": category,
        "symbol": symbol,
        "interval": str(interval),
        "start": int(start_ms),
        "end": int(end_ms),
        "limit": int(limit),
    }

    sleep_s = 10
    while True:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        j = r.json()

        if _is_bybit_rate_limit_payload(j):
            jitter = random.uniform(0.0, 1.0)
            print(f"[BYBIT] 10006 rate limit -> sleep {sleep_s:.0f}s")
            time.sleep(min(300, sleep_s) + jitter)
            sleep_s = min(300, sleep_s * 2)
            continue

        if j.get("retCode") != 0:
            raise RuntimeError(f"Bybit retCode={j.get('retCode')} retMsg={j.get('retMsg')}")

        lst = (((j.get("result") or {}).get("list")) or [])
        if not lst:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        rows = []
        for it in lst:
            rows.append(
                {
                    "timestamp": pd.to_datetime(int(it[0]), unit="ms", utc=True),
                    "open": float(it[1]),
                    "high": float(it[2]),
                    "low": float(it[3]),
                    "close": float(it[4]),
                    "volume": float(it[5]),
                }
            )
        return pd.DataFrame(rows)


def _load_candles_bybit(
    category: str = "linear",
    symbol: str = "BTCUSDT",
    interval: str = "30",
    candles: int = 3000,
) -> pd.DataFrame:
    """
    Fetch ~N latest candles by paging backwards (Bybit max 1000 per call).
    Returns tz-naive timestamp (datetime64[ns]) for merge_asof.
    """
    candles = int(candles)
    if candles <= 0:
        raise ValueError("bybit_candles must be > 0")

    end = int(pd.Timestamp.utcnow().timestamp() * 1000)
    out = []
    remaining = candles

    while remaining > 0:
        take = min(1000, remaining)

        # wide window; Bybit will still cap by 'limit'
        if str(interval).isdigit():
            bar_ms = int(interval) * 60_000
            start = end - take * bar_ms * 2
        else:
            start = end - 365 * 24 * 60 * 60_000

        df = _bybit_get_kline(
            category=category,
            symbol=symbol,
            interval=str(interval),
            start_ms=start,
            end_ms=end,
            limit=take,
        )
        if df.empty:
            break

        out.append(df)

        oldest = df["timestamp"].min()
        end = int(oldest.value // 1_000_000) - 1
        remaining -= len(df)

        if len(df) < take:
            break

    if not out:
        raise RuntimeError("Bybit returned no candles (check symbol/category/interval).")

    candles_df = pd.concat(out, ignore_index=True)
    candles_df = candles_df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # keep last N
    if len(candles_df) > candles:
        candles_df = candles_df.iloc[-candles:].reset_index(drop=True)

    # IMPORTANT: normalize timestamps -> tz-naive
    candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"], utc=True, errors="coerce")
    candles_df = candles_df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    return candles_df


# ============================================================
# ✅ load_inputs(source=...)
# ============================================================

def load_inputs(
    source: str = "csv",
    bybit_category: str = "linear",
    bybit_symbol: str = "BTCUSDT",
    bybit_interval: int = 30,
    bybit_candles: int = 3000,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Returns:
      candles: DataFrame with columns at least timestamp, open, high, low, close, volume
      trades : DataFrame or None (bybit mode)

    timestamp is tz-naive (datetime64[ns]) for merge_asof.
    """
    source = str(source).lower().strip()

    if source == "bybit":
        candles = _load_candles_bybit(
            category=bybit_category,
            symbol=bybit_symbol,
            interval=str(bybit_interval),
            candles=int(bybit_candles),
        )
        return candles, None

    # default (your old behavior): csv
    if not CANDLES_PATH.exists():
        raise FileNotFoundError(f"Missing {CANDLES_PATH}")
    if not TRADES_PATH.exists():
        raise FileNotFoundError(f"Missing {TRADES_PATH}")

    candles = pd.read_csv(CANDLES_PATH, engine="python", on_bad_lines="skip")
    trades  = pd.read_csv(TRADES_PATH, engine="python", on_bad_lines="skip")

    candles = _to_dt(candles, "timestamp").sort_values("timestamp").reset_index(drop=True)
    trades  = _to_dt(trades, "timestamp").sort_values("timestamp").reset_index(drop=True)

    if "side" in trades.columns:
        trades["side"] = trades["side"].astype(str).str.upper()
    if "outcome" in trades.columns:
        trades["outcome"] = trades["outcome"].astype(str).str.upper()

    return candles, trades


def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
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


def label_tts_tdp(c: pd.DataFrame, RANGE_LEN: int | None = None) -> pd.DataFrame:
    """
    RANGE_LEN valdo ir:
      - range_hi/lo (praeities)
      - dev_count langą
      - context high/low, pos_in_range ir extremes  ✅ (čia buvo "tyli" klaida)
    """
    if RANGE_LEN is None:
        RANGE_LEN = CTX_WIN

    c = c.copy()

    # ATR
    c["atr"] = atr(c, ATR_WIN)

    # CONTEXT window (FIX: CTX_WIN -> RANGE_LEN)
    ctx_high = c["high"].rolling(RANGE_LEN).max()
    ctx_low  = c["low"].rolling(RANGE_LEN).min()
    ctx_width = (ctx_high - ctx_low)

    c["range_width_atr"] = ctx_width / c["atr"]
    c["impulse_atr"] = (c["close"] - c["close"].shift(RANGE_LEN)).abs() / c["atr"]

    # RANGE bounds (praeities)
    range_hi = c["high"].rolling(RANGE_LEN).max().shift(1)
    range_lo = c["low"].rolling(RANGE_LEN).min().shift(1)
    c["range_hi"] = range_hi
    c["range_lo"] = range_lo

    # Breakout strength
    c["breakout_up_atr"] = (c["close"] - range_hi) / c["atr"]
    c["breakout_dn_atr"] = (range_lo - c["close"]) / c["atr"]

    # Deviations
    DEV_ATR = 0.5
    c["dev_up"] = c["high"] > (range_hi + DEV_ATR * c["atr"])
    c["dev_dn"] = c["low"]  < (range_lo - DEV_ATR * c["atr"])
    c["dev"] = c["dev_up"] | c["dev_dn"]

    c["dev_count"] = c["dev"].rolling(RANGE_LEN).sum()
    c["dev_up_count"] = c["dev_up"].rolling(RANGE_LEN).sum()
    c["dev_dn_count"] = c["dev_dn"].rolling(RANGE_LEN).sum()

    # Extremes by context
    pos = (c["close"] - ctx_low) / ctx_width.replace(0, np.nan)
    c["pos_in_range"] = pos.clip(0, 1)
    c["is_top_extreme"] = c["pos_in_range"] >= (1.0 - EXTREME_Q)
    c["is_bot_extreme"] = c["pos_in_range"] <= EXTREME_Q

    # ===== HTF trend =====
    htf = (
        c.set_index("timestamp")[["open", "high", "low", "close"]]
        .resample(HTF)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna()
    )

    htf["ema_fast"] = htf["close"].ewm(span=EMA_FAST, adjust=False).mean()
    htf["ema_slow"] = htf["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    htf["htf_trend"] = np.where(
        htf["ema_fast"] > htf["ema_slow"], "UP",
        np.where(htf["ema_fast"] < htf["ema_slow"], "DOWN", "FLAT")
    )

    c = c.sort_values("timestamp")
    htf = htf.reset_index().sort_values("timestamp")
    c = pd.merge_asof(c, htf[["timestamp", "htf_trend"]], on="timestamp", direction="backward")

    # ===== TTS =====
    tts_core = (
        (c["impulse_atr"] >= IMPULSE_ATR_MIN) &
        (c["range_width_atr"] <= RANGE_ATR_MAX) &
        (c["dev_count"] <= DEV_MAX)
    )
    tts_up = tts_core & (c["breakout_up_atr"] >= BREAKOUT_ATR_MIN)
    tts_dn = tts_core & (c["breakout_dn_atr"] >= BREAKOUT_ATR_MIN)

    # ===== TTS DEBUG =====
    c["tts_impulse_ok"] = c["impulse_atr"] >= IMPULSE_ATR_MIN
    c["tts_range_ok"] = c["range_width_atr"] <= RANGE_ATR_MAX
    c["tts_dev_ok"] = c["dev_count"] <= DEV_MAX
    c["tts_bu_ok"] = c["breakout_up_atr"] >= BREAKOUT_ATR_MIN
    c["tts_bd_ok"] = c["breakout_dn_atr"] >= BREAKOUT_ATR_MIN

    if "htf_trend" in c.columns:
        c["tts_trend_up_ok"] = c["htf_trend"] == "UP"
        c["tts_trend_dn_ok"] = c["htf_trend"] == "DOWN"
    else:
        c["tts_trend_up_ok"] = True
        c["tts_trend_dn_ok"] = True

    tts_core_dbg = c["tts_impulse_ok"] & c["tts_range_ok"] & c["tts_dev_ok"]

    # print TTS debug only when explicitly enabled
    import os
    _tts_debug = os.getenv("TTS_DEBUG", "").strip().lower() in ("1", "true", "yes", "y")
    if _tts_debug:
        print("\n[TTS DEBUG]")
        print("total candles:", len(c))
        print("impulse_ok :", int(c["tts_impulse_ok"].sum()))
        print("range_ok   :", int(c["tts_range_ok"].sum()))
        print("dev_ok     :", int(c["tts_dev_ok"].sum()))
        print("tts_core   :", int(tts_core_dbg.sum()))
        print("breakout_up:", int(c["tts_bu_ok"].sum()))
        print("breakout_dn:", int(c["tts_bd_ok"].sum()))
        if REQUIRE_TREND_FOR_TTS:
            print("trend_up_ok:", int(c["tts_trend_up_ok"].sum()))
            print("trend_dn_ok:", int(c["tts_trend_dn_ok"].sum()))
        print("TTS_UP:", int(tts_up.sum()))
        print("TTS_DN:", int(tts_dn.sum()))

        for col in ["impulse_atr", "range_width_atr", "breakout_up_atr"]:
            s = pd.to_numeric(c[col], errors="coerce").dropna()
            if len(s):
                print(f"{col} q:", s.quantile([0.5, 0.7, 0.8, 0.9]).to_dict())

    # ===== TDP =====
    tdp_impulse_ok = c["impulse_atr"] >= TDP_IMPULSE_MIN

    tdp_common = (
        (c["dev_count"] >= DEV_MIN_TDP) &
        (c["range_width_atr"] <= TDP_RANGE_ATR_MAX) &
        tdp_impulse_ok
    )

    # extremes jau iš RANGE_LEN konteksto ✅
    tdp_top = tdp_common & (c["dev_up_count"] >= 1) & c["is_top_extreme"]
    tdp_bot = tdp_common & (c["dev_dn_count"] >= 1) & c["is_bot_extreme"]

    if REQUIRE_TREND_FOR_TDP:
        tdp_top = tdp_top & (c["htf_trend"] == "DOWN")
        tdp_bot = tdp_bot & (c["htf_trend"] == "UP")

    # ===== Labels =====
    c["label"] = "NONE"
    c["sub_label"] = None
    c["tts_dir"] = None
    c["tdp_dir"] = None
    # TDP
    c.loc[tdp_top, ["label", "sub_label", "tdp_dir"]] = ["TDP", "TDP_TOP", "SHORT"]
    c.loc[tdp_bot, ["label", "sub_label", "tdp_dir"]] = ["TDP", "TDP_BOT", "LONG"]

    # TTS (perrašo jei sutampa)
    c.loc[tts_up, ["label", "sub_label", "tts_dir", "tdp_dir"]] = ["TTS", "TTS_UP", "LONG", None]
    c.loc[tts_dn, ["label", "sub_label", "tts_dir", "tdp_dir"]] = ["TTS", "TTS_DN", "SHORT", None]

    return c


def add_phase(ctx: pd.DataFrame) -> pd.DataFrame:
    """
    Adds ctx['phase'] based on regime + trend_dir (single truth).
    PHASE_RANGE > PHASE_TREND_UP/DOWN > PHASE_UNKNOWN
    """
    if ctx is None or len(ctx) == 0:
        return ctx

    regime = ctx.get("regime", pd.Series(index=ctx.index, dtype="object"))
    tdir = ctx.get("trend_dir", pd.Series(index=ctx.index, dtype="object"))

    regime = regime.astype(str).str.upper().fillna("")
    tdir = tdir.astype(str).str.upper().fillna("")

    phase = np.where(
        regime == "RANGE",
        "PHASE_RANGE",
        np.where(
            tdir == "UP",
            "PHASE_TREND_UP",
            np.where(tdir == "DOWN", "PHASE_TREND_DOWN", "PHASE_UNKNOWN"),
        ),
    )

    ctx = ctx.copy()
    ctx["phase"] = phase
    return ctx


def build_ctx(candles: pd.DataFrame) -> pd.DataFrame:
    """
    Returns ctx_m: timestamp-aligned context columns for merge_asof.
    Includes OHLC + ctx labels + market_regime fields + phase.
    """
    ctx = label_tts_tdp(candles)

    ctx_m = ctx.rename(columns={
        "label": "ctx_label",
        "sub_label": "ctx_sub_label",
        "tts_dir": "ctx_tts_dir",
        "tdp_dir": "ctx_tdp_dir",
    })

    # IMPORTANT: entry_model.generate_entries_from_ctx() expects 'sub_label'
    # so we keep an alias.
    if "ctx_sub_label" in ctx_m.columns:
        ctx_m["sub_label"] = ctx_m["ctx_sub_label"]

    # Ensure timestamp exists and sorted
    ctx_m["timestamp"] = pd.to_datetime(
        ctx_m["timestamp"], utc=True, errors="coerce"
    )
    ctx_m = (
        ctx_m.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    # Defaults for regime fields (so pipeline always has them)
    for col, default in {
        "regime": "",
        "trend_dir": "",
        "trend_strength": 0.0,
        "atr_pct": 0.0,
    }.items():
        if col not in ctx_m.columns:
            ctx_m[col] = default

    # --- MERGE MARKET REGIME FROM CSV (single truth) ---
    mr_path = EXPORT_DIR / "market_regime.csv"
    if mr_path.exists():
        mr = pd.read_csv(mr_path, engine="python", on_bad_lines="skip")
        mr["timestamp"] = pd.to_datetime(mr["timestamp"], utc=True, errors="coerce")

        mr = mr.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

        # minimal cols
        if "trend_dir" not in mr.columns:
            mr["trend_dir"] = ""
        if "trend_strength" not in mr.columns:
            mr["trend_strength"] = 0.0
        if "atr_pct" not in mr.columns:
            mr["atr_pct"] = 0.0
        if "regime" not in mr.columns:
            mr["regime"] = ""

        ctx_m = pd.merge_asof(
            ctx_m,
            mr[["timestamp", "regime", "trend_dir", "trend_strength", "atr_pct"]].sort_values("timestamp"),
            on="timestamp",
            direction="backward",
            suffixes=("", "_mr"),
        )

        # If merge created *_mr columns (edge-case), prefer merged values
        for col in ["regime", "trend_dir", "trend_strength", "atr_pct"]:
            mr_col = f"{col}_mr"
            if mr_col in ctx_m.columns:
                ctx_m[col] = ctx_m[mr_col]
                ctx_m.drop(columns=[mr_col], inplace=True)

    # Normalize regime fields (always)
    ctx_m["regime"] = ctx_m["regime"].astype(str).str.upper().fillna("")
    ctx_m["trend_dir"] = ctx_m["trend_dir"].astype(str).str.upper().fillna("")
    ctx_m["trend_strength"] = pd.to_numeric(ctx_m["trend_strength"], errors="coerce").fillna(0.0)
    ctx_m["atr_pct"] = pd.to_numeric(ctx_m["atr_pct"], errors="coerce").fillna(0.0)

    # --- ADD PHASE (MVP) ---
    ctx_m = add_phase(ctx_m)

    # --- KEEP COLUMNS ---
    keep = [
        "timestamp",
        "open", "high", "low", "close",          # critical for entry_model
        "ctx_label", "ctx_sub_label",
        "sub_label",                              # alias entry_model
        "ctx_tts_dir", "ctx_tdp_dir",
        "htf_trend",
        "impulse_atr", "range_width_atr", "dev_count", "pos_in_range",
        "atr", "range_hi", "range_lo", "dev_up", "dev_dn",
        # regime fields
        "regime", "trend_dir", "trend_strength", "atr_pct",
        # phase (NEW)
        "phase",
    ]
    keep = [k for k in keep if k in ctx_m.columns]
    return ctx_m[keep].copy()


def merge_trades(trades: pd.DataFrame, ctx_m: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge_asof(
        trades.sort_values("timestamp"),
        ctx_m.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )
    if "side" in merged.columns:
        merged["side"] = merged["side"].astype(str).str.upper()
    if "outcome" in merged.columns:
        merged["outcome"] = merged["outcome"].astype(str).str.upper()
    return merged


def apply_filters(merged: pd.DataFrame) -> pd.DataFrame:
    m = merged.copy()
    m["side"] = m["side"].astype(str).str.upper()
    if "outcome" in m.columns:
        m["outcome"] = m["outcome"].astype(str).str.upper()

    ok_dir = (
        ((m["ctx_label"] == "TTS") & m["ctx_tts_dir"].notna() & (m["side"] == m["ctx_tts_dir"])) |
        ((m["ctx_label"] == "TDP") & m["ctx_tdp_dir"].notna() & (m["side"] == m["ctx_tdp_dir"]))
    )

    ok_trend = pd.Series(True, index=m.index)

    if REQUIRE_TREND_FOR_TTS:
        ok_trend &= ~(
            (m["ctx_label"] == "TTS") & (
                ((m["side"] == "LONG") & (m["htf_trend"] != "UP")) |
                ((m["side"] == "SHORT") & (m["htf_trend"] != "DOWN"))
            )
        )

    if REQUIRE_TREND_FOR_TDP:
        ok_trend &= ~((m["ctx_sub_label"] == "TDP_TOP") & (m["htf_trend"] != "DOWN"))
        ok_trend &= ~((m["ctx_sub_label"] == "TDP_BOT") & (m["htf_trend"] != "UP"))

    f = m[ok_dir & ok_trend].copy()

    # Pastaba: šitas filtras praktiškai redundant, bet palieku tavo logiką.
    if REQUIRE_EXTREME_FOR_TDP:
        f = f[~((f["ctx_label"] == "TDP") & (f["ctx_tdp_dir"].isna()))].copy()

    return f


def compute_basic_stats(tr: pd.DataFrame, title: str) -> None:
    if tr.empty:
        print(f"\n{title}: no trades")
        return

    tr = tr.copy()
    tr["outcome"] = tr["outcome"].fillna("NO_HIT").astype(str).str.upper()

    total = len(tr)
    wins = int((tr["outcome"] == "WIN").sum())
    losses = int((tr["outcome"] == "LOSS").sum())
    nohit = int((tr["outcome"] == "NO_HIT").sum())
    winrate = (wins / (wins + losses) * 100.0) if (wins + losses) > 0 else 0.0

    print(f"\n{title}")
    print(f"Trades total:       {total}")
    print(f"Wins/Loss/NO_HIT:   {wins}/{losses}/{nohit}")
    print(f"Winrate (W/L only): {winrate:.2f}%")


def main():
    candles, trades = load_inputs()
    ctx_m = build_ctx(candles)
    merged = merge_trades(trades, ctx_m)

    print("Candles file:", CANDLES_PATH, "rows=", len(candles))
    print("Trades file :", TRADES_PATH, "rows=", len(trades))
    print("Candles time:", candles["timestamp"].min(), "->", candles["timestamp"].max())
    print("Trades  time:", trades["timestamp"].min(), "->", trades["timestamp"].max())

    print("\nTRADES ctx_label counts:")
    print(merged["ctx_label"].value_counts(dropna=False).head(10))

    f = apply_filters(merged)

    print("\nFILTERED label counts:")
    print(f["ctx_label"].value_counts(dropna=False))

    compute_basic_stats(f, title="FILTERED (DIR + TREND + EXTREME)")

    merged.to_csv(EXPORT_DIR / "trades_with_ctx.csv", index=False)
    f.to_csv(EXPORT_DIR / "trades_filtered.csv", index=False)
    print(f"\n✅ Saved:\n- {EXPORT_DIR / 'trades_with_ctx.csv'}\n- {EXPORT_DIR / 'trades_filtered.csv'}")


if __name__ == "__main__":
    main()


# ---- MARKET REGIME MERGE (safe) ----
MARKET_REGIME_PATH = Path("backtest/journal/exports_trades/market_regime.csv")

def merge_market_regime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Safely merge market_regime.csv into df by timestamp (merge_asof, backward).
    Always guarantees columns exist: regime, trend_dir, trend_strength, atr_pct
    """
    out = df.copy()

    # ensure timestamp
    if "timestamp" not in out.columns:
        raise KeyError("merge_market_regime: df missing 'timestamp' column")
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # defaults (so callers can rely on them even if csv missing)
    if "regime" not in out.columns:
        out["regime"] = ""
    if "trend_dir" not in out.columns:
        out["trend_dir"] = ""
    if "trend_strength" not in out.columns:
        out["trend_strength"] = 0.0
    if "atr_pct" not in out.columns:
        out["atr_pct"] = 0.0

    # if no file -> just normalize defaults and return
    if not MARKET_REGIME_PATH.exists():
        out["regime"] = out["regime"].astype(str).str.upper()
        out["trend_dir"] = out["trend_dir"].astype(str).str.upper()
        out["trend_strength"] = pd.to_numeric(out["trend_strength"], errors="coerce").fillna(0.0)
        out["atr_pct"] = pd.to_numeric(out["atr_pct"], errors="coerce").fillna(0.0)
        return out

    mr = pd.read_csv(MARKET_REGIME_PATH, engine="python", on_bad_lines="skip")
    if "timestamp" not in mr.columns:
        # can't merge -> return normalized defaults
        out["regime"] = out["regime"].astype(str).str.upper()
        out["trend_dir"] = out["trend_dir"].astype(str).str.upper()
        out["trend_strength"] = pd.to_numeric(out["trend_strength"], errors="coerce").fillna(0.0)
        out["atr_pct"] = pd.to_numeric(out["atr_pct"], errors="coerce").fillna(0.0)
        return out

    mr["timestamp"] = pd.to_datetime(mr["timestamp"], errors="coerce")
    mr = mr.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # make sure mr has required columns
    for col, default in [("regime",""), ("trend_dir",""), ("trend_strength",0.0), ("atr_pct",0.0)]:
        if col not in mr.columns:
            mr[col] = default

    mr = mr[["timestamp","regime","trend_dir","trend_strength","atr_pct"]].copy()
    mr["regime"] = mr["regime"].astype(str).str.upper()
    mr["trend_dir"] = mr["trend_dir"].astype(str).str.upper()
    mr["trend_strength"] = pd.to_numeric(mr["trend_strength"], errors="coerce").fillna(0.0)
    mr["atr_pct"] = pd.to_numeric(mr["atr_pct"], errors="coerce").fillna(0.0)

    out = pd.merge_asof(
        out,
        mr.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        suffixes=("", "_mr"),
    )

    # normalize final
    if "regime" in out.columns:
        out["regime"] = out["regime"].astype(str).str.upper()
    if "trend_dir" in out.columns:
        out["trend_dir"] = out["trend_dir"].astype(str).str.upper()
    if "trend_strength" in out.columns:
        out["trend_strength"] = pd.to_numeric(out["trend_strength"], errors="coerce").fillna(0.0)
    if "atr_pct" in out.columns:
        out["atr_pct"] = pd.to_numeric(out["atr_pct"], errors="coerce").fillna(0.0)

    return out


# -----------------------------------------------------------------------------
# Bybit helper (shared)
# -----------------------------------------------------------------------------
try:
    from backtest.journal.bybit_loader import load_bybit_latest  # type: ignore
except Exception:
    # fallback: allow importing module even if requests missing in some env
    load_bybit_latest = None  # type: ignore
