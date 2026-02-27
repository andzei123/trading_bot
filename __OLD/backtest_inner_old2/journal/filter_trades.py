from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

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
RANGE_ATR_MAX   = 3.0
DEV_MAX         = 1
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


def _to_dt(df: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    # Greita ir stabili (grid search metu labai svarbu)
    df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d %H:%M:%S", cache=True)
    return df.dropna(subset=[col])


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
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
    if RANGE_LEN is None:
        RANGE_LEN = CTX_WIN

    c = c.copy()

    # ATR
    c["atr"] = atr(c, ATR_WIN)

    # CONTEXT window
    ctx_high = c["high"].rolling(CTX_WIN).max()
    ctx_low  = c["low"].rolling(CTX_WIN).min()
    ctx_width = (ctx_high - ctx_low)

    c["range_width_atr"] = ctx_width / c["atr"]
    c["impulse_atr"] = (c["close"] - c["close"].shift(CTX_WIN)).abs() / c["atr"]

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

    # ===== TDP =====
    tdp_impulse_ok = c["impulse_atr"] >= TDP_IMPULSE_MIN

    tdp_common = (
        (c["dev_count"] >= DEV_MIN_TDP) &
        (c["range_width_atr"] <= TDP_RANGE_ATR_MAX) &
        tdp_impulse_ok
    )

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

    c.loc[tdp_top, ["label", "sub_label", "tdp_dir"]] = ["TDP", "TDP_TOP", "SHORT"]
    c.loc[tdp_bot, ["label", "sub_label", "tdp_dir"]] = ["TDP", "TDP_BOT", "LONG"]

    c.loc[tts_up, ["label", "sub_label", "tts_dir", "tdp_dir"]] = ["TTS", "TTS_UP", "LONG", None]
    c.loc[tts_dn, ["label", "sub_label", "tts_dir", "tdp_dir"]] = ["TTS", "TTS_DN", "SHORT", None]

    return c


def build_ctx(candles: pd.DataFrame) -> pd.DataFrame:
    """
    Returns ctx_m: timestamp-aligned context columns for merge_asof.
    """
    ctx = label_tts_tdp(candles)

    ctx_m = ctx.rename(columns={
        "label": "ctx_label",
        "sub_label": "ctx_sub_label",
        "tts_dir": "ctx_tts_dir",
        "tdp_dir": "ctx_tdp_dir",
    })

    # Svarbu: paliekam ir tuos laukus, kurių reikia entry model / debug
    keep = [
        "timestamp",
        "ctx_label", "ctx_sub_label",
        "ctx_tts_dir", "ctx_tdp_dir",
        "htf_trend",
        "impulse_atr", "range_width_atr", "dev_count", "pos_in_range",
        "atr", "range_hi", "range_lo", "dev_up", "dev_dn"
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
