from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict

import numpy as np
import pandas as pd

from backtest.journal.gates import allow_entry, GateConfig

# --- feature toggles (MVP) ---
ENABLE_TTS_RETEST = False  # keep OFF until TTS logic is validated


# ============================================================
# Data structures
# ============================================================

@dataclass
class Entry:
    timestamp: pd.Timestamp
    model: str               # "TDP_REENTRY" / "TTS_RETEST"
    side: str                # "LONG" / "SHORT"
    entry: float
    sl: float
    tp: float
    meta: str = ""
    ctx_sub_label: Optional[str] = None   # "TDP_TOP"/"TDP_BOT"/"TTS_UP"/"TTS_DN"

    # --- regime fields (ateina iš ctx, kuris turi market_regime merge) ---
    regime: Optional[str] = None
    trend_dir: Optional[str] = None
    trend_strength: Optional[float] = None
    atr_pct: Optional[float] = None
    phase: Optional[str] = None


# -----------------------------
# Level 2.5: candle cache
# -----------------------------
@dataclass(frozen=True)
class CandleCache:
    ts: np.ndarray                 # datetime64[ns]
    high: np.ndarray               # float64
    low: np.ndarray                # float64
    ts_to_idx: Dict[pd.Timestamp, int]


def build_candle_cache(candles: pd.DataFrame) -> CandleCache:
    """
    One-time preprocessing:
      - sort by timestamp
      - ensure timestamp is datetime
      - build numpy arrays + ts->index mapping
    """
    if candles is None or candles.empty:
        return CandleCache(
            ts=np.array([], dtype="datetime64[ns]"),
            high=np.array([], dtype=float),
            low=np.array([], dtype=float),
            ts_to_idx={},
        )

    c = candles.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    ts = c["timestamp"].to_numpy(dtype="datetime64[ns]")
    high = pd.to_numeric(c["high"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(c["low"], errors="coerce").to_numpy(dtype=float)

    # mapping for exact grid
    ts_to_idx = {pd.Timestamp(t): i for i, t in enumerate(ts)}

    return CandleCache(ts=ts, high=high, low=low, ts_to_idx=ts_to_idx)


# ============================================================
# Simulator (with candle_cache acceleration)
# ============================================================
def _touches(side: str, h: float, l: float, px: float, kind: str) -> bool:
    # kind: "TP"/"SL"/"PX"
    if kind in ("TP", "PX"):
        return (h >= px) if side == "LONG" else (l <= px)
    if kind == "SL":
        return (l <= px) if side == "LONG" else (h >= px)
    raise ValueError(kind)


def _conservative_order(side: str, h: float, l: float, sl_px: float, tp_px: float) -> Optional[str]:
    """
    Conservative: if both SL and TP touched in same candle -> assume SL first.
    """
    sl_hit = _touches(side, h, l, sl_px, "SL")
    tp_hit = _touches(side, h, l, tp_px, "TP")
    if sl_hit and tp_hit:
        return "LOSS"
    if sl_hit:
        return "LOSS"
    if tp_hit:
        return "WIN"
    return None


def simulate_trades(
    candles: pd.DataFrame,
    entries: List[Entry],
    max_hold_bars: int = 200,
    be_after_r: Optional[float] = None,       # e.g. 1.0 => after +1R move, SL -> BE
    partial_at_r: Optional[float] = None,     # e.g. 1.0 => take partial at +1R
    partial_frac: float = 0.7,                # fraction closed at partial
    candle_cache: Optional[CandleCache] = None,

    # schema support (DEV3/DEV-C): keep keyword-only at the end to avoid breaking positional callers
    symbol: str = "",
) -> pd.DataFrame:
    """
    Simulates outcomes and returns per-trade R-multiple with optional BE/partials.

    R model:
      - Risk R = abs(entry - initial_sl)
      - If LOSS: realized_R = -1 * (remaining_size)
      - If TP: realized_R = +RR * (remaining_size)
      - If partial hits first: realized_R += partial_frac * partial_at_r
        remaining_size = 1 - partial_frac
      - If BE activated and later SL hits => remaining part exits at 0R
    """
    if not entries:
        return pd.DataFrame()

    cache = candle_cache if candle_cache is not None else build_candle_cache(candles)
    if cache.ts.size == 0:
        return pd.DataFrame()

    ts_arr = cache.ts
    high_arr = cache.high
    low_arr = cache.low
    ts_to_idx = cache.ts_to_idx

    rows = []
    for i, e in enumerate(entries, 1):
        idx0 = ts_to_idx.get(pd.Timestamp(e.timestamp))
        if idx0 is None:
            continue

        side = str(e.side).upper()
        entry_px = float(e.entry)
        sl0 = float(e.sl)
        tp = float(e.tp)

        risk = abs(entry_px - sl0)
        if risk <= 0:
            continue

        rr = abs(tp - entry_px) / max(1e-9, risk)

        start_idx = idx0
        end_idx = min(len(ts_arr) - 1, start_idx + int(max_hold_bars))

        remaining = 1.0
        realized_r = 0.0
        partial_done = False
        be_active = False
        sl_active = sl0

        def price_at_r(r_mult: float) -> float:
            return (entry_px + r_mult * risk) if side == "LONG" else (entry_px - r_mult * risk)

        partial_px = price_at_r(float(partial_at_r)) if partial_at_r is not None else None
        be_px = price_at_r(float(be_after_r)) if be_after_r is not None else None

        outcome = "NO_HIT"
        exit_price = np.nan
        exit_ts = pd.NaT
        exit_reason = ""

        for j in range(start_idx, end_idx + 1):
            h = float(high_arr[j])
            l = float(low_arr[j])

            # 0) partial check
            if (not partial_done) and (partial_px is not None) and remaining > 0:
                if _touches(side, h, l, partial_px, "PX"):
                    # conservative: SL before partial if both touched
                    if _touches(side, h, l, sl_active, "SL"):
                        outcome = "LOSS"
                        exit_ts = pd.Timestamp(ts_arr[j])
                        exit_price = sl_active
                        exit_reason = "SL_before_partial"
                        realized_r += -1.0 * remaining
                        remaining = 0.0
                        break

                    realized_r += float(partial_at_r) * float(partial_frac)
                    remaining = max(0.0, 1.0 - float(partial_frac))
                    partial_done = True

            # 1) BE activation
            if (not be_active) and (be_px is not None) and remaining > 0:
                if _touches(side, h, l, be_px, "PX"):
                    if _touches(side, h, l, sl_active, "SL"):
                        outcome = "LOSS"
                        exit_ts = pd.Timestamp(ts_arr[j])
                        exit_price = sl_active
                        exit_reason = "SL_before_BE"
                        realized_r += -1.0 * remaining
                        remaining = 0.0
                        break

                    be_active = True
                    sl_active = entry_px

            # 2) TP/SL check
            res = _conservative_order(side, h, l, sl_active, tp)
            if res is None:
                continue

            exit_ts = pd.Timestamp(ts_arr[j])

            if res == "LOSS":
                if be_active and abs(sl_active - entry_px) < 1e-9:
                    outcome = "BE"
                    exit_price = entry_px
                    exit_reason = "BE_stop"
                    # realized_r unchanged
                else:
                    outcome = "LOSS"
                    exit_price = sl_active
                    exit_reason = "SL"
                    realized_r += -1.0 * remaining
                remaining = 0.0
                break

            if res == "WIN":
                outcome = "WIN"
                exit_price = tp
                exit_reason = "TP"
                realized_r += rr * remaining
                remaining = 0.0
                break

        rows.append({
            "id": i,
            "timestamp": pd.Timestamp(e.timestamp),
            "symbol": str(symbol).strip(),
            "status": "OK",
            "side": side,
            "entry": entry_px,
            "sl": sl0,
            "tp": tp,
            "rr": round(rr, 6),

            "model": e.model,
            "ctx_sub_label": e.ctx_sub_label,

            # --- regime fields in output CSV ---
            "regime": e.regime,
            "trend_dir": e.trend_dir,
            "trend_strength": e.trend_strength,
            "atr_pct": e.atr_pct,
            "phase": e.phase,

            "meta": f"model={e.model} {e.meta}".strip(),
            "outcome": outcome,
            "exit_price": exit_price,
            "exit_timestamp": exit_ts,
            "exit_reason": exit_reason,

            "partial_taken": bool(partial_done),
            "be_moved": bool(be_active),
            "R": round(float(realized_r), 6),
        })

    #print("USING CACHE:", candle_cache is not None)

    return pd.DataFrame(rows)


def generate_entries_from_ctx(
    ctx: pd.DataFrame,
    rr: float = 3.0,
    sl_atr_buffer: float = 0.25,
    tdp_dev_lookback: int = 6,
    tdp_bot_confirm_n: int = 3,            # CHANGED: rolling window n (default 3)
    tdp_bot_confirm_k: int = 2,            # ADDED: k-of-n confirm (default 2)
    require_impulse_before_tdp: bool = True,
    impulse_lookback: int = 10,
    impulse_size_atr: float = 1.2,
    tts_retest_lookback: int = 24,
    # MVP “quality knobs” (galėsi vėliau grid’inti)
    require_mid_reclaim_for_bot: bool = False,   # CHANGED: now real reclaim over range_lo
    allow_tdp_bot_in_range: bool = True,        # unchanged (but phase gate in mask enforces TREND_UP)
) -> List["Entry"]:
    """
    Clean MVP entry generator (deterministic, no duplicated gates).

    TDP_REENTRY:
      - TDP_TOP: dev_up_recent + close<=range_hi -> SHORT
      - TDP_BOT: dev_dn_recent + close>=range_lo -> LONG
        + optional: mid reclaim (close>=range_mid)
        + optional: N-bar micro-confirm (anti falling knife)
        + phase gate: allow in TREND_UP (and optionally RANGE)

    TTS_RETEST (optional):
      - TTS_UP retest -> LONG
      - TTS_DN retest -> SHORT
      + phase gate: by default avoid PHASE_RANGE to not mix (but you can change)

    Also applies allow_entry() smart gate before appending.
    """

    c = ctx.copy().sort_values("timestamp").reset_index(drop=True)

    needed = [
        "timestamp", "open", "high", "low", "close", "atr",
        "sub_label", "dev_up", "dev_dn", "range_hi", "range_lo",
    ]
    missing = [x for x in needed if x not in c.columns]
    if missing:
        raise KeyError(f"ctx missing columns: {missing}. Add to filter_trades.build_ctx()/label_tts_tdp() output.")

    # --- ensure regime fields exist ---
    for col, default in [
        ("regime", ""),
        ("trend_dir", ""),
        ("trend_strength", 0.0),
        ("atr_pct", 0.0),
        ("phase", ""),
    ]:
        if col not in c.columns:
            c[col] = default

    c["regime"] = c["regime"].astype(str).str.upper()
    c["trend_dir"] = c["trend_dir"].astype(str).str.upper()
    c["trend_strength"] = pd.to_numeric(c["trend_strength"], errors="coerce").fillna(0.0)
    c["atr_pct"] = pd.to_numeric(c["atr_pct"], errors="coerce").fillna(0.0)
    c["phase"] = c["phase"].astype(str).str.upper()

    # default smart gate config (pressure OFF)
    gate_cfg = GateConfig(trend_min=0.0, need_atr=False, atr_min=0.0015)

    # ---------------- helpers ----------------
    def allow_phase_for_tdp_bot(ph: str) -> bool:
        ph = str(ph).upper()
        if ph == "PHASE_TREND_UP":
            return True
        if ph == "PHASE_RANGE":
            return bool(allow_tdp_bot_in_range)
        return False  # block PHASE_TREND_DOWN + unknown

    def allow_phase_for_tts(ph: str) -> bool:
        # kol neturim atskiro RANGE modelio, default: neprekiaujam RANGE per TTS
        return str(ph).upper() != "PHASE_RANGE"

    # impulse features (optional)
    if require_impulse_before_tdp:
        if "impulse_dir" not in c.columns or "impulse_recent" not in c.columns:
            imp = (c["close"] - c["close"].shift(impulse_lookback)) / c["atr"]
            c["impulse_dir"] = np.where(imp > 0, "UP", np.where(imp < 0, "DOWN", "FLAT"))
            c["impulse_recent"] = (imp.abs() >= impulse_size_atr)

    entries: List["Entry"] = []

    recent_high = c["high"].rolling(tdp_dev_lookback).max()
    recent_low = c["low"].rolling(tdp_dev_lookback).min()

    # sweep low (jei dev_dn labai retas, bus dažnai NaN -> turim fallback)
    sweep_low = (
        c["low"]
        .where(c["dev_dn"].astype(bool))
        .rolling(tdp_dev_lookback)
        .min()
    )

    # ---------- base TDP conditions ----------
    reentry_short = (c["sub_label"] == "TDP_TOP") & (c["close"] <= c["range_hi"])
    reentry_long  = (c["sub_label"] == "TDP_BOT") & (c["close"] >= c["range_lo"])

    dev_up_recent = c["dev_up"].rolling(tdp_dev_lookback).max().fillna(False).astype(bool)
    dev_dn_recent = c["dev_dn"].rolling(tdp_dev_lookback).max().fillna(False).astype(bool)
    reentry_short &= dev_up_recent
    reentry_long  &= dev_dn_recent

    if require_impulse_before_tdp:
        # SHORT: prieš tai turi būti UP impulsas (sweep top)
        reentry_short &= c["impulse_recent"] & (c["impulse_dir"] == "UP")
        # LONG: prieš tai turi būti DOWN impulsas (sweep bottom)
        reentry_long  &= c["impulse_recent"] & (c["impulse_dir"] == "DOWN")

    # --- CHANGED (1): phase gate EARLY (po dev + impulse, PRIEŠ confirm) ---
    reentry_long &= (c["phase"].astype(str).str.upper() == "PHASE_TREND_UP")

    # --- CHANGED (4): mid reclaim -> real reclaim over range_lo (optional) ---
    if require_mid_reclaim_for_bot:
        reentry_long &= (c["close"] > c["range_lo"]) & (c["close"].shift(1) <= c["range_lo"])

    # --- CHANGED (2): soft confirm k-of-n (2 iš 3) ---
    n = max(1, int(tdp_bot_confirm_n))      # e.g. 3
    k = max(1, int(tdp_bot_confirm_k))      # e.g. 2

    up_bar = ((c["close"] > c["close"].shift(1)) & (c["low"] > c["low"].shift(1))).fillna(False).astype(bool)
    bot_confirm = (up_bar.rolling(n).sum() >= k).fillna(False).astype(bool)
    reentry_long &= bot_confirm

    # --- CHANGED (3): trend_strength floor (LONG only) ---
    trend_min_long = 0.02
    reentry_long &= (pd.to_numeric(c["trend_strength"], errors="coerce").fillna(0.0) >= trend_min_long)

    # ------------------- SHORT entries (TDP_TOP) -------------------
    for i in np.where(reentry_short.values)[0]:
        entry_px = float(c.loc[i, "close"])
        atr = float(c.loc[i, "atr"])

        sl = float(recent_high.loc[i] + sl_atr_buffer * atr)
        risk = max(1e-9, sl - entry_px)
        tp = entry_px - rr * risk

        row = c.loc[i].copy()
        row["side"] = "SHORT"
        row["ctx_sub_label"] = "TDP_TOP"
        if not allow_entry(row, gate_cfg):
            continue

        entries.append(Entry(
            timestamp=pd.Timestamp(c.loc[i, "timestamp"]),
            model="TDP_REENTRY",
            side="SHORT",
            entry=entry_px,
            sl=sl,
            tp=tp,
            meta="TDP_TOP reentry",
            ctx_sub_label="TDP_TOP",
            regime=str(c.loc[i, "regime"]),
            trend_dir=str(c.loc[i, "trend_dir"]),
            trend_strength=float(c.loc[i, "trend_strength"]),
            atr_pct=float(c.loc[i, "atr_pct"]),
            phase=str(c.loc[i, "phase"]),
        ))

    # ------------------- LONG entries (TDP_BOT) -------------------
    if True:  # DEBUG ON/OFF
        base = (c["sub_label"] == "TDP_BOT") & (c["close"] >= c["range_lo"])
        after_dev = base & dev_dn_recent
        if require_impulse_before_tdp:
            after_imp = after_dev & c["impulse_recent"] & (c["impulse_dir"] == "DOWN")
        else:
            after_imp = after_dev

        after_phase = after_imp & (c["phase"].astype(str).str.upper() == "PHASE_TREND_UP")

        if require_mid_reclaim_for_bot:
            after_mid = after_phase & (c["close"] > c["range_lo"]) & (c["close"].shift(1) <= c["range_lo"])
        else:
            after_mid = after_phase

        n = max(1, int(tdp_bot_confirm_n))
        k = max(1, int(tdp_bot_confirm_k))
        up_bar = ((c["close"] > c["close"].shift(1)) & (c["low"] > c["low"].shift(1))).fillna(False).astype(bool)
        bot_confirm = (up_bar.rolling(n).sum() >= k).fillna(False).astype(bool)
        after_conf = after_mid & bot_confirm

        trend_min_long = 0.02
        after_trend = after_conf & (pd.to_numeric(c["trend_strength"], errors="coerce").fillna(0.0) >= trend_min_long)

        print("\n[DEBUG LONG FUNNEL]")
        print("base:", int(base.sum()))
        print("after_dev:", int(after_dev.sum()))
        print("after_imp:", int(after_imp.sum()))
        print("after_phase:", int(after_phase.sum()))
        print("after_mid:", int(after_mid.sum()))
        print("after_conf:", int(after_conf.sum()))
        print("after_trend:", int(after_trend.sum()))
        print("phases (after_trend):")
        print(c.loc[after_trend, "phase"].value_counts(dropna=False).head(10))
        print()

    for i in np.where(reentry_long.values)[0]:
        # phase gate (vienas, aiškus) - paliekam kaip safety, nors mask'e jau prafiltruota
        ph = str(c.loc[i, "phase"]).upper()
        if not allow_phase_for_tdp_bot(ph):
            continue

        entry_px = float(c.loc[i, "close"])
        atr = float(c.loc[i, "atr"])

        base_low = sweep_low.loc[i]
        if pd.isna(base_low):
            base_low = recent_low.loc[i]  # fallback

        sl = float(base_low - sl_atr_buffer * atr)
        risk = max(1e-9, entry_px - sl)
        tp = entry_px + rr * risk

        row = c.loc[i].copy()
        row["side"] = "LONG"
        row["ctx_sub_label"] = "TDP_BOT"
        if not allow_entry(row, gate_cfg):
            continue

        entries.append(Entry(
            timestamp=pd.Timestamp(c.loc[i, "timestamp"]),
            model="TDP_REENTRY",
            side="LONG",
            entry=entry_px,
            sl=sl,
            tp=tp,
            meta="TDP_BOT reentry",
            ctx_sub_label="TDP_BOT",
            regime=str(c.loc[i, "regime"]),
            trend_dir=str(c.loc[i, "trend_dir"]),
            trend_strength=float(c.loc[i, "trend_strength"]),
            atr_pct=float(c.loc[i, "atr_pct"]),
            phase=str(c.loc[i, "phase"]),
        ))

    # ------------------- TTS_RETEST (optional) -------------------
    if ENABLE_TTS_RETEST:
        tts_up_flag = (c["sub_label"] == "TTS_UP")
        tts_dn_flag = (c["sub_label"] == "TTS_DN")

        had_tts_up = tts_up_flag.rolling(tts_retest_lookback).max().fillna(False).astype(bool)
        had_tts_dn = tts_dn_flag.rolling(tts_retest_lookback).max().fillna(False).astype(bool)

        tts_retest_long  = had_tts_up & (c["low"] <= c["range_hi"]) & (c["close"] > c["range_hi"])
        tts_retest_short = had_tts_dn & (c["high"] >= c["range_lo"]) & (c["close"] < c["range_lo"])

        for i in np.where(tts_retest_long.values)[0]:
            ph = str(c.loc[i, "phase"]).upper()
            if not allow_phase_for_tts(ph):
                continue

            entry_px = float(c.loc[i, "close"])
            atr = float(c.loc[i, "atr"])
            sl = float(min(c.loc[i, "low"], c.loc[i, "range_hi"]) - sl_atr_buffer * atr)
            risk = max(1e-9, entry_px - sl)
            tp = entry_px + rr * risk

            row = c.loc[i].copy()
            row["side"] = "LONG"
            row["ctx_sub_label"] = "TTS_UP"
            if not allow_entry(row, gate_cfg):
                continue

            entries.append(Entry(
                timestamp=pd.Timestamp(c.loc[i, "timestamp"]),
                model="TTS_RETEST",
                side="LONG",
                entry=entry_px,
                sl=sl,
                tp=tp,
                meta="TTS_UP retest",
                ctx_sub_label="TTS_UP",
                regime=str(c.loc[i, "regime"]),
                trend_dir=str(c.loc[i, "trend_dir"]),
                trend_strength=float(c.loc[i, "trend_strength"]),
                atr_pct=float(c.loc[i, "atr_pct"]),
                phase=str(c.loc[i, "phase"]),
            ))

        for i in np.where(tts_retest_short.values)[0]:
            ph = str(c.loc[i, "phase"]).upper()
            if not allow_phase_for_tts(ph):
                continue

            entry_px = float(c.loc[i, "close"])
            atr = float(c.loc[i, "atr"])
            sl = float(max(c.loc[i, "high"], c.loc[i, "range_lo"]) + sl_atr_buffer * atr)
            risk = max(1e-9, sl - entry_px)
            tp = entry_px - rr * risk

            row = c.loc[i].copy()
            row["side"] = "SHORT"
            row["ctx_sub_label"] = "TTS_DN"
            if not allow_entry(row, gate_cfg):
                continue

            entries.append(Entry(
                timestamp=pd.Timestamp(c.loc[i, "timestamp"]),
                model="TTS_RETEST",
                side="SHORT",
                entry=entry_px,
                sl=sl,
                tp=tp,
                meta="TTS_DN retest",
                ctx_sub_label="TTS_DN",
                regime=str(c.loc[i, "regime"]),
                trend_dir=str(c.loc[i, "trend_dir"]),
                trend_strength=float(c.loc[i, "trend_strength"]),
                atr_pct=float(c.loc[i, "atr_pct"]),
                phase=str(c.loc[i, "phase"]),
            ))

    # dedupe timestamp+side (keep first)
    if entries:
        df_e = pd.DataFrame([e.__dict__ for e in entries])
        df_e = df_e.sort_values(["timestamp", "model"]).drop_duplicates(["timestamp", "side"], keep="first")
        entries = [Entry(**row) for row in df_e.to_dict("records")]

    return entries
