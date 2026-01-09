from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import numpy as np
import pandas as pd


@dataclass
class Entry:
    timestamp: pd.Timestamp
    model: str               # "TDP_REENTRY" / "TTS_RETEST"
    side: str                # "LONG" / "SHORT"
    entry: float
    sl: float
    tp: float
    meta: str = ""


def _to_ts(df: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.dropna(subset=[col])


def generate_entries_from_ctx(
    ctx: pd.DataFrame,
    rr: float = 3.0,
    sl_atr_buffer: float = 0.25,
    tdp_dev_lookback: int = 6,
    require_impulse_before_tdp: bool = True,
    impulse_lookback: int = 10,
    impulse_size_atr: float = 1.2,
    tts_retest_lookback: int = 24,
) -> List[Entry]:
    """
    MVP signal generator:
    - TDP_REENTRY:
        * TDP_TOP: dev_up buvo neseniai + close grįžta <= range_hi -> SHORT
        * TDP_BOT: dev_dn buvo neseniai + close grįžta >= range_lo -> LONG
    - TTS_RETEST:
        * TTS_UP: breakout + retest (low <= range_hi, close > range_hi) -> LONG
        * TTS_DN: breakout + retest (high >= range_lo, close < range_lo) -> SHORT

    Reikalauja, kad ctx turėtų:
      timestamp, open, high, low, close, atr,
      sub_label, dev_up, dev_dn, range_hi, range_lo
    """
    c = ctx.copy().sort_values("timestamp").reset_index(drop=True)

    needed = ["timestamp", "open", "high", "low", "close", "atr", "sub_label", "dev_up", "dev_dn", "range_hi", "range_lo"]
    missing = [x for x in needed if x not in c.columns]
    if missing:
        raise KeyError(f"ctx missing columns: {missing}. Papildyk filter_trades.label_tts_tdp() kad grąžintų šituos.")

    # optional impulse-before-TDP
    if require_impulse_before_tdp:
        if "impulse_dir" not in c.columns or "impulse_recent" not in c.columns:
            imp = (c["close"] - c["close"].shift(impulse_lookback)) / c["atr"]
            c["impulse_dir"] = np.where(imp > 0, "UP", np.where(imp < 0, "DOWN", "FLAT"))
            c["impulse_recent"] = (imp.abs() >= impulse_size_atr)

    entries: List[Entry] = []

    recent_high = c["high"].rolling(tdp_dev_lookback).max()
    recent_low = c["low"].rolling(tdp_dev_lookback).min()

    # ---------- TDP_REENTRY ----------
    reentry_short = (c["sub_label"] == "TDP_TOP") & (c["close"] <= c["range_hi"])
    reentry_long = (c["sub_label"] == "TDP_BOT") & (c["close"] >= c["range_lo"])

    dev_up_recent = c["dev_up"].rolling(tdp_dev_lookback).max().fillna(False).astype(bool)
    dev_dn_recent = c["dev_dn"].rolling(tdp_dev_lookback).max().fillna(False).astype(bool)
    reentry_short &= dev_up_recent
    reentry_long &= dev_dn_recent

    if require_impulse_before_tdp:
        reentry_short &= (c["impulse_recent"]) & (c["impulse_dir"] == "UP")
        reentry_long &= (c["impulse_recent"]) & (c["impulse_dir"] == "DOWN")

    for i in np.where(reentry_short.values)[0]:
        entry = float(c.loc[i, "close"])
        atr = float(c.loc[i, "atr"])
        sl = float(recent_high.loc[i] + sl_atr_buffer * atr)
        risk = max(1e-9, sl - entry)
        tp = entry - rr * risk
        entries.append(Entry(
            timestamp=c.loc[i, "timestamp"],
            model="TDP_REENTRY",
            side="SHORT",
            entry=entry,
            sl=sl,
            tp=tp,
            meta="TDP_TOP reentry",
        ))

    for i in np.where(reentry_long.values)[0]:
        entry = float(c.loc[i, "close"])
        atr = float(c.loc[i, "atr"])
        sl = float(recent_low.loc[i] - sl_atr_buffer * atr)
        risk = max(1e-9, entry - sl)
        tp = entry + rr * risk
        entries.append(Entry(
            timestamp=c.loc[i, "timestamp"],
            model="TDP_REENTRY",
            side="LONG",
            entry=entry,
            sl=sl,
            tp=tp,
            meta="TDP_BOT reentry",
        ))

    # ---------- TTS_RETEST ----------
    tts_up_flag = (c["sub_label"] == "TTS_UP")
    tts_dn_flag = (c["sub_label"] == "TTS_DN")
    had_tts_up = tts_up_flag.rolling(tts_retest_lookback).max().fillna(False).astype(bool)
    had_tts_dn = tts_dn_flag.rolling(tts_retest_lookback).max().fillna(False).astype(bool)

    tts_retest_long = had_tts_up & (c["low"] <= c["range_hi"]) & (c["close"] > c["range_hi"])
    tts_retest_short = had_tts_dn & (c["high"] >= c["range_lo"]) & (c["close"] < c["range_lo"])

    for i in np.where(tts_retest_long.values)[0]:
        entry = float(c.loc[i, "close"])
        atr = float(c.loc[i, "atr"])
        sl = float(min(c.loc[i, "low"], c.loc[i, "range_hi"]) - sl_atr_buffer * atr)
        risk = max(1e-9, entry - sl)
        tp = entry + rr * risk
        entries.append(Entry(
            timestamp=c.loc[i, "timestamp"],
            model="TTS_RETEST",
            side="LONG",
            entry=entry,
            sl=sl,
            tp=tp,
            meta="TTS_UP retest",
        ))

    for i in np.where(tts_retest_short.values)[0]:
        entry = float(c.loc[i, "close"])
        atr = float(c.loc[i, "atr"])
        sl = float(max(c.loc[i, "high"], c.loc[i, "range_lo"]) + sl_atr_buffer * atr)
        risk = max(1e-9, sl - entry)
        tp = entry - rr * risk
        entries.append(Entry(
            timestamp=c.loc[i, "timestamp"],
            model="TTS_RETEST",
            side="SHORT",
            entry=entry,
            sl=sl,
            tp=tp,
            meta="TTS_DN retest",
        ))

    # dedupe timestamp+side
    if entries:
        df_e = pd.DataFrame([e.__dict__ for e in entries])
        df_e = _to_ts(df_e, "timestamp").sort_values(["timestamp", "model"]).drop_duplicates(["timestamp", "side"], keep="first")
        entries = [Entry(**row) for row in df_e.to_dict("records")]

    return entries


def _price_at_r(side: str, entry: float, risk: float, r: float) -> float:
    if side == "LONG":
        return entry + r * risk
    return entry - r * risk


def _touches(side: str, h: float, l: float, price: float, kind: str) -> bool:
    # kind: "up" (profit direction) or "down" (loss direction)
    if side == "LONG":
        return (h >= price) if kind == "up" else (l <= price)
    else:
        return (l <= price) if kind == "up" else (h >= price)


def simulate_trades(
    candles: pd.DataFrame,
    entries: List[Entry],
    max_hold_bars: int = 200,
    be_after_r: Optional[float] = None,
    partial_at_r: Optional[float] = None,
    partial_frac: float = 0.0,
) -> pd.DataFrame:
    """
    Simuliuoja trade'ų outcome pagal OHLC + (optional) BE/partials.
    Grąžina:
      - outcome: WIN / LOSS / BE / NO_HIT
      - r_multiple: realized R (su partials/BE)
      - partial_taken, be_moved
    Conservative rule: jei tą pačią žvakę pasiekia keli lygiai, laikom blogiausią eilę.
    """
    if not entries:
        return pd.DataFrame()

    c = candles.copy().sort_values("timestamp").reset_index(drop=True)
    c = _to_ts(c, "timestamp")
    ts_to_idx = {ts: i for i, ts in enumerate(c["timestamp"])}

    be_after_r = None if be_after_r is None else float(be_after_r)
    partial_at_r = None if partial_at_r is None else float(partial_at_r)
    partial_frac = float(partial_frac)

    rows = []
    trade_id = 0

    for e in entries:
        if e.timestamp not in ts_to_idx:
            continue

        trade_id += 1
        start_idx = ts_to_idx[e.timestamp]
        end_idx = min(len(c) - 1, start_idx + max_hold_bars)

        entry = float(e.entry)
        sl0 = float(e.sl)
        tp = float(e.tp)

        risk = abs(entry - sl0)
        if risk <= 0:
            continue

        # state
        sl = sl0
        be_moved = False
        partial_taken = False
        realized_r = 0.0
        remaining = 1.0

        outcome = "NO_HIT"
        exit_price = np.nan
        exit_ts = pd.NaT

        # precompute levels
        be_price = _price_at_r(e.side, entry, risk, be_after_r) if be_after_r is not None else None
        partial_price = _price_at_r(e.side, entry, risk, partial_at_r) if partial_at_r is not None else None

        for j in range(start_idx, end_idx + 1):
            h = float(c.loc[j, "high"])
            l = float(c.loc[j, "low"])

            # ----- conservative ordering inside one candle -----
            # We handle "worst-case" for trader:
            # If SL can be hit in candle, assume it happens before profit events.
            sl_hit_now = _touches(e.side, h, l, sl, kind="down")
            tp_hit_now = _touches(e.side, h, l, tp, kind="up")

            partial_hit_now = False
            if partial_price is not None and (not partial_taken) and partial_frac > 0:
                partial_hit_now = _touches(e.side, h, l, partial_price, kind="up")

            be_hit_now = False
            if be_price is not None and (not be_moved):
                be_hit_now = _touches(e.side, h, l, be_price, kind="up")

            # 1) If SL hit: worst-case exit on SL immediately.
            if sl_hit_now:
                if be_moved and abs(sl - entry) < 1e-12:
                    outcome = "BE"
                    exit_price = entry
                    exit_ts = c.loc[j, "timestamp"]
                    # realized_r unchanged for remaining part
                else:
                    outcome = "LOSS"
                    exit_price = sl
                    exit_ts = c.loc[j, "timestamp"]
                    realized_r += (-1.0) * remaining
                break

            # 2) Partial (if hit) — realize partial, reduce remaining
            if partial_hit_now:
                realized_r += float(partial_at_r) * float(partial_frac)
                remaining = max(0.0, remaining - float(partial_frac))
                partial_taken = True

            # 3) Move BE (if hit) — SL becomes entry (for remaining position)
            if be_hit_now:
                sl = entry
                be_moved = True

            # 4) TP hit (after partial/BE) — realize win on remaining
            if tp_hit_now:
                rr_eff = abs(tp - entry) / risk
                realized_r += rr_eff * remaining
                outcome = "WIN"
                exit_price = tp
                exit_ts = c.loc[j, "timestamp"]
                break

        rows.append({
            "id": trade_id,
            "timestamp": e.timestamp,
            "status": "OK",
            "side": e.side,
            "entry": entry,
            "sl": sl0,
            "tp": tp,
            "meta": f"model={e.model} {e.meta}".strip(),
            "outcome": outcome,
            "exit_price": exit_price,
            "exit_timestamp": exit_ts,
            "be_moved": bool(be_moved),
            "partial_taken": bool(partial_taken),
            "r_multiple": float(realized_r),
        })

    return pd.DataFrame(rows)
