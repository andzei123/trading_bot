from __future__ import annotations

"""Production decision pipeline core (1:1 for live + offline).

DEV4 PHASE-1

This module provides a single function :func:`run_pipeline_once` that executes
the *production* decision layer for one symbol at one timestamp.

Guardrail:
  - live_signal_runner.py and offline_live_runner_backtest.py are orchestrators.
  - All decision/filter/cap logic changes must happen here.

Fail-open:
  - Any exception results in an empty dataframe with guaranteed schema.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

def _series_col_or_default(df: pd.DataFrame, col: str, default: float) -> pd.Series:
    """Return numeric Series for df[col] or a constant Series(default) aligned to df.index."""
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if col in df.columns:
        s = pd.to_numeric(df[col], errors="coerce")
        return s.fillna(float(default))
    return pd.Series([float(default)] * len(df), index=df.index, dtype=float)


from backtest.filters.signal_cluster_filter import apply_signal_cluster_filter
from backtest.live.phase_router import decide_phase
from backtest.risk.portfolio_correlation_caps import _bucket as _corr_bucket  # type: ignore


# ------------------------------
# Stable schema (mirrors live_signal_runner)
# ------------------------------

LIVE_ENTRIES_COLUMNS = [
    "timestamp",
    "signal_ts",
    "model",
    "side",
    "entry",
    "sl",
    "tp",
    "rr",
    "ctx_sub_label",
    "regime",
    "trend_dir",
    "trend_strength",
    "atr_pct",
    "phase",
    "symbol",
    "liq_bias",
    "liq_risk_multiplier",
    "risk_multiplier",
    "block_reason",
    "context_allow",
    "macro_allow",
    "macro_reason",
    "macro_bias",
    "macro_bias_mismatch",
    "news_allow",
    "news_reason",
    "liq_allow",
    "liq_reason",
    "freeze_new_signals",
    "setup_age_hours",
    "setup_age_candles",
]


def _empty_entries_df() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in LIVE_ENTRIES_COLUMNS})


def _safe_to_datetime_utc(s) -> pd.Series:
    return pd.to_datetime(s, utc=True, errors="coerce")


def _entries_to_df(entries: list[Any], *, symbol: str) -> pd.DataFrame:
    """Normalize entry objects/dicts into a dataframe."""

    if not entries:
        df = _empty_entries_df()
        df["symbol"] = df["symbol"].astype("object")
        return df

    rows = []
    for e in entries:
        if isinstance(e, dict):
            get = e.get
        else:
            get = lambda k, default=None: getattr(e, k, default)
        ts = get("timestamp", None) or get("ts", None) or get("time", None)
        rows.append(
            {
                "timestamp": ts,
                "signal_ts": get("signal_ts", None),
                "model": get("model", ""),
                "side": get("side", ""),
                "entry": get("entry", None),
                "sl": get("sl", None),
                "tp": get("tp", None),
                "rr": get("rr", None),
                "ctx_sub_label": get("ctx_sub_label", get("sub_label", None)),
                "regime": get("regime", None),
                "trend_dir": get("trend_dir", None),
                "trend_strength": get("trend_strength", None),
                "atr_pct": get("atr_pct", None),
                "phase": get("phase", None),
                "symbol": symbol,
                "liq_bias": get("liq_bias", None),
                "liq_risk_multiplier": get("liq_risk_multiplier", None),
                "risk_multiplier": get("risk_multiplier", None),
                "block_reason": get("block_reason", None),
                "context_allow": get("context_allow", None),
                "macro_allow": get("macro_allow", None),
                "macro_reason": get("macro_reason", None),
                "macro_bias": get("macro_bias", None),
                "macro_bias_mismatch": get("macro_bias_mismatch", None),
                "news_allow": get("news_allow", None),
                "news_reason": get("news_reason", None),
                "liq_allow": get("liq_allow", None),
                "liq_reason": get("liq_reason", None),
                "freeze_new_signals": get("freeze_new_signals", None),
                "setup_age_hours": get("setup_age_hours", None),
                "setup_age_candles": get("setup_age_candles", None),
            }
        )

    df = pd.DataFrame(rows)
    # Guarantee columns
    for c in LIVE_ENTRIES_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[LIVE_ENTRIES_COLUMNS]
    # timestamps
    df["timestamp"] = _safe_to_datetime_utc(df["timestamp"])
    if "signal_ts" in df.columns:
        df["signal_ts"] = _safe_to_datetime_utc(df["signal_ts"])
    return df


def _invalidate_setups_hit_tp_sl(
    df_e: pd.DataFrame, candles: pd.DataFrame, latest_ts: pd.Timestamp
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Copy of live invalidation guard (conservative OHLC TP/SL check).

    Returns (df_active, df_closed).
    """
    if df_e is None or df_e.empty:
        return df_e, pd.DataFrame()
    if candles is None or candles.empty:
        return df_e, pd.DataFrame()

    c = candles.copy()
    if "timestamp" not in c.columns or "high" not in c.columns or "low" not in c.columns:
        return df_e, pd.DataFrame()

    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if pd.isna(latest_ts):
        return df_e, pd.DataFrame()

    df = df_e.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    for col in ("entry", "sl", "tp"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["setup_status"] = "ACTIVE"
    df["setup_close_reason"] = ""
    df["setup_entry_touch_ts"] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
    df["setup_close_ts"] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")

    closed_rows = []

    for idx, row in df.iterrows():
        setup_ts = row.get("timestamp")
        if pd.isna(setup_ts):
            continue
        side = str(row.get("side", "")).upper().strip()
        entry = row.get("entry")
        sl = row.get("sl")
        tp = row.get("tp")
        if not np.isfinite(entry) or not np.isfinite(sl) or not np.isfinite(tp):
            continue

        # consider candles strictly after setup_ts
        post = c[c["timestamp"] > setup_ts]
        if post.empty:
            continue

        entry_touch_ts: Optional[pd.Timestamp] = None
        close_ts: Optional[pd.Timestamp] = None
        close_reason: str = ""

        # 1) find entry touch
        for _, r in post.iterrows():
            if float(r["low"]) <= float(entry) <= float(r["high"]):
                entry_touch_ts = pd.Timestamp(r["timestamp"])
                break
        if entry_touch_ts is None:
            continue

        # 2) after entry touch: detect SL/TP hit
        post2 = post[post["timestamp"] >= entry_touch_ts]
        for _, r in post2.iterrows():
            h = float(r["high"])
            l = float(r["low"])
            ts = pd.Timestamp(r["timestamp"])

            if side == "LONG":
                sl_hit = l <= float(sl)
                tp_hit = h >= float(tp)
                if sl_hit and tp_hit:
                    close_reason = "SL_HIT"
                    close_ts = ts
                    break
                if sl_hit:
                    close_reason = "SL_HIT"
                    close_ts = ts
                    break
                if tp_hit:
                    close_reason = "TP_HIT"
                    close_ts = ts
                    break
            else:  # SHORT
                sl_hit = h >= float(sl)
                tp_hit = l <= float(tp)
                if sl_hit and tp_hit:
                    close_reason = "SL_HIT"
                    close_ts = ts
                    break
                if sl_hit:
                    close_reason = "SL_HIT"
                    close_ts = ts
                    break
                if tp_hit:
                    close_reason = "TP_HIT"
                    close_ts = ts
                    break

        if close_ts is None:
            continue

        df.at[idx, "setup_status"] = "CLOSED"
        df.at[idx, "setup_close_reason"] = close_reason
        df.at[idx, "setup_entry_touch_ts"] = entry_touch_ts
        df.at[idx, "setup_close_ts"] = close_ts
        closed_rows.append(idx)

    if not closed_rows:
        return df, pd.DataFrame()

    df_closed = df.loc[closed_rows].copy()
    df_active = df.drop(index=closed_rows).copy()
    return df_active, df_closed


def run_pipeline_once(
    *,
    symbol: str,
    candles_df: pd.DataFrame,
    ctx: Dict[str, Any],
    portfolio_state: Dict[str, Any],
    debug: bool = False,
) -> pd.DataFrame:
    """Run the production decision pipeline for one symbol.

    Parameters
    ----------
    symbol:
        Trading symbol, e.g. BTCUSDT.
    candles_df:
        Historical candle window (must include timestamp/open/high/low/close).
    ctx:
        Context snapshot (macro, regime, liq, etc.). Must include at least:
          - latest_ts (pd.Timestamp or str)
          - bybit_interval (minutes as str/int) for age calculations (optional)
          - macro_bias (optional)
          - risk_multiplier (optional)
          - freeze_new_signals (optional)
    portfolio_state:
        Portfolio exposure snapshot. Must include bucket_used.
    debug:
        Print extra logs.
    """
    try:
        if candles_df is None or candles_df.empty:
            return _empty_entries_df()

        candles = candles_df.copy()
        candles["timestamp"] = pd.to_datetime(candles["timestamp"], utc=True, errors="coerce")
        candles = candles.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        if candles.empty:
            return _empty_entries_df()

        latest_ts = pd.to_datetime(ctx.get("latest_ts", candles["timestamp"].iloc[-1]), utc=True, errors="coerce")
        if pd.isna(latest_ts):
            latest_ts = candles["timestamp"].iloc[-1]

        # -------------------
        # PHASE_PRE
        # -------------------
        macro_bias = str(ctx.get("macro_bias", "NEUTRAL") or "NEUTRAL").upper()
        ph_pre = decide_phase(candles=candles, macro_bias=macro_bias)
        ctx["phase"] = ph_pre

        # Prefer PhaseDecision.atr_pct (works for offline too); fallback to candles column if present
        atrp = getattr(ph_pre, "atr_pct", None)
        if atrp is None:
            try:
                atrp = float(candles["atr_pct"].iloc[-1]) if "atr_pct" in candles.columns else np.nan
            except Exception:
                atrp = np.nan

        # normalize ctx for downstream telemetry
        try:
            if atrp is not None and np.isfinite(float(atrp)):
                ctx["atr_pct"] = float(atrp)
        except Exception:
            pass

        print(f"[PHASE_PRE][{symbol}] {ph_pre} | macro_bias={macro_bias} atr%={atrp}")
        # -------------------
        # ENTRY GENERATION
        # -------------------
        try:
            from backtest.engine.entry_model import generate_entries_from_ctx
        except Exception:
            # fallback for alt layout
            from backtest.engine import entry_model as _em  # type: ignore

            generate_entries_from_ctx = _em.generate_entries_from_ctx  # type: ignore

        # build full ctx df for entry model: use journal.filter_trades ctx builder (same as live)
        import backtest.journal.filter_trades as ft

        ctx_df = ft.build_ctx(candles)
        # inject macro fields used by entry_model
        for k in ("macro_bias", "macro_phase", "macro_strength", "cross_asset_regime", "cross_asset_reason"):
            if k in ctx:
                try:
                    ctx_df[k] = ctx[k]
                except Exception:
                    pass
        try:
            ctx_df["phase"] = ph_pre
        except Exception:
            pass

        entries = generate_entries_from_ctx(ctx_df)
        print(f"[ENTRY_DIAG][{symbol}] raw_entries={len(entries)}")


        # -------------------
        # DEBUG FORCE (pipeline-level) — to exercise corr/budget/execution layers
        # -------------------
        if debug and bool(ctx.get("debug_force_entries", False)) and (not entries):
            try:
                last = candles.iloc[-1]
                setup_ts = last["timestamp"]
                close = float(last["close"])

                forced = []
                for side in ("LONG", "SHORT"):
                    forced.append(
                        {
                            "timestamp": setup_ts,
                            "model": "DEBUG_FORCED",
                            "side": side,
                            "entry": close,
                            "sl": close * (0.99 if side == "LONG" else 1.01),
                            "tp": close * (1.02 if side == "LONG" else 0.98),
                            "rr": 2.0,
                            "score": 0.5,
                            "notes": "pipeline_debug_force",
                            # optional: keep these consistent with downstream expectations
                            "risk_multiplier": 1.0,
                            "atr_pct": ctx.get("atr_pct", np.nan),
                            "phase": str(ctx.get("phase", "")),
                        }
                    )

                entries = forced
                print(f"[DEBUG_FORCE_PIPELINE][{symbol}] forced_entries={len(entries)} ts={setup_ts} close={close:.4f}")
            except Exception as e:
                print(f"[DEBUG_FORCE_PIPELINE][{symbol}] failed: {e}")

                
        # -------------------
        # CLUSTER FILTER (works on entries list)
        # -------------------
        try:
            kept_entries, dropped_entries = apply_signal_cluster_filter(
                entries,
                max_per_group=int(ctx.get("cluster_max_per_group", 2) or 2),
                score=str(ctx.get("cluster_score", "RR") or "RR"),
                phase=str(ph_pre or "") or None,
            )
            if debug:
                print(f"[CLUSTER_FILTER][{symbol}] kept={len(kept_entries)} dropped={len(dropped_entries)}")
            entries = kept_entries
        except Exception:
            pass

        df_e = _entries_to_df(entries, symbol=symbol)
        if df_e.empty:
            return _empty_entries_df()

        # ensure phase column is filled
        if "phase" in df_e.columns:
            df_e["phase"] = df_e["phase"].fillna(ph_pre)
        else:
            df_e["phase"] = ph_pre

        # -------------------
        # ENTRY QUALITY FILTERS (placeholder: keep as-is; live filters already encoded upstream)
        # -------------------

        # -------------------
        # CORR_CAP (SOFT + DEBUG) — 1:1 with live
        # -------------------
        CAP_BTC = float(ctx.get("cap_btc", 0.02) or 0.02)
        CAP_ALT = float(ctx.get("cap_alt", 0.02) or 0.02)
        CAP_MEME = float(ctx.get("cap_meme", 0.01) or 0.01)
        BASE_RISK = float(ctx.get("base_risk", 0.002) or 0.002)

        bu = (portfolio_state or {}).get("bucket_used", {}) or {}
        corr_btc_used = float(bu.get("BTC", 0.0) or 0.0)
        corr_alt_used = float(bu.get("ALT", 0.0) or 0.0)
        corr_meme_used = float(bu.get("MEME", 0.0) or 0.0)

        try:
            print(f"[PORTFOLIO_EXPOSURE] bucket_used={bu}")
        except Exception:
            pass

        df_corr_kept = []
        df_corr_dropped = []

        for _, r in df_e.iterrows():
            sym = str(r.get("symbol", "") or "").upper()

            rm_raw = r.get("risk_multiplier", 1.0)
            try:
                rm = float(rm_raw)
            except Exception:
                rm = 1.0
            if rm != rm or rm in (float("inf"), float("-inf")):
                rm = 1.0

            plan_risk = float(BASE_RISK * rm)
            if plan_risk != plan_risk or plan_risk in (float("inf"), float("-inf")):
                plan_risk = 0.0

            bucket = _corr_bucket(sym)
            if bucket == "BTC":
                used = corr_btc_used
                cap = CAP_BTC
            elif bucket == "MEME":
                used = corr_meme_used
                cap = CAP_MEME
            else:
                used = corr_alt_used
                cap = CAP_ALT

            try:
                used = float(used or 0.0)
            except Exception:
                used = 0.0
            try:
                cap = float(cap or 0.0)
            except Exception:
                cap = 0.0
            if used != used:
                used = 0.0
            if cap != cap:
                cap = 0.0

            would = used + plan_risk

            print(
                f"[CORR_CAP_DEBUG] cap_btc={CAP_BTC:.4f} cap_alt={CAP_ALT:.4f} cap_meme={CAP_MEME:.4f} "
                f"used_btc={corr_btc_used:.4f} used_alt={corr_alt_used:.4f} used_meme={corr_meme_used:.4f} "
                f"plan_risk={plan_risk:.4f} bucket={bucket} would={would:.4f}"
            )

            # HARD DROP only if > 1.2x cap
            if cap > 0 and would > cap * 1.2:
                df_corr_dropped.append(r)
                continue

            # SOFT CAP: if over cap but <= 1.2x → throttle
            if cap > 0 and would > cap:
                new_rm = rm * 0.25
                try:
                    r = r.copy()
                except Exception:
                    pass
                r["risk_multiplier"] = new_rm
                plan_risk = float(BASE_RISK * new_rm)
                print(f"[CORR_CAP_SOFT] bucket={bucket} applied_multiplier=0.25")

            # update exposure
            if bucket == "BTC":
                corr_btc_used += plan_risk
            elif bucket == "MEME":
                corr_meme_used += plan_risk
            else:
                corr_alt_used += plan_risk

            df_corr_kept.append(r)

        df_e = pd.DataFrame(df_corr_kept)
        df_corr_dropped = pd.DataFrame(df_corr_dropped)
        print(f"[CORR_CAP][{symbol}] kept={len(df_e)} dropped={len(df_corr_dropped)}")

        # -------------------
        # BUDGET_CAP (1:1 with live)
        # -------------------
        BASE_RISK_PER_TRADE = float(ctx.get("base_risk_per_trade", 0.002) or 0.002)
        BUCKET_CAP = float(ctx.get("bucket_cap", 0.006) or 0.006)
        GLOBAL_CAP = float(ctx.get("global_cap", 0.012) or 0.012)

        long_used = 0.0
        range_used = 0.0
        short_used = 0.0
        global_used = 0.0

        kept_rows = []
        dropped_rows = []

        if df_e is not None and not df_e.empty:
            if "risk_multiplier" in df_e.columns:
                rm = pd.to_numeric(df_e["risk_multiplier"], errors="coerce").fillna(1.0)
            else:
                rm = pd.Series(1.0, index=df_e.index)

            dm = _series_col_or_default(df_e, "dynamic_multiplier", 1.0)
            egm = _series_col_or_default(df_e, "equity_governor_multiplier", 1.0)

            try:
                df_e["plan_risk"] = float(BASE_RISK_PER_TRADE) * rm.astype(float) * dm.astype(float) * egm.astype(float)
            except Exception:
                df_e["plan_risk"] = float(BASE_RISK_PER_TRADE) * rm.astype(float)

            for _, r in df_e.iterrows():
                side = str(r.get("side", "") or "").upper()
                plan_risk = float(r.get("plan_risk", BASE_RISK_PER_TRADE) or BASE_RISK_PER_TRADE)

                if side == "LONG":
                    if (long_used + plan_risk > BUCKET_CAP) or (global_used + plan_risk > GLOBAL_CAP):
                        dropped_rows.append(r)
                        continue
                    long_used += plan_risk
                elif side == "SHORT":
                    if (short_used + plan_risk > BUCKET_CAP) or (global_used + plan_risk > GLOBAL_CAP):
                        dropped_rows.append(r)
                        continue
                    short_used += plan_risk
                else:
                    if (range_used + plan_risk > BUCKET_CAP) or (global_used + plan_risk > GLOBAL_CAP):
                        dropped_rows.append(r)
                        continue
                    range_used += plan_risk

                global_used += plan_risk
                kept_rows.append(r)

            df_kept = pd.DataFrame(kept_rows)
            df_drop = pd.DataFrame(dropped_rows)
            df_e = df_kept

            print(
                f"[BUDGET][{symbol}] kept={len(df_kept)} dropped={len(df_drop)} "
                f"long_used={long_used:.4f} range_used={range_used:.4f} short_used={short_used:.4f} global_used={global_used:.4f}"
            )
        else:
            print(
                f"[BUDGET][{symbol}] kept=0 dropped=0 "
                f"long_used={long_used:.4f} range_used={range_used:.4f} short_used={short_used:.4f} global_used={global_used:.4f}"
            )

        # -------------------
        # INVALIDATION (TP/SL hit already)
        # -------------------
        try:
            df_e, _df_closed = _invalidate_setups_hit_tp_sl(df_e, candles, latest_ts)
            # Keep logs consistent with live (proof hook)
            if debug and _df_closed is not None and (not _df_closed.empty):
                print(f"[INVALIDATION][{symbol}] closed={len(_df_closed)} kept={len(df_e)}")
        except Exception:
            pass

        # -------------------
        # FINAL NORMALIZE
        # -------------------
        for c in LIVE_ENTRIES_COLUMNS:
            if c not in df_e.columns:
                df_e[c] = np.nan
        df_e = df_e.reindex(columns=LIVE_ENTRIES_COLUMNS)
        df_e["symbol"] = symbol
        # Use signal_ts for stable schema
        if "signal_ts" not in df_e.columns or df_e["signal_ts"].isna().all():
            df_e["signal_ts"] = latest_ts
        return df_e

    except Exception as e:
        if debug:
            print(f"[PIPELINE_CORE][{symbol}] fail-open exception: {repr(e)}")
        return _empty_entries_df()