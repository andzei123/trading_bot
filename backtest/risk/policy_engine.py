from __future__ import annotations

from typing import Any

import pandas as pd


def evaluate_policy_asset(
    *,
    symbol: str,
    risk_guard_status: str,
    risk_guard_action: str,
) -> dict[str, Any]:
    """
    Thin wrapper for symbol / asset-specific risk-guard decision.

    EXACT extraction from runner.
    DO NOT change semantics.
    """
    try:
        status = str(risk_guard_status)
        action = str(risk_guard_action)
        status_u = status.upper()

        return {
            "symbol": str(symbol).upper(),
            "status": status,
            "action": action,
            "log": bool(status_u in ("DEFENSIVE", "OFF")),
            "block_new_signals": bool(status_u == "OFF" and action.lower() == "off"),
            "defensive": bool(status_u == "DEFENSIVE"),
        }

    except Exception:
        # fail-open — keep runner behavior
        return {
            "symbol": str(symbol).upper(),
            "status": str(risk_guard_status),
            "action": str(risk_guard_action),
            "log": False,
            "block_new_signals": False,
            "defensive": False,
        }

def evaluate_policy_portfolio(
    df_e: pd.DataFrame,
    *,
    symbol: str,
    risk_guard_status: str,
    risk_guard_action: str,
    portfolio_state_path: str,
    emit_n: int,
    bybit_interval: Any,
    build_portfolio_cfg: Any,
    load_portfolio_state: Any,
    portfolio_state_cls: Any,
    filter_signals_portfolio_fn: Any,
) -> dict[str, Any]:
    """
    Thin wrapper for runner-owned portfolio policy decision boundary.

    Exact extraction of portfolio-level allow/block/throttle setup.
    DO NOT change semantics.
    """
    try:
        cfg = build_portfolio_cfg()

        asset_policy = evaluate_policy_asset(
            symbol=str(symbol),
            risk_guard_status=str(risk_guard_status),
            risk_guard_action=str(risk_guard_action),
        )

        if asset_policy["defensive"]:
            cfg.max_signals_per_cycle = min(int(getattr(cfg, "max_signals_per_cycle", 1)), 1)
            cfg.per_symbol_cooldown_candles = max(int(getattr(cfg, "per_symbol_cooldown_candles", 0)), 12)
            cfg.max_1_signal_per_candle_per_symbol = True

        state_path_use = str(portfolio_state_path)
        if emit_n > 1:
            if state_path_use.endswith(".json"):
                state_path_use = state_path_use[:-5] + "_backfill.json"
            else:
                state_path_use = state_path_use + "_backfill.json"

            cfg.per_symbol_cooldown_candles = 0
            cfg.max_1_signal_per_candle_per_symbol = False
            cfg.max_signals_per_cycle = max(int(getattr(cfg, "max_signals_per_cycle", 1)), len(df_e))

        backfill_mode = emit_n > 1
        if backfill_mode:
            state = portfolio_state_cls(state_path_use)
        else:
            state = load_portfolio_state(state_path_use)

        if "symbol" not in df_e.columns:
            df_e = df_e.copy()
            df_e["symbol"] = symbol

        before_portfolio = len(df_e)
        df_e = filter_signals_portfolio_fn(
            signals_df=df_e,
            cfg=cfg,
            state=state,
            bybit_interval_min=int(bybit_interval),
        )

        return {
            "ok": True,
            "df_e": df_e,
            "cfg": cfg,
            "asset_policy": asset_policy,
            "state": state,
            "backfill_mode": bool(backfill_mode),
            "state_path_use": str(state_path_use),
            "before_portfolio": int(before_portfolio),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
        }



def evaluate_policy_kill_switch(
    *,
    symbol: str,
    kill_threshold_r: float,
    btc_kill_threshold_r: float,
    kill_window_days: int,
    kill_trades_csv: str,
    out_csv: str,
) -> dict[str, Any]:
    """
    Thin wrapper for kill-switch decision (Stage 3 Step 3).
    DO NOT change semantics.
    """
    from backtest.live.kill_switch import rolling_r_guard

    try:
        ks_symbol = str(symbol).upper()

        ks_threshold_r = (
            float(btc_kill_threshold_r)
            if ks_symbol == "BTCUSDT"
            else float(kill_threshold_r)
        )

        ks = rolling_r_guard(
            trades_csv=str(kill_trades_csv) if str(kill_trades_csv).strip() else str(out_csv),
            threshold_r=ks_threshold_r,
            window_days=int(kill_window_days),
            symbols=[ks_symbol],
        )

        return {
            "ok": bool(ks.ok),
            "reason": str(ks.reason),
            "symbol": ks_symbol,
            "threshold_used": float(ks_threshold_r),
            "error": None,
        }

    except Exception as e:
        return {
            "ok": True,  # fail-open (IMPORTANT: unchanged behavior)
            "reason": "",
            "symbol": str(symbol).upper(),
            "threshold_used": None,
            "error": repr(e),
        }

def evaluate_policy_sizing(
    df_e: pd.DataFrame,
    *,
    context_risk_multiplier: float,
    equity_governor: Any,
) -> dict[str, Any]:
    """
    Exact extraction of runner equity-governor sizing block.

    DO NOT change semantics.
    """

    df_e = df_e.copy()

    # 1) stamp context multiplier (unchanged)
    df_e["risk_multiplier"] = float(context_risk_multiplier)

    # 2) derive rm_before (unchanged)
    try:
        rm_before = float(df_e["risk_multiplier"].astype(float).iloc[0]) if len(df_e) else 1.0
    except Exception:
        rm_before = 1.0

    # 3) apply governor (unchanged)
    try:
        rm_after, eg = equity_governor.apply(rm_before)

        df_e["risk_multiplier"] = float(rm_after)
        df_e["equity_dd"] = float(eg.dd)
        df_e["equity_governor_multiplier"] = float(eg.multiplier)

        return {
            "df_e": df_e,
            "logged": True,
            "equity_dd": float(eg.dd),
            "equity_governor_multiplier": float(eg.multiplier),
        }

    except Exception:
        # fail-open — EXACT same behavior
        df_e["equity_dd"] = 0.0
        df_e["equity_governor_multiplier"] = 1.0

        return {
            "df_e": df_e,
            "logged": False,
        }

def evaluate_policy_has_open(
    df_tr: pd.DataFrame,
    *,
    symbol: str,
    allow_multiple: bool = False,
) -> dict[str, Any]:
    has_open = False
    try:
        has_open = (
            (df_tr["status"].astype(str).str.upper() == "OPEN")
            & (df_tr["symbol"].astype(str) == str(symbol))
        ).any()
    except Exception:
        has_open = False

    return {
        "has_open": bool(has_open),
        "block": bool(has_open and (not allow_multiple)),
        "reason": "HAS_OPEN" if (has_open and (not allow_multiple)) else "",
    }


def evaluate_policy_corr_cap(
    df_e: pd.DataFrame,
    *,
    portfolio_state_path: str,
    base_risk: float = 0.002,
    cap_btc: float = 0.02,
    cap_alt: float = 0.02,
    cap_meme: float = 0.01,
) -> dict[str, Any]:
    from backtest.portfolio.portfolio_exposure import load_portfolio_exposure

    corr_btc_used = 0.0
    corr_alt_used = 0.0
    corr_meme_used = 0.0

    try:
        exp = load_portfolio_exposure(portfolio_state_path)
        bu = exp.get("bucket_used", {}) or {}
        corr_btc_used = float(bu.get("BTC", 0.0) or 0.0)
        corr_alt_used = float(bu.get("ALT", 0.0) or 0.0)
        corr_meme_used = float(bu.get("MEME", 0.0) or 0.0)
    except Exception:
        pass

    kept_rows: list[Any] = []
    dropped_rows: list[Any] = []

    if df_e is not None and not df_e.empty:
        for _, r in df_e.iterrows():
            symbol = str(r.get("symbol", "")).upper()

            rm_raw = r.get("risk_multiplier", 1.0)

            try:
                rm = float(rm_raw)
            except Exception:
                rm = 1.0

            if rm != rm or rm == float("inf") or rm == float("-inf"):
                rm = 1.0

            plan_risk = float(base_risk * rm)

            if plan_risk != plan_risk or plan_risk == float("inf") or plan_risk == float("-inf"):
                plan_risk = 0.0

            if symbol.startswith("BTC"):
                bucket = "BTC"
                used = corr_btc_used
                cap = cap_btc
            elif symbol in ["DOGEUSDT", "PEPEUSDT", "WIFUSDT"]:
                bucket = "MEME"
                used = corr_meme_used
                cap = cap_meme
            else:
                bucket = "ALT"
                used = corr_alt_used
                cap = cap_alt

            try:
                used = float(used) if used is not None else 0.0
            except Exception:
                used = 0.0
            try:
                cap = float(cap) if cap is not None else 0.0
            except Exception:
                cap = 0.0
            if used != used:
                used = 0.0
            if cap != cap:
                cap = 0.0

            would = used + plan_risk

            if would > cap * 1.2:
                dropped_rows.append(r)
                continue

            if would > cap:
                new_rm = rm * 0.25
                r["risk_multiplier"] = new_rm
                plan_risk = base_risk * new_rm

            if bucket == "BTC":
                corr_btc_used += plan_risk
            elif bucket == "ALT":
                corr_alt_used += plan_risk
            else:
                corr_meme_used += plan_risk

            kept_rows.append(r)

        df_kept = pd.DataFrame(kept_rows)
        df_drop = pd.DataFrame(dropped_rows)
    else:
        df_kept = df_e
        df_drop = df_e

    return {
        "df_kept": df_kept,
        "df_drop": df_drop,
    }


def evaluate_policy_budget(
    df_e: pd.DataFrame,
    *,
    base_risk_per_trade: float,
    bucket_cap: float,
    global_cap: float,
) -> dict[str, Any]:
    long_used = 0.0
    range_used = 0.0
    short_used = 0.0
    global_used = 0.0

    kept_rows: list[Any] = []
    dropped_rows: list[Any] = []

    if df_e is not None and not df_e.empty:
        if "risk_multiplier" in df_e.columns:
            rm = pd.to_numeric(df_e["risk_multiplier"], errors="coerce").fillna(1.0)
        else:
            rm = pd.Series(1.0, index=df_e.index)

        if "dynamic_multiplier" in df_e.columns:
            dm = pd.to_numeric(df_e["dynamic_multiplier"], errors="coerce").fillna(1.0)
        else:
            dm = pd.Series(1.0, index=df_e.index)

        if "equity_governor_multiplier" in df_e.columns:
            egm = pd.to_numeric(df_e["equity_governor_multiplier"], errors="coerce").fillna(1.0)
        else:
            egm = pd.Series(1.0, index=df_e.index)

        try:
            df_e = df_e.copy()
            df_e["plan_risk"] = float(base_risk_per_trade) * rm.astype(float) * (
                dm.astype(float) if hasattr(dm, "astype") else float(dm)
            ) * (
                egm.astype(float) if hasattr(egm, "astype") else float(egm)
            )
        except Exception:
            df_e = df_e.copy()
            df_e["plan_risk"] = float(base_risk_per_trade) * rm.astype(float)

        for _, r in df_e.iterrows():
            side = str(r.get("side", "")).upper()
            plan_risk = float(r.get("plan_risk", base_risk_per_trade) or base_risk_per_trade)

            if side == "LONG":
                if (long_used + plan_risk > bucket_cap) or (global_used + plan_risk > global_cap):
                    dropped_rows.append(r)
                    continue
                long_used += plan_risk

            elif side == "SHORT":
                if (short_used + plan_risk > bucket_cap) or (global_used + plan_risk > global_cap):
                    dropped_rows.append(r)
                    continue
                short_used += plan_risk

            else:
                if (range_used + plan_risk > bucket_cap) or (global_used + plan_risk > global_cap):
                    dropped_rows.append(r)
                    continue
                range_used += plan_risk

            global_used += plan_risk
            kept_rows.append(r)

        df_kept = pd.DataFrame(kept_rows)
        df_drop = pd.DataFrame(dropped_rows)
    else:
        df_kept = df_e
        df_drop = df_e

    return {
        "df_kept": df_kept,
        "df_drop": df_drop,
        "long_used": float(long_used),
        "range_used": float(range_used),
        "short_used": float(short_used),
        "global_used": float(global_used),
    }
