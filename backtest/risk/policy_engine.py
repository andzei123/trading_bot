from __future__ import annotations

from typing import Any

import pandas as pd


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
