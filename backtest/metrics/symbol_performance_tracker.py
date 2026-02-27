from __future__ import annotations

from pathlib import Path
import pandas as pd


def _derive_r(df: pd.DataFrame) -> pd.Series:
    """Best-effort R derivation (fail-open)."""
    # 1) direct R columns
    for col in ["R", "r", "realized_r", "realized_R", "pnl_r"]:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # 2) outcome + rr
    if "outcome" in df.columns:
        out = df["outcome"].astype(str).str.upper()
        rr = pd.to_numeric(df["rr"], errors="coerce").fillna(0.0) if "rr" in df.columns else pd.Series(1.0, index=df.index)
        R = pd.Series(0.0, index=df.index, dtype=float)
        R[out.isin(["WIN", "TP", "TP_HIT"])] = rr[out.isin(["WIN", "TP", "TP_HIT"])].astype(float)
        R[out.isin(["LOSS", "SL", "SL_HIT"])] = -1.0
        R[out.isin(["BE", "BREAKEVEN"])] = 0.0
        return R

    return pd.Series(0.0, index=df.index, dtype=float)


def update_symbol_performance(
    trades_csv: str | Path = "backtest/journal/trades.csv",
    out_csv: str | Path = "backtest/journal/exports_live/symbol_performance.csv",
) -> None:
    """Compute per-symbol performance (fail-open).

    Output schema: symbol,trades,R_sum,winrate
    """
    p_in = Path(trades_csv)
    p_out = Path(out_csv)
    p_out.parent.mkdir(parents=True, exist_ok=True)

    if not p_in.exists():
        # write empty header (so downstream tooling can always read)
        pd.DataFrame(columns=["symbol", "trades", "R_sum", "winrate"]).to_csv(p_out, index=False)
        return

    # robust read (legacy file may have malformed lines)
    try:
        df = pd.read_csv(p_in, engine="python", on_bad_lines="skip")
    except Exception:
        pd.DataFrame(columns=["symbol", "trades", "R_sum", "winrate"]).to_csv(p_out, index=False)
        return

    if df is None or df.empty:
        pd.DataFrame(columns=["symbol", "trades", "R_sum", "winrate"]).to_csv(p_out, index=False)
        return

    # symbol fallback
    if "symbol" not in df.columns:
        df["symbol"] = "GLOBAL"
    df["symbol"] = df["symbol"].astype(str).fillna("GLOBAL").str.upper().str.strip()
    df.loc[df["symbol"] == "", "symbol"] = "GLOBAL"

    # derive R
    R = _derive_r(df)

    tmp = pd.DataFrame({"symbol": df["symbol"], "R": pd.to_numeric(R, errors="coerce").fillna(0.0)})

    g = tmp.groupby("symbol", dropna=False)

    out = pd.DataFrame({
        "symbol": g.size().index.astype(str),
        "trades": g.size().values.astype(int),
        "R_sum": g["R"].sum().values.astype(float),
        "winrate": (g["R"].apply(lambda s: float((s > 0).mean()) if len(s) else 0.0)).values.astype(float),
    })

    # stable sort
    out = out.sort_values(["trades", "R_sum"], ascending=[False, False]).reset_index(drop=True)

    out.to_csv(p_out, index=False)
