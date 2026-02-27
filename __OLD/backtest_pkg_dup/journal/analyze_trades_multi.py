from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


CANON_OUTCOMES = {"WIN", "LOSS", "BE", "NO_HIT"}


def _to_utc(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")


def _infer_outcome(df: pd.DataFrame) -> pd.Series:
    """Canonicalize outcome.

    Prefers df['outcome'] if present, else derives from R.
    """
    if "outcome" in df.columns:
        o = df["outcome"].astype(str).str.upper().str.strip()
        # If already canonical, keep, else try to map common variants
        mapped = o.replace(
            {
                "BREAKEVEN": "BE",
                "BREAK_EVEN": "BE",
                "BREAK-EVEN": "BE",
                "NOHIT": "NO_HIT",
                "NO HIT": "NO_HIT",
                "NONE": "NO_HIT",
                "N/A": "NO_HIT",
                "": "NO_HIT",
                "NAN": "NO_HIT",
            }
        )
        # Anything unknown -> derive from R if possible
        unknown = ~mapped.isin(list(CANON_OUTCOMES))
        if unknown.any() and "R" in df.columns:
            r = pd.to_numeric(df.loc[unknown, "R"], errors="coerce")
            mapped.loc[unknown] = np.where(
                r.isna(),
                "NO_HIT",
                np.where(r > 1e-12, "WIN", np.where(r < -1e-12, "LOSS", "BE")),
            )
        return mapped.where(mapped.isin(list(CANON_OUTCOMES)), "NO_HIT")

    if "R" in df.columns:
        r = pd.to_numeric(df["R"], errors="coerce")
        return pd.Series(
            np.where(
                r.isna(),
                "NO_HIT",
                np.where(r > 1e-12, "WIN", np.where(r < -1e-12, "LOSS", "BE")),
            ),
            index=df.index,
        )

    return pd.Series("NO_HIT", index=df.index)


def _maxdd_r(trades: pd.DataFrame) -> float:
    """Max drawdown in R units for the sequence."""
    if trades.empty:
        return 0.0
    r = pd.to_numeric(trades["R"], errors="coerce").fillna(0.0).to_numpy()
    curve = np.cumsum(r)
    peak = np.maximum.accumulate(curve)
    dd = curve - peak
    return float(dd.min())  # negative or 0


def _wl_summary(g: pd.DataFrame) -> dict:
    out = _infer_outcome(g)
    r = pd.to_numeric(g.get("R", pd.Series(index=g.index, dtype=float)), errors="coerce")

    total = int(len(g))
    win = int((out == "WIN").sum())
    loss = int((out == "LOSS").sum())
    be = int((out == "BE").sum())
    no_hit = int((out == "NO_HIT").sum())

    wl_den = max(1, win + loss)
    wr = win / wl_den

    exp_r = float(r.mean()) if len(r) else 0.0
    total_r = float(r.sum()) if len(r) else 0.0

    # MaxDD computed on time-sorted slice (if timestamp present)
    if "timestamp" in g.columns:
        gg = g.sort_values("timestamp")
    else:
        gg = g
    maxdd = _maxdd_r(gg)

    return {
        "total": total,
        "win": win,
        "loss": loss,
        "be": be,
        "no_hit": no_hit,
        "winrate_WL": wr,
        "expectancy_R": exp_r,
        "total_R": total_r,
        "maxDD_R": maxdd,
    }


def _group_report(df: pd.DataFrame, by_cols: list[str]) -> pd.DataFrame:
    if not by_cols:
        s = _wl_summary(df)
        return pd.DataFrame([s])

    rows = []
    for keys, g in df.groupby(by_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = {c: k for c, k in zip(by_cols, keys)}
        base.update(_wl_summary(g))
        rows.append(base)
    out = pd.DataFrame(rows)

    # Pretty columns
    if "winrate_WL" in out.columns:
        out["winrate_WL"] = (out["winrate_WL"] * 100.0).round(2)
    for c in ["expectancy_R", "total_R", "maxDD_R"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(4)

    # Sort by total_R desc by default
    sort_cols = ["total_R"]
    for c in sort_cols:
        if c in out.columns:
            out = out.sort_values(c, ascending=False)
            break

    return out.reset_index(drop=True)


def _best_worst_by_period(df: pd.DataFrame, period: str, top_n: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (best, worst) trades per period (and per symbol if available)."""
    if "timestamp" not in df.columns:
        return df.iloc[0:0].copy(), df.iloc[0:0].copy()

    d = df.copy()
    d["period"] = d["timestamp"].dt.to_period(period).astype(str)

    group_cols = ["period"]
    if "symbol" in d.columns:
        group_cols.append("symbol")

    # Keep some useful columns
    keep_cols = [
        c
        for c in [
            "timestamp",
            "symbol",
            "model",
            "side",
            "ctx_sub_label",
            "phase",
            "entry",
            "sl",
            "tp",
            "exit_price",
            "exit_timestamp",
            "exit_reason",
            "R",
            "meta",
        ]
        if c in d.columns
    ]

    d["R"] = pd.to_numeric(d.get("R"), errors="coerce")

    best_rows = []
    worst_rows = []
    for _, g in d.groupby(group_cols, dropna=False):
        gg = g.dropna(subset=["R"]).sort_values("R", ascending=False)
        best_rows.append(gg.head(top_n)[keep_cols + ["period"]])
        worst_rows.append(gg.tail(top_n)[keep_cols + ["period"]].sort_values("R"))

    best = pd.concat(best_rows, ignore_index=True) if best_rows else d.iloc[0:0].copy()
    worst = pd.concat(worst_rows, ignore_index=True) if worst_rows else d.iloc[0:0].copy()

    # Order columns: period first
    cols = ["period"] + [c for c in best.columns if c != "period"]
    best = best[cols]
    worst = worst[cols]

    return best, worst


def main() -> int:
    p = argparse.ArgumentParser(description="Analyze trades_simulated.csv and output per-symbol/model/side/phase reports.")
    p.add_argument("--trades", type=str, required=True, help="Path to trades_simulated.csv (or combined multi-symbol trades CSV)")
    p.add_argument("--out_dir", type=str, default="backtest/journal/exports_reports", help="Directory to write report CSVs")
    p.add_argument("--period", type=str, default="M", help="Pandas Period alias for best/worst tables: M=month, W=week, Q=quarter")
    p.add_argument("--top_n", type=int, default=10, help="Top N best/worst trades per period")

    args = p.parse_args()

    trades_path = Path(args.trades)
    if not trades_path.exists():
        raise SystemExit(f"Trades file not found: {trades_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(trades_path)

    # Normalize
    _to_utc(df, "timestamp")
    _to_utc(df, "exit_timestamp")

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    else:
        df["symbol"] = "UNKNOWN"

    for c in ["model", "side", "ctx_sub_label", "phase", "regime", "trend_dir"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.upper().str.strip()

    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # Core reports
    reports = {
        "summary_total.csv": _group_report(df, []),
        "summary_by_symbol.csv": _group_report(df, ["symbol"]),
        "summary_by_symbol_model.csv": _group_report(df, ["symbol", "model"] if "model" in df.columns else ["symbol"]),
        "summary_by_symbol_side.csv": _group_report(df, ["symbol", "side"] if "side" in df.columns else ["symbol"]),
        "summary_by_symbol_phase.csv": _group_report(df, ["symbol", "phase"] if "phase" in df.columns else ["symbol"]),
    }

    # Optional deeper splits
    if "ctx_sub_label" in df.columns:
        reports["summary_by_symbol_ctx.csv"] = _group_report(df, ["symbol", "ctx_sub_label"])
    if "regime" in df.columns:
        reports["summary_by_symbol_regime.csv"] = _group_report(df, ["symbol", "regime"])

    for name, rdf in reports.items():
        rdf.to_csv(out_dir / name, index=False)

    # Best/worst by period
    best, worst = _best_worst_by_period(df, args.period, int(args.top_n))
    best.to_csv(out_dir / "best_trades_by_period.csv", index=False)
    worst.to_csv(out_dir / "worst_trades_by_period.csv", index=False)

    print(f"Wrote reports to: {out_dir}")
    for name in sorted(reports.keys()):
        print(" -", name)
    print(" - best_trades_by_period.csv")
    print(" - worst_trades_by_period.csv")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
