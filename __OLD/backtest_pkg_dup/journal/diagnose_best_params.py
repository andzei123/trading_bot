from __future__ import annotations

import json
from pathlib import Path
import sys
import argparse

import pandas as pd
import numpy as np

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
JOURNAL_DIR = THIS_FILE.parent
ENGINE_DIR = PROJECT_ROOT / "backtest" / "engine"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(JOURNAL_DIR))
sys.path.insert(0, str(ENGINE_DIR))

import filter_trades as ft  # for EXPORT_DIR (and json helpers if you want)


EXPORT_DIR = ft.EXPORT_DIR
BEST_JSON = EXPORT_DIR / "best_params.json"
BEST_TRADES_ALL = EXPORT_DIR / "best_trades_all.csv"


def _read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def wl_winrate(df: pd.DataFrame) -> float:
    if df.empty or "outcome" not in df.columns:
        return float("nan")
    o = df["outcome"].astype(str).str.upper()
    w = int((o == "WIN").sum())
    l = int((o == "LOSS").sum())
    wl = w + l
    return (w / wl * 100.0) if wl else float("nan")


def expectancy_from_R(df: pd.DataFrame) -> float:
    if df.empty or "R" not in df.columns:
        return float("nan")
    return float(pd.to_numeric(df["R"], errors="coerce").dropna().mean())


def sum_R(df: pd.DataFrame) -> float:
    if df.empty or "R" not in df.columns:
        return float("nan")
    return float(pd.to_numeric(df["R"], errors="coerce").fillna(0.0).sum())


def max_drawdown_R(r: pd.Series) -> float:
    """returns max drawdown in R (negative number)"""
    eq = r.fillna(0.0).cumsum()
    peak = eq.cummax()
    dd = eq - peak
    return float(dd.min()) if len(dd) else float("nan")


def ensure_dt(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # try common timestamp columns
    for col in ["exit_time", "close_time", "timestamp", "ts", "datetime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    # pick one for grouping (prefer exit_time)
    if "exit_time" in df.columns and df["exit_time"].notna().any():
        df["_t"] = df["exit_time"]
    elif "close_time" in df.columns and df["close_time"].notna().any():
        df["_t"] = df["close_time"]
    elif "timestamp" in df.columns and df["timestamp"].notna().any():
        df["_t"] = df["timestamp"]
    else:
        raise SystemExit(
            "Could not find a usable time column. Expected one of: exit_time/close_time/timestamp/ts/datetime"
        )
    df["_t"] = pd.to_datetime(df["_t"], errors="coerce")
    return df


def print_equity_summary(df: pd.DataFrame) -> None:
    r = pd.to_numeric(df.get("R", pd.Series([], dtype=float)), errors="coerce")
    trades = float(len(df))
    s = float(r.fillna(0.0).sum()) if len(r) else float("nan")
    exp = float(r.dropna().mean()) if len(r.dropna()) else float("nan")
    med = float(r.dropna().median()) if len(r.dropna()) else float("nan")
    mdd = max_drawdown_R(r)
    print("=== EQUITY SUMMARY ===")
    print(f"trades={trades:.1f} sum_R={s:.4f} exp_R={exp:.6f} median_R={med:.6f} maxDD_R={mdd:.4f}")


def monthly_table(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["month"] = d["_t"].dt.to_period("M").astype(str)
    g = d.groupby("month", dropna=False)
    out = g.apply(
        lambda x: pd.Series(
            {
                "trades": int(len(x)),
                "sum_R": round(sum_R(x), 4),
                "expectancy_R": round(expectancy_from_R(x), 6),
                "winrate_WL_%": round(wl_winrate(x), 6),
            }
        )
    ).reset_index()
    return out.sort_values("sum_R", ascending=True).reset_index(drop=True)


def breakdown_outcome(df: pd.DataFrame, title: str) -> None:
    if "outcome" not in df.columns:
        return
    o = df["outcome"].astype(str).str.upper()
    tbl = o.value_counts(dropna=False).rename_axis("outcome").reset_index(name="count")
    tbl["pct"] = (tbl["count"] / max(1, len(df)) * 100.0).round(3)
    print(f"=== OUTCOME BREAKDOWN: {title} ===")
    print(tbl.to_string(index=False))


def breakdown_side(df: pd.DataFrame, title: str) -> None:
    if "side" not in df.columns:
        return
    s = df["side"].astype(str).str.upper()
    d = df.copy()
    d["_side"] = s
    g = d.groupby("_side", dropna=False)
    tbl = g.apply(
        lambda x: pd.Series(
            {
                "trades": int(len(x)),
                "sum_R": round(sum_R(x), 4),
                "expectancy_R": round(expectancy_from_R(x), 6),
                "winrate_WL_%": round(wl_winrate(x), 6),
            }
        )
    ).reset_index().rename(columns={"_side": "side"})
    tbl = tbl.sort_values("sum_R", ascending=True).reset_index(drop=True)
    print(f"=== SIDE BREAKDOWN: {title} ===")
    print(tbl.to_string(index=False))


def breakdown_col(df: pd.DataFrame, col: str, title: str, top_n: int = 10) -> None:
    if col not in df.columns:
        return
    d = df.copy()
    d[col] = d[col].astype(str)
    g = d.groupby(col, dropna=False)
    tbl = g.apply(
        lambda x: pd.Series(
            {
                "trades": int(len(x)),
                "sum_R": round(sum_R(x), 4),
                "expectancy_R": round(expectancy_from_R(x), 6),
                "winrate_WL_%": round(wl_winrate(x), 6),
            }
        )
    ).reset_index().sort_values("sum_R", ascending=True).reset_index(drop=True)

    print(f"=== {col} WORST {top_n}: {title} ===")
    print(tbl.head(top_n).to_string(index=False))


def segment_by_trade_time(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    d = df.sort_values("_t").reset_index(drop=True).copy()
    n = len(d)
    if n == 0:
        return {"early_33%": d, "mid_33%": d, "late_33%": d}
    a = int(round(n * 1 / 3))
    b = int(round(n * 2 / 3))
    return {
        "early_33%": d.iloc[:a].copy(),
        "mid_33%": d.iloc[a:b].copy(),
        "late_33%": d.iloc[b:].copy(),
    }


def deep_dive_segment(name: str, seg: pd.DataFrame) -> None:
    print(f"\n=== DEEP DIVE {name} ===")
    print_equity_summary(seg)
    breakdown_outcome(seg, name)
    breakdown_side(seg, name)
    breakdown_col(seg, "ctx_label", name, top_n=10)
    breakdown_col(seg, "ctx_sub_label", name, top_n=10)
    # papildomai: trend/regime jei turi
    breakdown_col(seg, "htf_trend", name, top_n=10)
    breakdown_col(seg, "regime", name, top_n=10)


def worst_months_in_segment(seg: pd.DataFrame, top_k: int = 3) -> pd.DataFrame:
    m = monthly_table(seg)
    return m.head(top_k).copy()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--trades",
        type=str,
        default=str(BEST_TRADES_ALL),
        help="Path to trades CSV (default: exports_trades/best_trades_all.csv)",
    )
    ap.add_argument(
        "--best-json",
        type=str,
        default=str(BEST_JSON),
        help="Path to best_params.json (optional).",
    )
    args = ap.parse_args()

    trades_path = Path(args.trades)
    best_json_path = Path(args.best_json)

    if not trades_path.exists():
        raise SystemExit(f"Missing trades file: {trades_path}")

    best = _read_json(best_json_path) if best_json_path.exists() else {}

    df = pd.read_csv(trades_path)
    df = ensure_dt(df)


    print("Best params:", best)
    print_equity_summary(df)

    # overall monthly
    m_all = monthly_table(df)
    print("=== WORST 5 months by sum_R (ALL) ===")
    print(m_all.head(5).to_string(index=False))

    # segments
    segs = segment_by_trade_time(df)

    # show segment tables quickly
    print("\n=== SEGMENT SUMMARY (trade-time tertiles) ===")
    rows = []
    for k, s in segs.items():
        rows.append({
            "segment": k,
            "trades": int(len(s)),
            "sum_R": round(sum_R(s), 4),
            "expectancy_R": round(expectancy_from_R(s), 6),
            "winrate_WL_%": round(wl_winrate(s), 6),
        })
    print(pd.DataFrame(rows).to_string(index=False))

    # deep dives: MID + LATE (kaip prašei)
    deep_dive_segment("mid_33%", segs["mid_33%"])
    deep_dive_segment("late_33%", segs["late_33%"])

    # worst 3 months inside MID
    mid = segs["mid_33%"]
    wm = worst_months_in_segment(mid, top_k=3)
    print("\n=== WORST 3 months by sum_R (MID_33%) ===")
    print(wm.to_string(index=False))

    # deep dive each worst mid month
    for _, r in wm.iterrows():
        month = r["month"]
        sub = mid[mid["_t"].dt.to_period("M").astype(str) == month].copy()
        print(f"\n=== MID_33% deep dive worst month: {month} ===")
        print_equity_summary(sub)
        breakdown_outcome(sub, f"mid {month}")
        breakdown_side(sub, f"mid {month}")
        breakdown_col(sub, "ctx_sub_label", f"mid {month}", top_n=15)
        breakdown_col(sub, "htf_trend", f"mid {month}", top_n=10)
        breakdown_col(sub, "regime", f"mid {month}", top_n=10)


if __name__ == "__main__":
    main()
