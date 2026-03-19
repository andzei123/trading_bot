from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
from typing import Optional

import pandas as pd


PREFERRED_PNL_COLUMNS = [
    "pnl",
    "net_pnl",
    "realized_pnl",
    "profit",
    "trade_pnl",
]

PREFERRED_EQUITY_COLUMNS = [
    "equity",
    "balance",
    "cum_pnl",
    "cumulative_pnl",
    "running_pnl",
]

PREFERRED_TIME_COLUMNS = [
    "timestamp",
    "time",
    "datetime",
    "date",
    "open_time",
    "close_time",
]

IGNORE_DIR_NAMES = {
    ".git",
    ".idea",
    ".venv",
    "__pycache__",
    "node_modules",
}


def safe_read_csv(path: Path) -> Optional[pd.DataFrame]:
    encodings = ["utf-8", "utf-8-sig", "cp1257", "cp1251", "latin1"]
    for enc in encodings:
        try:
            return pd.read_csv(path, low_memory=False, encoding=enc)
        except Exception:
            continue
    return None


def first_matching_column(columns: list[str], candidates: list[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def infer_pnl_series(df: pd.DataFrame) -> tuple[Optional[pd.Series], Optional[str]]:
    cols = list(df.columns)
    pnl_col = first_matching_column(cols, PREFERRED_PNL_COLUMNS)
    if pnl_col:
        s = pd.to_numeric(df[pnl_col], errors="coerce")
        if s.notna().any():
            return s, pnl_col

    eq_col = first_matching_column(cols, PREFERRED_EQUITY_COLUMNS)
    if eq_col:
        eq = pd.to_numeric(df[eq_col], errors="coerce")
        if eq.notna().sum() >= 2:
            pnl = eq.diff().fillna(0.0)
            return pnl, f"derived_from:{eq_col}"

    return None, None


def infer_equity_series(df: pd.DataFrame, pnl: Optional[pd.Series]) -> tuple[Optional[pd.Series], Optional[str]]:
    cols = list(df.columns)
    eq_col = first_matching_column(cols, PREFERRED_EQUITY_COLUMNS)
    if eq_col:
        eq = pd.to_numeric(df[eq_col], errors="coerce")
        if eq.notna().sum() >= 2:
            return eq, eq_col

    if pnl is not None and pnl.notna().any():
        eq = pnl.fillna(0.0).cumsum()
        return eq, "derived_from:pnl"

    return None, None


def infer_time_column(df: pd.DataFrame) -> Optional[str]:
    return first_matching_column(list(df.columns), PREFERRED_TIME_COLUMNS)


def compute_max_drawdown(equity: pd.Series) -> float:
    equity = pd.to_numeric(equity, errors="coerce").dropna()
    if equity.empty:
        return float("nan")
    running_max = equity.cummax()
    dd = equity - running_max
    return float(dd.min())


def compute_profit_factor(pnl: pd.Series) -> float:
    pnl = pd.to_numeric(pnl, errors="coerce").dropna()
    if pnl.empty:
        return float("nan")
    gross_profit = pnl[pnl > 0].sum()
    gross_loss = -pnl[pnl < 0].sum()
    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else float("nan")
    return float(gross_profit / gross_loss)


def compute_winrate(pnl: pd.Series) -> float:
    pnl = pd.to_numeric(pnl, errors="coerce").dropna()
    if pnl.empty:
        return float("nan")
    non_zero = pnl[pnl != 0]
    if non_zero.empty:
        return float("nan")
    return float((non_zero > 0).mean())


def find_csv_files(root: Path) -> list[Path]:
    results: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIR_NAMES]
        for fn in filenames:
            if fn.lower().endswith(".csv"):
                results.append(Path(dirpath) / fn)
    return sorted(results)


def analyze_csv(path: Path, root: Path) -> Optional[dict]:
    df = safe_read_csv(path)
    if df is None or df.empty:
        return None

    pnl, pnl_source = infer_pnl_series(df)
    equity, equity_source = infer_equity_series(df, pnl)
    time_col = infer_time_column(df)

    if pnl is None and equity is None:
        return None

    pnl_non_na = pd.Series(dtype=float) if pnl is None else pd.to_numeric(pnl, errors="coerce").dropna()
    equity_non_na = pd.Series(dtype=float) if equity is None else pd.to_numeric(equity, errors="coerce").dropna()

    total_pnl = float(pnl_non_na.sum()) if not pnl_non_na.empty else float("nan")
    winrate = compute_winrate(pnl_non_na) if not pnl_non_na.empty else float("nan")
    profit_factor = compute_profit_factor(pnl_non_na) if not pnl_non_na.empty else float("nan")
    max_drawdown = compute_max_drawdown(equity_non_na) if not equity_non_na.empty else float("nan")

    first_ts = ""
    last_ts = ""
    if time_col:
        try:
            ts = pd.to_datetime(df[time_col], errors="coerce")
            ts = ts.dropna()
            if not ts.empty:
                first_ts = str(ts.iloc[0])
                last_ts = str(ts.iloc[-1])
        except Exception:
            pass

    score = 0.0
    if not math.isnan(total_pnl):
        score += total_pnl
    if not math.isnan(winrate):
        score += winrate * 1000.0
    if not math.isnan(profit_factor) and math.isfinite(profit_factor):
        score += profit_factor * 100.0
    if not math.isnan(max_drawdown):
        score += max_drawdown  # usually negative

    rel_file = path.relative_to(root).as_posix()
    rel_folder = path.parent.relative_to(root).as_posix()

    return {
        "file": str(path),
        "rel_file": rel_file,
        "filename": path.name,
        "folder": str(path.parent),
        "rel_folder": rel_folder,
        "rows": len(df),
        "columns": ", ".join(df.columns[:20]),
        "pnl_source": pnl_source or "",
        "equity_source": equity_source or "",
        "time_col": time_col or "",
        "first_ts": first_ts,
        "last_ts": last_ts,
        "total_pnl": total_pnl,
        "winrate": winrate,
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
        "score": score,
    }


def fmt_num(x: float, pct: bool = False) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    if x == float("inf"):
        return "inf"
    return f"{x:.2%}" if pct else f"{x:.4f}"


def build_folder_summary(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("rel_folder", dropna=False)
        .agg(
            file_count=("rel_file", "count"),
            total_pnl_sum=("total_pnl", "sum"),
            avg_winrate=("winrate", "mean"),
            avg_profit_factor=("profit_factor", "mean"),
            worst_drawdown=("max_drawdown", "min"),
            best_score=("score", "max"),
            first_seen=("first_ts", "min"),
            last_seen=("last_ts", "max"),
        )
        .reset_index()
    )

    grouped["folder_score"] = (
        grouped["total_pnl_sum"].fillna(0.0)
        + grouped["avg_winrate"].fillna(0.0) * 1000.0
        + grouped["avg_profit_factor"].replace([float("inf")], 10.0).fillna(0.0) * 100.0
        + grouped["worst_drawdown"].fillna(0.0)
    )

    grouped = grouped.sort_values(
        by=["folder_score", "total_pnl_sum", "avg_profit_factor", "avg_winrate"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    return grouped


def save_excel_if_possible(
    out_xlsx: Path,
    df_ranked: pd.DataFrame,
    df_best_pnl: pd.DataFrame,
    df_best_pf: pd.DataFrame,
    df_best_wr: pd.DataFrame,
    df_lowest_dd: pd.DataFrame,
    df_folder_summary: pd.DataFrame,
) -> bool:
    try:
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            df_ranked.to_excel(writer, sheet_name="ranked_all", index=False)
            df_best_pnl.to_excel(writer, sheet_name="best_pnl", index=False)
            df_best_pf.to_excel(writer, sheet_name="best_profit_factor", index=False)
            df_best_wr.to_excel(writer, sheet_name="best_winrate", index=False)
            df_lowest_dd.to_excel(writer, sheet_name="best_drawdown", index=False)
            df_folder_summary.to_excel(writer, sheet_name="folder_summary", index=False)
        return True
    except ModuleNotFoundError:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Map CSV performance across a project.")
    parser.add_argument("--root", default=".", help="Root folder to scan")
    parser.add_argument("--top", type=int, default=30, help="How many top files to show")
    parser.add_argument("--out", default="csv_performance_map.xlsx", help="Output Excel file")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    csv_files = find_csv_files(root)

    print(f"Scanning root: {root}")
    print(f"CSV files found: {len(csv_files)}")

    rows = []
    for i, path in enumerate(csv_files, start=1):
        result = analyze_csv(path, root)
        if result:
            rows.append(result)
        if i % 100 == 0:
            print(f"Processed {i}/{len(csv_files)} CSV files...")

    if not rows:
        print("No analyzable CSV files found.")
        return

    df = pd.DataFrame(rows)

    df_ranked = df.sort_values(
        by=["score", "total_pnl", "profit_factor", "winrate"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    df_best_pnl = df.sort_values(by="total_pnl", ascending=False, na_position="last").reset_index(drop=True)
    df_best_pf = df.sort_values(by="profit_factor", ascending=False, na_position="last").reset_index(drop=True)
    df_best_wr = df.sort_values(by="winrate", ascending=False, na_position="last").reset_index(drop=True)
    df_lowest_dd = df.sort_values(by="max_drawdown", ascending=False, na_position="last").reset_index(drop=True)
    df_folder_summary = build_folder_summary(df)

    preview = df_ranked.copy()
    preview["winrate_str"] = preview["winrate"].apply(lambda x: fmt_num(x, pct=True))
    preview["profit_factor_str"] = preview["profit_factor"].apply(fmt_num)
    preview["total_pnl_str"] = preview["total_pnl"].apply(fmt_num)
    preview["max_drawdown_str"] = preview["max_drawdown"].apply(fmt_num)

    print("\nTOP CSV FILES BY SCORE\n")
    cols = [
        "filename",
        "total_pnl_str",
        "winrate_str",
        "profit_factor_str",
        "max_drawdown_str",
        "first_ts",
        "last_ts",
        "rel_file",
    ]
    print(preview[cols].head(args.top).to_string(index=False))

    folder_preview = df_folder_summary.copy()
    folder_preview["avg_winrate_str"] = folder_preview["avg_winrate"].apply(lambda x: fmt_num(x, pct=True))
    folder_preview["avg_profit_factor_str"] = folder_preview["avg_profit_factor"].apply(fmt_num)
    folder_preview["total_pnl_sum_str"] = folder_preview["total_pnl_sum"].apply(fmt_num)
    folder_preview["worst_drawdown_str"] = folder_preview["worst_drawdown"].apply(fmt_num)

    print("\nTOP FOLDERS BY AGGREGATED SCORE\n")
    folder_cols = [
        "rel_folder",
        "file_count",
        "total_pnl_sum_str",
        "avg_winrate_str",
        "avg_profit_factor_str",
        "worst_drawdown_str",
        "first_seen",
        "last_seen",
    ]
    print(folder_preview[folder_cols].head(min(20, args.top)).to_string(index=False))

    out_xlsx = Path(args.out).resolve()
    out_csv = out_xlsx.with_suffix(".csv")
    out_folder_csv = out_xlsx.with_name(out_xlsx.stem + "_folders.csv")

    df_ranked.to_csv(out_csv, index=False, encoding="utf-8-sig")
    df_folder_summary.to_csv(out_folder_csv, index=False, encoding="utf-8-sig")

    print(f"\nSaved CSV report: {out_csv}")
    print(f"Saved folder summary: {out_folder_csv}")

    excel_saved = save_excel_if_possible(
        out_xlsx,
        df_ranked,
        df_best_pnl,
        df_best_pf,
        df_best_wr,
        df_lowest_dd,
        df_folder_summary,
    )

    if excel_saved:
        print(f"Saved Excel report: {out_xlsx}")
        print("Sheets: ranked_all, best_pnl, best_profit_factor, best_winrate, best_drawdown, folder_summary")
    else:
        print("Excel report was skipped because 'openpyxl' is not installed.")
        print("Install it with: pip install openpyxl")


if __name__ == "__main__":
    main()