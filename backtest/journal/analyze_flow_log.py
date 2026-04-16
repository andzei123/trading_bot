from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _expand_model_rows(df: pd.DataFrame, stage_col: str, count_col: str) -> pd.DataFrame:
    rows = []
    if stage_col not in df.columns:
        return pd.DataFrame(columns=["symbol", "model", "cycles", count_col])

    for _, r in df.iterrows():
        symbol = str(r.get("symbol", "") or "")
        summary = str(r.get(stage_col, "") or "").strip()
        if not summary:
            continue
        parts = [p for p in summary.split(";") if p]
        seen = set()
        for p in parts:
            if ":" not in p:
                continue
            model, raw_n = p.split(":", 1)
            model = str(model).strip()
            if not model:
                continue
            try:
                n = int(float(raw_n))
            except Exception:
                n = 0
            rows.append({
                "symbol": symbol,
                "model": model,
                "cycles": 1 if model not in seen else 0,
                count_col: n,
            })
            seen.add(model)
    if not rows:
        return pd.DataFrame(columns=["symbol", "model", "cycles", count_col])
    return pd.DataFrame(rows)


def analyze(flow_log: Path, summary_csv: Path) -> int:
    if not flow_log.exists():
        print(f"Flow log not found: {flow_log}")
        return 1

    df = pd.read_csv(flow_log)
    if df.empty:
        print("Flow log is empty.")
        return 0

    for c in [
        "raw_entries_count",
        "after_wait_count",
        "after_stale_count",
        "after_idempotency_count",
        "after_per_cycle_guard_count",
        "emitted_count",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    if "skipped_open_position" in df.columns:
        df["skipped_open_position"] = df["skipped_open_position"].astype(str).str.lower().isin(["true", "1", "yes"])
    else:
        df["skipped_open_position"] = False

    if "death_stage" not in df.columns:
        df["death_stage"] = ""

    symbol_summary = (
        df.groupby("symbol", dropna=False)
        .apply(
            lambda g: pd.Series(
                {
                    "cycles": int(len(g)),
                    "raw_entries_cycles": int((g["raw_entries_count"] > 0).sum()),
                    "raw_entries_total": int(g["raw_entries_count"].sum()),
                    "emitted_cycles": int((g["emitted_count"] > 0).sum()),
                    "emitted_total": int(g["emitted_count"].sum()),
                    "died_wait_cycles": int((g["death_stage"] == "wait").sum()),
                    "died_stale_cycles": int((g["death_stage"] == "stale").sum()),
                    "died_idempotency_cycles": int((g["death_stage"] == "idempotency").sum()),
                    "open_position_skip_cycles": int((g["death_stage"] == "open_position_skip").sum()),
                    "no_raw_cycles": int((g["death_stage"] == "no_raw").sum()),
                }
            )
        )
        .reset_index()
        .sort_values("symbol")
    )

    death_stage_summary = (
        df.groupby("death_stage", dropna=False)
        .size()
        .reset_index(name="cycles")
        .sort_values(["cycles", "death_stage"], ascending=[False, True])
    )

    raw_models = _expand_model_rows(df, "model_summary_raw", "raw_rows")
    wait_models = _expand_model_rows(df, "model_summary_after_wait", "after_wait_rows")
    stale_models = _expand_model_rows(df, "model_summary_after_stale", "after_stale_rows")
    emit_models = _expand_model_rows(df, "model_summary_after_per_cycle_guard", "post_guard_rows")

    model_summary = None
    if not raw_models.empty or not wait_models.empty or not stale_models.empty or not emit_models.empty:
        frames = []
        for part in [raw_models, wait_models, stale_models, emit_models]:
            if not part.empty:
                frames.append(part)
        merged = None
        for part in frames:
            keys = [c for c in ["symbol", "model"] if c in part.columns]
            if merged is None:
                merged = part.copy()
            else:
                merged = merged.merge(part, on=keys, how="outer")
        model_summary = merged.fillna(0)
        for c in ["cycles", "raw_rows", "after_wait_rows", "after_stale_rows", "post_guard_rows"]:
            if c in model_summary.columns:
                model_summary[c] = pd.to_numeric(model_summary[c], errors="coerce").fillna(0).astype(int)
        model_summary["wait_deaths_on_model_cycles"] = (model_summary["raw_rows"] > 0).astype(int) - (model_summary.get("after_wait_rows", 0) > 0).astype(int)
        model_summary["stale_deaths_on_model_cycles"] = (model_summary.get("after_wait_rows", 0) > 0).astype(int) - (model_summary.get("after_stale_rows", 0) > 0).astype(int)
        model_summary["idempotency_deaths_on_model_cycles"] = (model_summary.get("after_stale_rows", 0) > 0).astype(int) - (model_summary.get("post_guard_rows", 0) > 0).astype(int)
        model_summary["wait_deaths_on_model_cycles"] = model_summary["wait_deaths_on_model_cycles"].clip(lower=0)
        model_summary["stale_deaths_on_model_cycles"] = model_summary["stale_deaths_on_model_cycles"].clip(lower=0)
        model_summary["idempotency_deaths_on_model_cycles"] = model_summary["idempotency_deaths_on_model_cycles"].clip(lower=0)
        model_summary = (
            model_summary.groupby("model", dropna=False)
            .agg(
                raw_cycles=("cycles", "sum"),
                raw_rows=("raw_rows", "sum"),
                after_wait_rows=("after_wait_rows", "sum"),
                after_stale_rows=("after_stale_rows", "sum"),
                post_guard_rows=("post_guard_rows", "sum"),
                wait_deaths_on_model_cycles=("wait_deaths_on_model_cycles", "sum"),
                stale_deaths_on_model_cycles=("stale_deaths_on_model_cycles", "sum"),
                idempotency_deaths_on_model_cycles=("idempotency_deaths_on_model_cycles", "sum"),
            )
            .reset_index()
            .sort_values("model")
        )

    print("=== ATS FLOW SUMMARY: BY SYMBOL ===")
    print(symbol_summary.to_string(index=False))
    print()

    print("=== ATS FLOW SUMMARY: BY DEATH STAGE ===")
    print(death_stage_summary.to_string(index=False))
    print()

    if model_summary is not None and not model_summary.empty:
        print("=== ATS FLOW SUMMARY: BY MODEL ===")
        print(model_summary.to_string(index=False))
        print()

    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    symbol_summary_out = symbol_summary.copy()
    symbol_summary_out.insert(0, "summary_type", "symbol")
    symbol_summary_out.to_csv(summary_csv, index=False)

    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze ATS live flow_log.csv")
    ap.add_argument("--flow_log", default="backtest/journal/exports_live/flow_log.csv")
    ap.add_argument("--summary_csv", default="backtest/journal/exports_live/flow_summary.csv")
    args = ap.parse_args()
    return analyze(Path(args.flow_log), Path(args.summary_csv))


if __name__ == "__main__":
    raise SystemExit(main())
