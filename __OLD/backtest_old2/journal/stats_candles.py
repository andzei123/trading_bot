from __future__ import annotations

import pandas as pd
from pathlib import Path


CANDLES_PATH = Path("backtest/journal/candles.csv")
OUT_DIR = Path("backtest/journal/exports")


def _bucket(score: float) -> str:
    if score >= 0.85: return ">=0.85"
    if score >= 0.80: return ">=0.80"
    if score >= 0.75: return ">=0.75"
    if score >= 0.70: return ">=0.70"
    if score >= 0.60: return ">=0.60"
    return "<0.60"


def main():
    if not CANDLES_PATH.exists():
        print(f"❌ Neradau failo: {CANDLES_PATH}")
        return

    df = pd.read_csv(CANDLES_PATH, engine="python", on_bad_lines="skip")
    if df.empty:
        print("❌ candles.csv tuščias (arba visos eilutės nusiskipino).")
        return

    # minimal
    needed = {"timestamp", "allowed", "reason", "score"}
    miss = needed - set(df.columns)
    if miss:
        print(f"❌ Trūksta stulpelių candles.csv: {sorted(miss)}")
        print(f"   Yra: {list(df.columns)}")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["allowed"] = df["allowed"].astype(bool)
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0.0)
    df["score_bucket"] = df["score"].apply(_bucket)
    df["date"] = df["timestamp"].dt.date

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    total = len(df)
    allowed_cnt = int(df["allowed"].sum())
    ok_cnt = int((df["reason"] == "OK").sum())
    allowed_rate = (allowed_cnt / total * 100.0) if total else 0.0
    ok_rate_total = (ok_cnt / total * 100.0) if total else 0.0
    ok_rate_allowed = (ok_cnt / allowed_cnt * 100.0) if allowed_cnt else 0.0

    print("\n==================== CANDLES FUNNEL STATS ====================")
    print(f"Total candles:                 {total}")
    print(f"Allowed candles:               {allowed_cnt}  ({allowed_rate:.2f}%)")
    print(f"OK signals (reason == OK):     {ok_cnt}  ({ok_rate_total:.2f}% of all)")
    print(f"OK rate among allowed:         {ok_rate_allowed:.2f}%")
    print("==============================================================\n")

    # 1) Reason breakdown
    reason_tbl = (
        df.groupby("reason")
        .agg(
            candles=("reason", "count"),
            allowed=("allowed", "sum"),
            avg_score=("score", "mean"),
        )
        .sort_values("candles", ascending=False)
    )
    reason_tbl["allowed_rate_%"] = (reason_tbl["allowed"] / reason_tbl["candles"] * 100.0).round(2)
    print("Reason breakdown (top):")
    print(reason_tbl.head(20).to_string())
    print()

    # 2) Score bucket x reason
    pivot_reason_score = (
        df.pivot_table(
            index="reason",
            columns="score_bucket",
            values="idx" if "idx" in df.columns else "score",
            aggfunc="count",
            fill_value=0,
        )
        .sort_index()
    )

    # 3) Daily OK / Allowed
    daily = (
        df.groupby("date")
        .agg(
            candles=("date", "count"),
            allowed=("allowed", "sum"),
            ok=("reason", lambda s: (s == "OK").sum()),
        )
        .sort_index()
    )
    daily["allowed_rate_%"] = (daily["allowed"] / daily["candles"] * 100.0).round(2)
    daily["ok_rate_all_%"] = (daily["ok"] / daily["candles"] * 100.0).round(2)
    allowed_num = pd.to_numeric(daily["allowed"], errors="coerce")
    ok_num = pd.to_numeric(daily["ok"], errors="coerce")

    daily["ok_rate_allowed_%"] = (
            ok_num / allowed_num.where(allowed_num > 0) * 100.0
    ).round(2)

    # 4) Flag combos vs reason (jei yra flag’ai)
    # --- ensure flag columns from notes (if missing) ---
    from pandas.api.types import is_bool_dtype

    # --- ensure flag columns from notes (if missing) ---
    if "notes" in df.columns:
        notes = df["notes"].fillna("").astype(str)

        def parse_bool(key: str) -> pd.Series:
            return notes.str.contains(rf"\b{key}=True\b", regex=True).fillna(False)

        flag_map = {
            "wyckoff_range": "range",
            "deviation": "dev",
            "liquidity_sweep": "sweep",
            "ob_before_sweep": "ob",
        }

        for col, key in flag_map.items():
            if col not in df.columns:
                df[col] = parse_bool(key)
            else:
                if not is_bool_dtype(df[col]):
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .map({"true": True, "false": False})
                        .fillna(False)
                    )

            df[col] = df[col].astype(bool)

    flag_cols = [c for c in ["wyckoff_range", "deviation", "liquidity_sweep", "ob_before_sweep"] if c in df.columns]
    combo_tbl = None
    for c in flag_cols:
        if df[c].dtype == "object":
            df[c] = (
                df[c].astype(str).str.strip().str.lower()
                .map({"true": True, "false": False})
                .fillna(False)
            )
        df[c] = df[c].astype(bool)

        df["combo"] = df[flag_cols].astype(str).agg(
            lambda row: " | ".join([f"{c}={row[c]}" for c in flag_cols]),
            axis=1
        )

        combo_tbl = (
            df.groupby(["combo", "reason"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )

    # ---- EXPORTS ----
    reason_tbl.to_csv(OUT_DIR / "reason_breakdown.csv", index=True)
    pivot_reason_score.to_csv(OUT_DIR / "reason_x_score_bucket.csv", index=True)
    daily.to_csv(OUT_DIR / "daily_funnel.csv", index=True)
    if combo_tbl is not None:
        combo_tbl.to_csv(OUT_DIR / "combo_x_reason.csv", index=False)

    print("flag_cols:", flag_cols)
    print(df["combo"].value_counts().head(5))

    print(f"✅ Exported:")
    print(f"  - {OUT_DIR / 'reason_breakdown.csv'}")
    print(f"  - {OUT_DIR / 'reason_x_score_bucket.csv'}")
    print(f"  - {OUT_DIR / 'daily_funnel.csv'}")
    if combo_tbl is not None:
        print(f"  - {OUT_DIR / 'combo_x_reason.csv'}")


if __name__ == "__main__":
    main()
