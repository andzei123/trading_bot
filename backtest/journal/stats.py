from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


# ✅ pasikeisk į savo failą (trades.csv / trades_short.csv ir t.t.)
TRADES_PATH = Path("backtest/journal/trades.csv")
OUT_DIR = Path("backtest/journal/exports_trades")


def compute_r(outcome: str, rr: float) -> float:
    outcome = str(outcome).upper()
    if outcome == "WIN":
        return float(rr)
    if outcome == "LOSS":
        return -1.0
    return 0.0


def _parse_bool_from_notes(notes: str, key: str) -> bool:
    # ieško: "range=True", "dev=False", "sweep=True", "ob=True"
    if not notes:
        return False
    m = re.search(rf"\b{re.escape(key)}=(True|False)\b", notes)
    if not m:
        return False
    return m.group(1) == "True"


def ensure_flag_cols(df: pd.DataFrame) -> pd.DataFrame:
    notes = df["notes"].fillna("").astype(str) if "notes" in df.columns else pd.Series([""] * len(df))

    mapping = {
        "wyckoff_range": "range",
        "deviation": "dev",
        "liquidity_sweep": "sweep",
        "ob_before_sweep": "ob",
    }

    for col, key in mapping.items():
        if col not in df.columns:
            df[col] = notes.apply(lambda s: _parse_bool_from_notes(s, key))
        else:
            # normalizuojam į bool (jei buvo string/0/1 ir pan.)
            if df[col].dtype == "object":
                df[col] = (
                    df[col].astype(str).str.strip().str.lower()
                    .map({"true": True, "false": False, "1": True, "0": False})
                    .fillna(False)
                )
            df[col] = df[col].astype(bool)

    return df


def score_bucket(s: float) -> str:
    if s >= 0.85:
        return ">=0.85"
    if s >= 0.80:
        return ">=0.80"
    if s >= 0.75:
        return ">=0.75"
    if s >= 0.70:
        return ">=0.70"
    if s >= 0.60:
        return ">=0.60"
    return "<0.60"


def profit_factor(r: pd.Series) -> float:
    wins = r[r > 0].sum()
    losses = r[r < 0].sum()
    return float(wins / abs(losses)) if losses < 0 else float("inf")


def max_drawdown_from_r(r: pd.Series) -> float:
    eq = r.cumsum()
    peak = eq.cummax()
    dd = eq - peak
    return float(dd.min()) if len(dd) else 0.0


def group_trade_stats(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    base = df.copy()

    # W/L tik iš WIN/LOSS (NO_HIT išmeta iš winrate skaičiavimo)
    base["is_win"] = base["outcome"].eq("WIN")
    base["is_loss"] = base["outcome"].eq("LOSS")
    base["wl_count"] = base["outcome"].isin(["WIN", "LOSS"]).astype(int)

    rows = []
    for keys, g in base.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)

        cnt = len(g)
        wins = int(g["is_win"].sum())
        losses = int(g["is_loss"].sum())
        wl = int(g["wl_count"].sum())
        winrate = (wins / wl * 100.0) if wl > 0 else 0.0

        mean_r = float(g["R"].mean()) if cnt else 0.0
        sum_r = float(g["R"].sum()) if cnt else 0.0
        pf = profit_factor(g["R"])
        mdd = max_drawdown_from_r(g["R"])

        rows.append((*keys, cnt, wins, losses, winrate, mean_r, sum_r, pf, mdd))

    cols = group_cols + ["count", "wins", "losses", "winrate", "expectancy", "sumR", "profit_factor", "max_dd_R"]
    out = pd.DataFrame(rows, columns=cols)
    return out


def main():
    if not TRADES_PATH.exists():
        print(f"❌ Neradau failo: {TRADES_PATH}")
        return

    df = pd.read_csv(TRADES_PATH, engine="python", on_bad_lines="skip")

    if df.empty:
        print("❌ CSV tuščias (arba visos eilutės nusiskipino).")
        return

    needed = {"timestamp", "side", "rr"}
    missing = needed - set(df.columns)
    if missing:
        print(f"❌ Trūksta stulpelių: {sorted(missing)}")
        print(f"   Yra: {list(df.columns)}")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    if "outcome" not in df.columns:
        df["outcome"] = "NO_HIT"
    df["outcome"] = df["outcome"].fillna("NO_HIT").astype(str).str.upper()

    df["rr"] = pd.to_numeric(df["rr"], errors="coerce").fillna(0.0)
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0.0)
    else:
        df["score"] = 0.0

    df = ensure_flag_cols(df)

    # R
    df["R"] = [compute_r(o, rr) for o, rr in zip(df["outcome"], df["rr"])]

    # Combo string
    df["combo"] = (
        "range=" + df["wyckoff_range"].astype(str) + " | "
        + "dev=" + df["deviation"].astype(str) + " | "
        + "sweep=" + df["liquidity_sweep"].astype(str) + " | "
        + "ob=" + df["ob_before_sweep"].astype(str)
    )

    # Score bucket
    df["score_bucket"] = df["score"].apply(score_bucket)

    # ====== BASIC REPORT ======
    total = len(df)
    wins = int((df["outcome"] == "WIN").sum())
    losses = int((df["outcome"] == "LOSS").sum())
    nohit = int((df["outcome"] == "NO_HIT").sum())
    wl = wins + losses
    wr = (wins / wl * 100.0) if wl else 0.0

    print("\n==================== TRADES STATS (R-based) ====================")
    print(f"File:               {TRADES_PATH}")
    print(f"Trades total:       {total}")
    print(f"Wins/Loss/NO_HIT:   {wins}/{losses}/{nohit}")
    print(f"Winrate (W/L only): {wr:.2f}%")
    print(f"Expectancy:         {df['R'].mean():.3f} R")
    print(f"Profit factor:      {profit_factor(df['R']):.3f}")
    print(f"Max drawdown:       {max_drawdown_from_r(df['R']):.3f} R")
    print("===============================================================\n")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ====== 1) Top combos (by count) ======
    combo_stats = group_trade_stats(df, ["combo"]).sort_values("count", ascending=False)
    print("Top combos by count (top 15):")
    print(combo_stats.head(15).to_string(index=False))

    # ====== 2) Best combos (by expectancy, min trades filter) ======
    min_trades = 50
    best_combo = combo_stats[combo_stats["count"] >= min_trades].sort_values("expectancy", ascending=False)
    print(f"\nBest combos by expectancy (min_trades={min_trades}, top 15):")
    print(best_combo.head(15).to_string(index=False))

    # ====== 3) Score buckets ======
    bucket_stats = group_trade_stats(df, ["score_bucket"]).sort_values("count", ascending=False)
    print("\nScore buckets:")
    print(bucket_stats.to_string(index=False))

    # ====== 4) Combo x Score bucket ======
    combo_bucket = group_trade_stats(df, ["combo", "score_bucket"]).sort_values("count", ascending=False)
    print("\nTop combo x score_bucket (top 20):")
    print(combo_bucket.head(20).to_string(index=False))

    # ====== EXPORTS ======
    combo_stats.to_csv(OUT_DIR / "combo_stats.csv", index=False)
    best_combo.to_csv(OUT_DIR / "best_combo_by_expectancy.csv", index=False)
    bucket_stats.to_csv(OUT_DIR / "score_bucket_stats.csv", index=False)
    combo_bucket.to_csv(OUT_DIR / "combo_x_score_bucket.csv", index=False)

    print("\n✅ Exported:")
    print(f" - {OUT_DIR / 'combo_stats.csv'}")
    print(f" - {OUT_DIR / 'best_combo_by_expectancy.csv'}")
    print(f" - {OUT_DIR / 'score_bucket_stats.csv'}")
    print(f" - {OUT_DIR / 'combo_x_score_bucket.csv'}")


if __name__ == "__main__":
    main()
