import argparse
import pandas as pd
import numpy as np


def pick_r_col(df: pd.DataFrame) -> str:
    for c in ["R", "r", "realized_r", "pnl_r"]:
        if c in df.columns:
            return c
    raise ValueError("Nerastas outcome stulpelis: reikia vieno iš ['R', 'r', 'realized_r', 'pnl_r']")


def compute_kpis(path: str) -> dict:
    df = pd.read_csv(path)
    r_col = pick_r_col(df)
    r = pd.to_numeric(df[r_col], errors="coerce").dropna()

    wins = r[r > 0]
    losses = r[r <= 0]

    pf = wins.sum() / abs(losses.sum()) if len(losses) > 0 and abs(losses.sum()) > 0 else np.nan
    expectancy = r.mean() if len(r) else np.nan
    winrate = (r > 0).mean() if len(r) else np.nan
    total_r = r.sum() if len(r) else np.nan

    return {
        "trades": int(len(r)),
        "total_R": float(total_r) if pd.notna(total_r) else np.nan,
        "winrate": float(winrate) if pd.notna(winrate) else np.nan,
        "expectancy": float(expectancy) if pd.notna(expectancy) else np.nan,
        "PF": float(pf) if pd.notna(pf) else np.nan,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True, help="Bazinis trades CSV")
    parser.add_argument("--variant", required=True, help="Wait/confirm entry trades CSV")
    args = parser.parse_args()

    base = compute_kpis(args.baseline)
    var = compute_kpis(args.variant)

    print("\n=== BASELINE ===")
    for k, v in base.items():
        print(f"{k}: {v}")

    print("\n=== VARIANT ===")
    for k, v in var.items():
        print(f"{k}: {v}")

    print("\n=== DELTA (variant - baseline) ===")
    for k in ["trades", "total_R", "winrate", "expectancy", "PF"]:
        try:
            print(f"{k}: {var[k] - base[k]}")
        except Exception:
            print(f"{k}: n/a")


if __name__ == "__main__":
    main()