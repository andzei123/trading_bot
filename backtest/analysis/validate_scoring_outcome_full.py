import glob
import os
import pandas as pd

# =========================
# LOAD CANDLES
# =========================
candles = pd.read_csv("backtest/journal/candles_ohlc.csv")

time_col = next((c for c in ["time", "timestamp", "open_time", "datetime"] if c in candles.columns), None)
if time_col is None:
    raise Exception(f"No time column in candles: {candles.columns.tolist()}")

high_col = next((c for c in candles.columns if c.lower() == "high"), None)
low_col = next((c for c in candles.columns if c.lower() == "low"), None)
if high_col is None or low_col is None:
    raise Exception(f"No high/low columns in candles: {candles.columns.tolist()}")

candles = candles.rename(columns={time_col: "time"}).sort_values("time")


def outcome(entry, tp, sl):
    for _, c in candles.iterrows():
        if c[low_col] <= tp:
            return "TP"
        if c[high_col] >= sl:
            return "SL"
    return "NO_HIT"


REQUIRED = ["symbol", "model", "side", "entry", "tp", "sl"]


def safe_read_csv(path):
    try:
        if not os.path.exists(path):
            print(f"[SKIP] missing file: {path}")
            return None
        if os.path.getsize(path) == 0:
            print(f"[SKIP] empty file: {path}")
            return None

        df = pd.read_csv(path)

        if df is None or len(df.columns) == 0:
            print(f"[SKIP] no columns: {path}")
            return None

        missing = [c for c in REQUIRED if c not in df.columns]
        if missing:
            print(f"[SKIP] missing required columns in {path}: {missing}")
            print(f"       available: {df.columns.tolist()}")
            return None

        return df

    except Exception as e:
        print(f"[SKIP] failed to read {path}: {e}")
        return None


def normalize(df):
    out = df[REQUIRED].copy()
    out["entry"] = pd.to_numeric(out["entry"], errors="coerce")
    out["tp"] = pd.to_numeric(out["tp"], errors="coerce")
    out["sl"] = pd.to_numeric(out["sl"], errors="coerce")
    out = out.dropna(subset=["symbol", "model", "side", "entry", "tp", "sl"]).copy()
    out["_k"] = out[["symbol", "model", "side", "entry", "tp", "sl"]].astype(str).agg("|".join, axis=1)
    return out


legacy_files = sorted(
    f for f in glob.glob("backtest/journal/validation_runs/legacy_*.csv")
    if not f.endswith("_cycle_metrics.csv") and not f.endswith("_dropped.csv")
)

signal_files = sorted(
    f for f in glob.glob("backtest/journal/validation_runs/signal_*.csv")
    if not f.endswith("_cycle_metrics.csv") and not f.endswith("_dropped.csv")
)

print("LEGACY FILES:", legacy_files)
print("SIGNAL FILES:", signal_files)

if not legacy_files or not signal_files:
    raise Exception("No validation run files found.")

pair_count = min(len(legacy_files), len(signal_files))
rows = []

for i in range(pair_count):
    l_file = legacy_files[i]
    s_file = signal_files[i]

    print(f"\n=== PAIR {i+1} ===")
    print("LEGACY:", l_file)
    print("SIGNAL:", s_file)

    l_raw = safe_read_csv(l_file)
    s_raw = safe_read_csv(s_file)

    if l_raw is None or s_raw is None:
        print("[SKIP] pair skipped due to unreadable file")
        continue

    l = normalize(l_raw)
    s = normalize(s_raw)

    print(f"  legacy rows={len(l)} signal rows={len(s)}")

    l_only = l[~l["_k"].isin(s["_k"])].copy()
    s_only = s[~s["_k"].isin(l["_k"])].copy()

    print(f"  legacy-only={len(l_only)} signal-only={len(s_only)}")

    for _, row in l_only.iterrows():
        rows.append({
            "run_pair": i + 1,
            "mode": "LEGACY",
            "symbol": row["symbol"],
            "model": row["model"],
            "side": row["side"],
            "entry": row["entry"],
            "tp": row["tp"],
            "sl": row["sl"],
            "outcome": outcome(row["entry"], row["tp"], row["sl"]),
        })

    for _, row in s_only.iterrows():
        rows.append({
            "run_pair": i + 1,
            "mode": "SIGNAL",
            "symbol": row["symbol"],
            "model": row["model"],
            "side": row["side"],
            "entry": row["entry"],
            "tp": row["tp"],
            "sl": row["sl"],
            "outcome": outcome(row["entry"], row["tp"], row["sl"]),
        })

df = pd.DataFrame(rows)

print("\n=== RAW RESULTS ===")
if len(df):
    print(df.to_string(index=False))
else:
    print("(no differing selections across completed run pairs)")

print("\n=== SUMMARY ===")
if len(df):
    print(df.groupby(["mode", "outcome"]).size().to_string())
else:
    print("(empty)")

print("\n=== BY SYMBOL ===")
if len(df):
    print(df.groupby(["mode", "symbol", "outcome"]).size().to_string())
else:
    print("(empty)")

print("\n=== WIN RATE ===")
if len(df):
    wins = df[df["outcome"] == "TP"].groupby("mode").size()
    total = df.groupby("mode").size()
    print((wins / total).fillna(0).to_string())
else:
    print("(empty)")

print("\n=== PRACTICAL VERDICT ===")
if len(df) == 0:
    print("No differing selections across completed run pairs. SIGNAL_SCORE has not shown practical selection impact in this sample.")
else:
    wins = df[df["outcome"] == "TP"].groupby("mode").size()
    total = df.groupby("mode").size()
    rates = (wins / total).fillna(0)

    legacy_wr = float(rates.get("LEGACY", 0.0))
    signal_wr = float(rates.get("SIGNAL", 0.0))

    print(f"LEGACY win rate on differing trades: {legacy_wr:.4f}")
    print(f"SIGNAL win rate on differing trades: {signal_wr:.4f}")

    if signal_wr > legacy_wr:
        print("SIGNAL_SCORE shows better outcome on differing trade choices in this sample.")
    elif signal_wr < legacy_wr:
        print("SIGNAL_SCORE shows worse outcome on differing trade choices in this sample.")
    else:
        print("SIGNAL_SCORE shows no outcome advantage on differing trade choices in this sample.")