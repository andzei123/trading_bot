import pandas as pd

legacy = pd.read_csv("backtest/journal/legacy_signals_live_full.csv")
signal = pd.read_csv("backtest/journal/signal_score_signals_live_full.csv")

l = legacy[legacy["symbol"] == "SOLUSDT"].copy()
s = signal[signal["symbol"] == "SOLUSDT"].copy()

# imame tik tuos stulpelius, kurie egzistuoja
wanted_cols = [
    "model","side","entry","tp","sl",
    "score","signal_score",
    "phase","macro_bias",
    "regime","ctx_sub_label","rr"
]

cols = [c for c in wanted_cols if c in l.columns]

print("\n=== LEGACY SOL ===")
print(l[cols])

print("\n=== SIGNAL_SCORE SOL ===")
print(s[cols])

# papildoma analizė (jei yra entry/sl)
if "entry" in l.columns and "sl" in l.columns:
    print("\n=== LEGACY SORTED BY ENTRY (SHORT first) ===")
    print(l.sort_values("entry", ascending=False)[["entry","tp","sl"]])

    l["risk"] = abs(l["entry"] - l["sl"])
    print("\n=== LEGACY SORTED BY RISK (tightest first) ===")
    print(l.sort_values("risk")[["entry","sl","risk"]])