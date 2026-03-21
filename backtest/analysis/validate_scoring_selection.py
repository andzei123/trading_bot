"""
VALIDATION ONLY — DO NOT IMPORT INTO RUNTIME

Compare LEGACY vs SIGNAL_SCORE outputs from live_signal_runner.

Usage:
python -m backtest.analysis.validate_scoring_selection \
    --legacy backtest/journal/legacy_signals_live.csv \
    --signal backtest/journal/signal_score_signals_live.csv
"""

import argparse
import pandas as pd


KEY_COLS = [
    "symbol",
    "model",
    "side",
    "entry",
    "tp",
    "sl",
]

DIAG_COLS = [
    "symbol",
    "model",
    "side",
    "entry",
    "tp",
    "sl",
    "rr",
    "regime",
    "ctx_sub_label",
    "score",
    "signal_score",
    "phase",
    "macro_bias",
]


def load(path):
    df = pd.read_csv(path)
    missing = [c for c in KEY_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required key columns in {path}: {missing}")
    df["_key"] = df[KEY_COLS].astype(str).agg("|".join, axis=1)
    return df


def diagnostics(df):
    cols = [c for c in DIAG_COLS if c in df.columns]
    return df[cols] if cols else df.copy()


def safe_mean(df, col):
    if col not in df.columns or len(df) == 0:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(s) == 0:
        return None
    return float(s.mean())


def fmt_num(x, digits=4):
    if x is None or pd.isna(x):
        return "n/a"
    return f"{x:.{digits}f}"


def summary_by_symbol(df_legacy, df_signal):
    symbols = sorted(set(df_legacy["symbol"]).union(df_signal["symbol"]))
    rows = []
    for s in symbols:
        l = df_legacy[df_legacy["symbol"] == s]
        g = df_signal[df_signal["symbol"] == s]
        rows.append({
            "symbol": s,
            "legacy_final_kept": len(l),
            "signal_final_kept": len(g),
            "changed": "yes" if set(l["_key"]) != set(g["_key"]) else "no",
            "legacy_top_model": l["model"].iloc[0] if len(l) and "model" in l.columns else None,
            "signal_top_model": g["model"].iloc[0] if len(g) and "model" in g.columns else None,
            "legacy_top_side": l["side"].iloc[0] if len(l) and "side" in l.columns else None,
            "signal_top_side": g["side"].iloc[0] if len(g) and "side" in g.columns else None,
        })
    return pd.DataFrame(rows)


def changed_rows(df_legacy, df_signal):
    legacy_keys = set(df_legacy["_key"])
    signal_keys = set(df_signal["_key"])
    only_legacy = df_legacy[df_legacy["_key"].isin(legacy_keys - signal_keys)].copy()
    only_signal = df_signal[df_signal["_key"].isin(signal_keys - legacy_keys)].copy()
    return only_legacy, only_signal


def per_symbol_changed(df_legacy, df_signal, symbol):
    l = df_legacy[df_legacy["symbol"] == symbol].copy()
    g = df_signal[df_signal["symbol"] == symbol].copy()
    legacy_keys = set(l["_key"])
    signal_keys = set(g["_key"])
    only_legacy = l[l["_key"].isin(legacy_keys - signal_keys)].copy()
    only_signal = g[g["_key"].isin(signal_keys - legacy_keys)].copy()
    return only_legacy, only_signal


def selection_diagnostics(df_legacy, df_signal):
    rows = []
    for symbol in sorted(set(df_legacy["symbol"]).union(df_signal["symbol"])):
        only_legacy, only_signal = per_symbol_changed(df_legacy, df_signal, symbol)
        if len(only_legacy) == 0 and len(only_signal) == 0:
            continue
        rows.append({
            "symbol": symbol,
            "legacy_changed_rows": len(only_legacy),
            "signal_changed_rows": len(only_signal),
            "legacy_avg_score": safe_mean(only_legacy, "score"),
            "signal_avg_score": safe_mean(only_signal, "score"),
            "legacy_avg_signal_score": safe_mean(only_legacy, "signal_score"),
            "signal_avg_signal_score": safe_mean(only_signal, "signal_score"),
        })
    return pd.DataFrame(rows)


def symbol_verdict(df_legacy, df_signal, symbol):
    only_legacy, only_signal = per_symbol_changed(df_legacy, df_signal, symbol)
    if len(only_legacy) == 0 and len(only_signal) == 0:
        return {
            "symbol": symbol,
            "verdict": "No final selection change",
            "reason": "Final emitted rows are identical.",
        }

    legacy_avg_score = safe_mean(only_legacy, "score")
    signal_avg_score = safe_mean(only_signal, "score")
    legacy_avg_signal = safe_mean(only_legacy, "signal_score")
    signal_avg_signal = safe_mean(only_signal, "signal_score")

    score_improved = (
        legacy_avg_score is not None and signal_avg_score is not None and signal_avg_score > legacy_avg_score
    )
    signal_improved = (
        legacy_avg_signal is not None and signal_avg_signal is not None and signal_avg_signal > legacy_avg_signal
    )

    if score_improved and signal_improved:
        verdict = "Changed with stronger changed-row quality"
    elif score_improved or signal_improved:
        verdict = "Changed with partial quality improvement"
    else:
        verdict = "Changed, but improvement not proven"

    reason = (
        f"score {fmt_num(signal_avg_score)} vs {fmt_num(legacy_avg_score)}; "
        f"signal_score {fmt_num(signal_avg_signal)} vs {fmt_num(legacy_avg_signal)}"
    )
    return {"symbol": symbol, "verdict": verdict, "reason": reason}


def global_verdict(df_legacy, df_signal):
    only_legacy, only_signal = changed_rows(df_legacy, df_signal)
    if len(only_legacy) == 0 and len(only_signal) == 0:
        return "SIGNAL_SCORE is only cosmetically different in this sample: final emitted selections did not change."

    legacy_avg_score = safe_mean(only_legacy, "score")
    signal_avg_score = safe_mean(only_signal, "score")
    legacy_avg_signal = safe_mean(only_legacy, "signal_score")
    signal_avg_signal = safe_mean(only_signal, "signal_score")

    improved_score = (
        legacy_avg_score is not None and signal_avg_score is not None and signal_avg_score > legacy_avg_score
    )
    improved_signal = (
        legacy_avg_signal is not None and signal_avg_signal is not None and signal_avg_signal > legacy_avg_signal
    )

    if improved_score and improved_signal:
        return (
            "SIGNAL_SCORE materially changes selection and shows stronger changed-row quality than LEGACY in this sample."
        )
    if improved_score or improved_signal:
        return (
            "SIGNAL_SCORE changes final trade choice with partial evidence of improvement, but not enough to declare it broadly better yet."
        )
    return (
        "SIGNAL_SCORE changes final trade choice, but improvement is not proven by this sample."
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--legacy", required=True)
    parser.add_argument("--signal", required=True)
    args = parser.parse_args()

    df_legacy = load(args.legacy)
    df_signal = load(args.signal)

    print("\n=== SECTION A — SUMMARY BY SYMBOL ===\n")
    print(summary_by_symbol(df_legacy, df_signal).to_string(index=False))

    only_legacy, only_signal = changed_rows(df_legacy, df_signal)

    print("\n=== SECTION B — CHANGED SELECTIONS ONLY (LEGACY ONLY) ===\n")
    print(diagnostics(only_legacy).to_string(index=False) if len(only_legacy) else "(none)")

    print("\n=== SECTION B — CHANGED SELECTIONS ONLY (SIGNAL_SCORE ONLY) ===\n")
    print(diagnostics(only_signal).to_string(index=False) if len(only_signal) else "(none)")

    print("\n=== SECTION C — SELECTION DIAGNOSTICS ===\n")
    diag = selection_diagnostics(df_legacy, df_signal).copy()
    if len(diag):
        for col in ["legacy_avg_score", "signal_avg_score", "legacy_avg_signal_score", "signal_avg_signal_score"]:
            if col in diag.columns:
                diag[col] = diag[col].map(fmt_num)
        print(diag.to_string(index=False))
    else:
        print("(no changed selections)")

    print("\n=== SECTION D — PRACTICAL VERDICT ===\n")
    for symbol in sorted(set(df_legacy["symbol"]).union(df_signal["symbol"])):
        v = symbol_verdict(df_legacy, df_signal, symbol)
        print(f"{symbol}: {v['verdict']} — {v['reason']}")

    print("\nGLOBAL:")
    print(global_verdict(df_legacy, df_signal))


if __name__ == "__main__":
    main()
