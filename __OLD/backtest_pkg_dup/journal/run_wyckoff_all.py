from __future__ import annotations

import argparse
import itertools
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Core helpers
# -----------------------------
def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    """Simple ATR (SMA of True Range)."""
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev = close.shift(1)
    tr = pd.concat([(high - low).abs(), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def resample_ohlc(c: pd.DataFrame, tf: str) -> pd.DataFrame:
    """Resample candle dataframe (timestamp, open, high, low, close) to tf."""
    h = (
        c.set_index("timestamp")[["open", "high", "low", "close"]]
        .resample(tf)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna()
    )
    return h.reset_index()


def compute_range(h: pd.DataFrame, lookback: int = 40) -> pd.DataFrame:
    """Rolling range with ATR-normalized features."""
    d = h.copy()
    d["atr"] = atr(d, 14)
    d["range_hi"] = d["high"].rolling(lookback).max().shift(1)
    d["range_lo"] = d["low"].rolling(lookback).min().shift(1)

    d["range_width"] = d["range_hi"] - d["range_lo"]
    d["range_width_atr"] = d["range_width"] / d["atr"]

    rw = d["range_width"].replace(0, np.nan)
    d["pos_in_range"] = (d["close"] - d["range_lo"]) / rw  # [0..1]
    return d


def mark_spring_upthrust(h: pd.DataFrame, spring_atr: float = 0.3, upthrust_atr: float = 0.3) -> pd.DataFrame:
    """Mark Wyckoff spring/upthrust candidates on HTF bars."""
    d = h.copy()
    d["spring"] = (d["low"] < d["range_lo"] - spring_atr * d["atr"]) & (d["close"] > d["range_lo"])
    d["upthrust"] = (d["high"] > d["range_hi"] + upthrust_atr * d["atr"]) & (d["close"] < d["range_hi"])
    return d


def find_confirmation(h: pd.DataFrame, i: int, side: str, n: int):
    """
    Returns (entry_ts, entry_price, confirm_bars) or None.
    LONG: within next n bars close > base_bar_high
    SHORT: within next n bars close < base_bar_low
    """
    if n <= 0:
        return None
    base = h.iloc[i]
    future = h.iloc[i + 1 : i + 1 + n]
    if future.empty:
        return None

    if side == "LONG":
        hit = future[future["close"] > base["high"]]
    else:
        hit = future[future["close"] < base["low"]]

    if hit.empty:
        return None

    first = hit.iloc[0]
    # confirm_bars: 1 means next bar, 2 means two bars later, ...
    try:
        confirm_bars = int(first.name - base.name)
    except Exception:
        confirm_bars = int(hit.index[0] - i)
    return first["timestamp"], float(first["close"]), int(confirm_bars)


def simulate_trade_ltf(c_ltf: pd.DataFrame, entry_ts, side: str, entry: float, sl: float, tp: float):
    """
    LTF sim: from entry_ts forward, check first SL/TP hit.
    Note: without intrabar ordering, if both hit in same bar this uses:
    LONG: SL checked before TP; SHORT: SL checked before TP (conservative).
    """
    future = c_ltf[c_ltf["timestamp"] >= entry_ts].copy()
    if future.empty:
        return None

    for _, r in future.iterrows():
        hi = float(r["high"])
        lo = float(r["low"])
        if side == "LONG":
            if lo <= sl:
                return ("LOSS", sl, r["timestamp"])
            if hi >= tp:
                return ("WIN", tp, r["timestamp"])
        else:
            if hi >= sl:
                return ("LOSS", sl, r["timestamp"])
            if lo <= tp:
                return ("WIN", tp, r["timestamp"])

    last = future.iloc[-1]
    return ("NO_HIT", float(last["close"]), last["timestamp"])


# -----------------------------
# Metrics / diagnostics
# -----------------------------
def _mk_metrics(g: pd.DataFrame) -> dict:
    if g.empty:
        return {"trades": 0, "sum_R": 0.0, "exp_R": 0.0, "median_R": 0.0, "winrate": 0.0, "maxDD_R": 0.0}
    r = g["R"].astype(float).to_numpy()
    eq = np.cumsum(r)
    dd = eq - np.maximum.accumulate(eq)
    return {
        "trades": int(len(g)),
        "sum_R": float(np.sum(r)),
        "exp_R": float(np.mean(r)),
        "median_R": float(np.median(r)),
        "winrate": float((g["outcome"] == "WIN").mean()),
        "maxDD_R": float(dd.min()) if len(dd) else 0.0,
    }


def diagnostics(df: pd.DataFrame, worst_n: int = 10, month_detail: str | None = None):
    if df.empty:
        print("No trades for diagnostics.")
        return

    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce")
    d = d.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    d["month"] = d["timestamp"].dt.to_period("M").astype(str)

    # overall
    allm = _mk_metrics(d)
    print("\nDiagnostics overall:")
    print(
        pd.DataFrame(
            [
                {
                    "scope": "ALL",
                    "trades": allm["trades"],
                    "sum_R": allm["sum_R"],
                    "exp_R": allm["exp_R"],
                    "median_R": allm["median_R"],
                    "winrate": allm["winrate"],
                    "maxDD_R": allm["maxDD_R"],
                }
            ]
        ).to_string(index=False)
    )

    # by side
    rows = []
    for side, g in d.groupby("side", sort=True):
        m = _mk_metrics(g)
        rows.append({"side": side, **m})
    by_side = pd.DataFrame(rows).sort_values("side").reset_index(drop=True)
    print("\nDiagnostics by side:")
    print(by_side.to_string(index=False))

    # by month, side
    rows = []
    for (mth, side), g in d.groupby(["month", "side"], sort=True):
        m = _mk_metrics(g)
        rows.append({"month": mth, "side": side, **m})
    by_ms = pd.DataFrame(rows).sort_values(["month", "side"]).reset_index(drop=True)
    print("\nDiagnostics by month, side:")
    print(by_ms[["month", "side", "trades", "sum_R", "exp_R", "median_R", "winrate", "maxDD_R"]].to_string(index=False))

    # worst months
    by_m = (
        d.groupby("month", sort=True, as_index=False)
        .apply(lambda g: pd.Series({"sum_R": _mk_metrics(g)["sum_R"]}), include_groups=False)
        .reset_index(drop=True)
        .sort_values("sum_R")
        .head(5)
    )
    print("\nWorst months (sum_R):")
    print(by_m.to_string(index=False))

    # Buckets printer
    def _bucket_report(name: str, col: str, bins: list[float]):
        x = d.dropna(subset=[col]).copy()
        if x.empty:
            return
        x["bucket"] = pd.cut(x[col].astype(float), bins=bins, include_lowest=True)
        rows2 = []
        for (b, side), g in x.groupby(["bucket", "side"], sort=True, observed=True):
            m = _mk_metrics(g)
            rows2.append({"bucket": str(b), "side": side, **m})
        out = pd.DataFrame(rows2)
        if out.empty:
            return
        out = out.sort_values(["bucket", "side"]).reset_index(drop=True)
        print(f"\nBuckets: {name}")
        print(out[["bucket", "side", "trades", "sum_R", "exp_R", "median_R", "winrate", "maxDD_R"]].to_string(index=False))

    # bins (match your prints)
    _bucket_report("range_width_atr (flatness / chop)", "range_width_atr", [-1e-9, 4, 6, 8])
    _bucket_report("confirm_bars (how fast market confirms)", "confirm_bars", [-1e-9, 1, 2, 3, 5])
    _bucket_report("edge_dist_atr (entry distance from range edge)", "edge_dist_atr", [-1e-9, 0.5, 0.75, 1.0, 1.5, 2.0, 999.0])
    _bucket_report("breakout_depth_atr (spring/upthrust pierce depth)", "breakout_depth_atr", [-1e-9, 0.5, 0.75, 1.0])

    # worst trades
    n = min(int(worst_n), len(d))
    worst = d.sort_values("R").head(n)
    cols = [
        "timestamp",
        "side",
        "outcome",
        "R",
        "range_width_atr",
        "pos_in_range",
        "confirm_bars",
        "edge_dist_atr",
        "breakout_depth_atr",
        "base_timestamp",
        "base_type",
    ]
    print(f"\nWorst {n} trades:")
    print(worst[cols].to_string(index=False))

    # month detail
    if month_detail:
        md = str(month_detail)
        g = d[d["month"] == md].copy()
        if g.empty:
            print(f"\nMonth detail: {md} -> no trades")
        else:
            print(f"\nMonth detail: {md}")
            cols2 = [
                "timestamp",
                "side",
                "entry",
                "sl",
                "tp",
                "outcome",
                "exit_timestamp",
                "R",
                "range_width_atr",
                "pos_in_range",
                "confirm_bars",
                "edge_dist_atr",
                "breakout_depth_atr",
                "base_timestamp",
                "base_type",
            ]
            print(g.sort_values("timestamp")[cols2].to_string(index=False))


# -----------------------------
# Backtest runner (single config)
# -----------------------------
@dataclass(frozen=True)
class PrecomputeKey:
    candles: str
    htf: str
    ltf: str
    lookback: int
    spring_atr: float
    upthrust_atr: float


def _load_and_prepare(key: PrecomputeKey) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    c = pd.read_csv(key.candles, engine="python", on_bad_lines="skip")
    c["timestamp"] = pd.to_datetime(c["timestamp"], errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    h = resample_ohlc(c, key.htf).reset_index(drop=True)
    h = compute_range(h, lookback=key.lookback)
    h = mark_spring_upthrust(h, spring_atr=key.spring_atr, upthrust_atr=key.upthrust_atr)

    ltf = resample_ohlc(c, key.ltf)
    return c, h, ltf


def run_wyckoff(args: argparse.Namespace, precomputed: Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]] = None) -> pd.DataFrame:
    """
    Return trades dataframe for provided args.
    If precomputed provided -> (candles_df, htf_df, ltf_df).
    """
    if precomputed is None:
        key = PrecomputeKey(
            candles=args.candles,
            htf=args.htf,
            ltf=args.ltf,
            lookback=int(args.lookback),
            spring_atr=float(args.spring_atr),
            upthrust_atr=float(args.upthrust_atr),
        )
        _, h, ltf = _load_and_prepare(key)
    else:
        _, h, ltf = precomputed

    trades: list[dict[str, Any]] = []
    tid = 1

    for i in range(len(h)):
        r = h.iloc[i]

        # basic availability checks
        if pd.isna(r["atr"]) or pd.isna(r["range_hi"]) or pd.isna(r["range_lo"]) or pd.isna(r["range_width"]):
            continue
        if not np.isfinite(r["range_width"]) or float(r["range_width"]) <= 0:
            continue

        atrv = float(r["atr"])
        range_hi = float(r["range_hi"])
        range_lo = float(r["range_lo"])

        # range width filter
        rwa = float(r.get("range_width_atr", np.nan))
        if not np.isfinite(rwa) or rwa > float(args.max_range_width_atr):
            continue

        # pos in range
        pos = float(r.get("pos_in_range", np.nan))
        if not np.isfinite(pos):
            continue

        side = None
        base_type = None

        spring_low = None
        upthrust_high = None
        breakout_depth_atr = None

        if bool(r.get("spring", False)):
            # near-edge filter (spring must occur near range low)
            if pos >= float(args.edge_frac):
                continue
            side = "LONG"
            base_type = "SPRING"
            spring_low = float(r["low"])
            breakout_depth_atr = (range_lo - spring_low) / atrv  # pierce depth below range_lo
        elif bool(r.get("upthrust", False)):
            # near-edge filter (upthrust must occur near range high)
            if pos <= (1.0 - float(args.edge_frac)):
                continue
            side = "SHORT"
            base_type = "UPTHRUST"
            upthrust_high = float(r["high"])
            breakout_depth_atr = (upthrust_high - range_hi) / atrv  # pierce depth above range_hi
        else:
            continue

        if breakout_depth_atr is None or not np.isfinite(breakout_depth_atr):
            continue

        # max breakout depth filter
        if args.max_breakout_depth_atr is not None and float(breakout_depth_atr) > float(args.max_breakout_depth_atr):
            continue

        # confirmation
        conf = find_confirmation(h, i, side, int(args.confirm_n))
        if conf is None:
            continue
        entry_ts, entry, confirm_bars = conf

        # max confirm bars filter
        if args.max_confirm_bars is not None and int(confirm_bars) > int(args.max_confirm_bars):
            continue

        # edge distance at entry (how far it already moved away from the edge)
        if side == "LONG":
            edge_dist_atr = (float(entry) - range_lo) / atrv
        else:
            edge_dist_atr = (range_hi - float(entry)) / atrv

        if not np.isfinite(edge_dist_atr):
            continue

        # max edge distance filter (global)
        if args.max_edge_dist_atr is not None and float(edge_dist_atr) > float(args.max_edge_dist_atr):
            continue

        # NEW: for SHORT only - minimum edge distance filter (useful if shorts need "pullback into range")
        if side == "SHORT" and args.short_min_edge_dist_atr is not None:
            if float(edge_dist_atr) < float(args.short_min_edge_dist_atr):
                continue

        # SL/TP
        if side == "LONG":
            sl = float(spring_low - float(args.sl_buf) * atrv)
            risk = float(entry) - sl
            if risk <= 0:
                continue
            tp = float(entry + float(args.rr) * risk)
        else:
            sl = float(upthrust_high + float(args.sl_buf) * atrv)
            risk = sl - float(entry)
            if risk <= 0:
                continue
            tp = float(entry - float(args.rr) * risk)

        res = simulate_trade_ltf(ltf, entry_ts, side, entry, sl, tp)
        if res is None:
            continue
        outcome, exit_price, exit_ts = res

        # R
        if outcome == "WIN":
            R = (tp - entry) / risk if side == "LONG" else (entry - tp) / risk
        elif outcome == "LOSS":
            R = -1.0
        else:
            R = (exit_price - entry) / risk if side == "LONG" else (entry - exit_price) / risk

        trades.append(
            {
                "id": tid,
                "timestamp": entry_ts,
                "side": side,
                "entry": float(entry),
                "sl": float(sl),
                "tp": float(tp),
                "rr": float(args.rr),
                "model": "WYCKOFF",
                "outcome": outcome,
                "exit_price": float(exit_price),
                "exit_timestamp": exit_ts,
                "R": float(R),
                # diagnostics fields
                "range_width_atr": float(rwa),
                "pos_in_range": float(pos),
                "confirm_bars": int(confirm_bars),
                "edge_dist_atr": float(edge_dist_atr),
                "breakout_depth_atr": float(breakout_depth_atr),
                "base_timestamp": r["timestamp"],
                "base_type": base_type,
            }
        )
        tid += 1

    return pd.DataFrame(trades)


# -----------------------------
# Grid search
# -----------------------------
def _parse_list(s: Optional[str], cast=float) -> Optional[list]:
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    # allow JSON lists too
    if s.startswith("["):
        return [cast(x) for x in json.loads(s)]
    return [cast(x.strip()) for x in s.split(",") if x.strip() != ""]


def grid_search(args: argparse.Namespace) -> pd.DataFrame:
    """
    Run grid search over selected params. Saves a summary CSV and prints top rows.
    """
    # Build grid dict: param_name -> list(values)
    grid: dict[str, list[Any]] = {}

    def add_grid(name: str, values: Optional[list[Any]]):
        if values is None or len(values) == 0:
            return
        grid[name] = values

    add_grid("rr", _parse_list(args.grid_rr, float))
    add_grid("sl_buf", _parse_list(args.grid_sl_buf, float))
    add_grid("edge_frac", _parse_list(args.grid_edge_frac, float))
    add_grid("max_range_width_atr", _parse_list(args.grid_max_range_width_atr, float))
    add_grid("confirm_n", _parse_list(args.grid_confirm_n, int))
    add_grid("max_confirm_bars", _parse_list(args.grid_max_confirm_bars, int))
    add_grid("max_edge_dist_atr", _parse_list(args.grid_max_edge_dist_atr, float))
    add_grid("max_breakout_depth_atr", _parse_list(args.grid_max_breakout_depth_atr, float))
    add_grid("short_min_edge_dist_atr", _parse_list(args.grid_short_min_edge_dist_atr, float))

    if not grid:
        raise SystemExit(
            "Grid is empty. Provide at least one --grid-* argument (e.g. --grid-rr 1.0,1.5,2.0)."
        )

    # Precompute cache (speeds grid)
    cache: dict[PrecomputeKey, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]] = {}

    def get_precomputed(a: argparse.Namespace):
        key = PrecomputeKey(
            candles=a.candles,
            htf=a.htf,
            ltf=a.ltf,
            lookback=int(a.lookback),
            spring_atr=float(a.spring_atr),
            upthrust_atr=float(a.upthrust_atr),
        )
        if key not in cache:
            cache[key] = _load_and_prepare(key)
        return cache[key]

    # Iterate combos
    names = list(grid.keys())
    values_product = list(itertools.product(*[grid[k] for k in names]))

    rows = []
    best = None  # (score, metrics, cfg_dict)
    for vals in values_product:
        cfg = dict(zip(names, vals))

        a = SimpleNamespace(**vars(args))
        for k, v in cfg.items():
            setattr(a, k, v)

        trades = run_wyckoff(a, precomputed=get_precomputed(a))
        m = _mk_metrics(trades)

        # filter too-few-trades
        if args.grid_min_trades is not None and m["trades"] < int(args.grid_min_trades):
            continue

        # scoring
        if args.grid_score == "sum_R":
            score = m["sum_R"]
        elif args.grid_score == "exp_R":
            score = m["exp_R"]
        elif args.grid_score == "exp_R_sqrtN":
            score = m["exp_R"] * float(np.sqrt(max(1, m["trades"])))
        elif args.grid_score == "sum_R_over_dd":
            denom = abs(m["maxDD_R"]) if m["maxDD_R"] != 0 else 1e-9
            score = m["sum_R"] / denom
        else:
            score = m["sum_R"]

        row = {**cfg, **m, "score": float(score)}
        rows.append(row)

        if best is None or score > best[0]:
            best = (float(score), m, cfg)

    out = pd.DataFrame(rows)
    if out.empty:
        print("Grid search: no rows (maybe grid_min_trades filtered everything).")
        return out

    out = out.sort_values(["score", "trades"], ascending=[False, False]).reset_index(drop=True)

    outp = Path(args.grid_out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(outp, index=False)
    print(f"Saved grid summary: {outp.resolve()}")

    topn = int(args.grid_top)
    print(f"\nTop {min(topn, len(out))} combos by score='{args.grid_score}':")
    print(out.head(topn).to_string(index=False))

    if args.grid_save_best and best is not None:
        score, m, cfg = best
        a = SimpleNamespace(**vars(args))
        for k, v in cfg.items():
            setattr(a, k, v)
        trades = run_wyckoff(a, precomputed=get_precomputed(a))

        # Save best trades and params
        best_trades_path = Path(args.grid_best_trades_out)
        best_trades_path.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(best_trades_path, index=False)

        best_params_path = Path(args.grid_best_params_out)
        best_params_path.parent.mkdir(parents=True, exist_ok=True)
        best_params_path.write_text(json.dumps({"score": score, "metrics": m, "params": cfg}, indent=2), encoding="utf-8")

        print(f"\nSaved best trades: {best_trades_path.resolve()}")
        print(f"Saved best params: {best_params_path.resolve()}")

    return out


# -----------------------------
# CLI / Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()

    # data + timeframe
    ap.add_argument("--candles", default="backtest/journal/candles_ohlc.csv")
    ap.add_argument("--htf", default="4h")
    ap.add_argument("--ltf", default="15min")
    ap.add_argument("--lookback", type=int, default=40)

    # trade params
    ap.add_argument("--rr", type=float, default=2.0)
    ap.add_argument("--sl-buf", dest="sl_buf", type=float, default=0.25)
    ap.add_argument("--spring-atr", dest="spring_atr", type=float, default=0.3)
    ap.add_argument("--upthrust-atr", dest="upthrust_atr", type=float, default=0.3)

    # range quality filters
    ap.add_argument("--max-range-width-atr", dest="max_range_width_atr", type=float, default=8.0)
    ap.add_argument("--edge-frac", dest="edge_frac", type=float, default=0.25)

    # confirmation
    ap.add_argument("--confirm-n", dest="confirm_n", type=int, default=5)
    ap.add_argument("--confirm-bars", dest="confirm_n", type=int)  # alias

    # optional filters
    ap.add_argument("--max-confirm-bars", type=int, default=None)
    ap.add_argument("--max-edge-dist-atr", type=float, default=None)
    ap.add_argument("--max-breakout-depth-atr", type=float, default=None)

    # NEW: SHORT-only filter
    ap.add_argument("--short-min-edge-dist-atr", type=float, default=None)

    # outputs
    ap.add_argument("--out", default="backtest/journal/exports_trades/wyckoff_trades.csv")

    # diagnostics
    ap.add_argument("--diag-worst-n", type=int, default=10)
    ap.add_argument("--diag-month", type=str, default=None)
    ap.add_argument(
        "--diag-only",
        action="store_true",
        help="Skip backtest, load --out and run diagnostics on existing trades CSV.",
    )

    # grid search mode
    ap.add_argument("--grid", action="store_true", help="Run grid search over parameters specified by --grid-* args.")

    # grid param lists (comma separated or JSON list)
    ap.add_argument("--grid-rr", default=None, help="e.g. 1.0,1.5,2.0")
    ap.add_argument("--grid-sl-buf", dest="grid_sl_buf", default=None, help="e.g. 0.1,0.25,0.5")
    ap.add_argument("--grid-edge-frac", dest="grid_edge_frac", default=None, help="e.g. 0.2,0.25,0.3")
    ap.add_argument("--grid-max-range-width-atr", dest="grid_max_range_width_atr", default=None, help="e.g. 6,8,10")
    ap.add_argument("--grid-confirm-n", dest="grid_confirm_n", default=None, help="e.g. 3,5,8")
    ap.add_argument("--grid-max-confirm-bars", dest="grid_max_confirm_bars", default=None, help="e.g. 1,2,3")
    ap.add_argument("--grid-max-edge-dist-atr", dest="grid_max_edge_dist_atr", default=None, help="e.g. 0.75,1.0,1.25")
    ap.add_argument("--grid-max-breakout-depth-atr", dest="grid_max_breakout_depth_atr", default=None, help="e.g. 0.5,0.75,1.0")
    ap.add_argument("--grid-short-min-edge-dist-atr", dest="grid_short_min_edge_dist_atr", default=None, help="e.g. 0.5,1.0,1.5")

    # grid options
    ap.add_argument("--grid-min-trades", type=int, default=10, help="Skip combos with fewer trades than this.")
    ap.add_argument("--grid-score", default="exp_R_sqrtN", choices=["sum_R", "exp_R", "exp_R_sqrtN", "sum_R_over_dd"])
    ap.add_argument("--grid-top", type=int, default=20)
    ap.add_argument("--grid-out", default="backtest/journal/exports_trades/wyckoff_grid.csv")
    ap.add_argument("--grid-save-best", action="store_true")
    ap.add_argument("--grid-best-trades-out", default="backtest/journal/exports_trades/wyckoff_best_trades.csv")
    ap.add_argument("--grid-best-params-out", default="backtest/journal/exports_trades/wyckoff_best_params.json")

    args = ap.parse_args()

    # Diagnostics-only mode (quick inspect)
    if args.diag_only:
        inp = Path(args.out)
        if not inp.exists():
            raise SystemExit(f"Missing trades file: {inp}")
        df = pd.read_csv(inp)
        print("Loaded trades:", str(inp))
        if df.empty:
            print("Trades CSV is empty.")
            return
        r = df["R"].astype(float)
        eq = r.cumsum()
        dd = eq - eq.cummax()
        print(
            f"Wyckoff trades={len(df)} sum_R={r.sum():.4f} exp_R={r.mean():.6f} "
            f"median_R={r.median():.6f} maxDD_R={dd.min():.4f}"
        )
        diagnostics(df, worst_n=int(args.diag_worst_n), month_detail=args.diag_month)
        return

    # Grid search mode
    if args.grid:
        grid_search(args)
        return

    # Single run
    df = run_wyckoff(args)

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outp, index=False)

    if df.empty:
        print("No wyckoff trades.")
        print("Saved:", outp.resolve())
        return

    r = df["R"].astype(float)
    eq = r.cumsum()
    dd = eq - eq.cummax()
    print(
        f"Wyckoff trades={len(df)} sum_R={r.sum():.4f} exp_R={r.mean():.6f} "
        f"median_R={r.median():.6f} maxDD_R={dd.min():.4f}"
    )
    print("Saved:", outp.resolve())

    diagnostics(df, worst_n=int(args.diag_worst_n), month_detail=args.diag_month)


if __name__ == "__main__":
    main()
