import csv
from pathlib import Path

from backtest.data.loader import load_candles
from backtest.engine.replay import ReplayEngine
from backtest.engine.context_builder import ContextBuilder
from backtest.engine.rules import HardRules
from backtest.engine.signal import SignalEngine
from backtest.engine.execution import ExecutionSimulator


def find_entry_trigger(df, i: int, side: str, lookahead: int = 5):
    """
    Trigger:
      SHORT -> close < signal_candle.low
      LONG  -> close > signal_candle.high

    Grąžina entry_idx arba None, jei per lookahead nerado.
    """
    if i >= len(df):
        return None

    sig_low = float(df.iloc[i]["low"])
    sig_high = float(df.iloc[i]["high"])

    end = min(len(df) - 1, i + lookahead)
    for j in range(i + 1, end + 1):
        close = float(df.iloc[j]["close"])
        if side == "SHORT" and close < sig_low:
            return j
        if side == "LONG" and close > sig_high:
            return j
    return None


def main():
    df_15m = load_candles("backtest/data/BTCUSDT_15m.csv")

    context_builder = ContextBuilder(df_15m=df_15m)
    engine = ReplayEngine(df_15m, context_builder)

    rules = HardRules()
    signals = SignalEngine()
    executor = ExecutionSimulator(df_15m)

    out_path = Path("backtest/journal/trades.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not out_path.exists()

    with out_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if write_header:
            writer.writerow([
                "idx", "timestamp", "reason", "side",
                "entry", "sl", "tp", "rr", "score", "notes",
                "outcome", "exit_price", "exit_idx", "bars_held",
                "symbol"
            ])

        i = 0
        while (candle := engine.next_candle()) is not None:
            ctx = context_builder.build(i)

            # FAIL-OPEN: jei context_builder neduoda atr_pct, pasiskaičiuojam minimaliai
            if "atr_pct" not in ctx:
                # paprasta aproksimacija: (high-low)/close dabartinei žvakei
                c = float(df_15m.iloc[i]["close"])
                h = float(df_15m.iloc[i]["high"])
                l = float(df_15m.iloc[i]["low"])
                ctx["atr_pct"] = (h - l) / c if c else 0.0
            allowed, reason = rules.check(ctx)
            score = signals.evaluate(ctx) if allowed else 0.0

            if reason == "OK":
                # side iš notes
                notes_raw = str(ctx.get("notes", ""))
                side = "LONG" if "sdir=DOWN" in notes_raw else "SHORT"

                # (jei nori palikti tik SHORT, palik šitą filtrą)
                # if side == "LONG":
                #     i += 1
                #     continue

                # 1) randam trigger entry per artimiausias N žvakes
                entry_idx = find_entry_trigger(df_15m, i, side, lookahead=5)
                if entry_idx is None:
                    i += 1
                    continue

                ts = df_15m.index[entry_idx]
                entry = float(df_15m.iloc[entry_idx]["close"])

                # 2) SL/TP skaičiuojam nuo SIGNAL candle (i), ne nuo entry candle
                if side == "LONG":
                    sl = float(df_15m.iloc[i]["low"])
                    tp = entry + 3.0 * (entry - sl)
                else:
                    sl = float(df_15m.iloc[i]["high"])
                    tp = entry - 3.0 * (sl - entry)

                # 3) simuliacija nuo entry_idx
                result = executor.simulate(
                    entry_idx=entry_idx,
                    side=side,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    max_bars=200
                )

                # idx: gali būti i (signal idx) arba entry_idx — pasirink vieną.
                # Aš siūlau idx=i (trade id pagal signal candle), o entry_idx padėti į notes.

                notes = notes_raw.replace(",", ";")
                notes = f"{notes} entry_idx={entry_idx}"
                # RR apskaičiavimas iš entry/sl/tp (fail-open)
                rr = 0.0
                try:
                    if side == "LONG":
                        denom = (entry - sl)
                        rr = (tp - entry) / denom if denom != 0 else 0.0
                    else:  # SHORT
                        denom = (sl - entry)
                        rr = (entry - tp) / denom if denom != 0 else 0.0
                except Exception:
                    rr = 0.0

                writer.writerow([
                    i,  # idx
                    ts,  # timestamp (entry candle time)
                    reason,  # reason
                    side,  # side
                    entry, sl, tp,  # entry/sl/tp
                    rr, float(score),  # rr/score
                    notes,  # notes (su entry_idx viduj)
                    result.outcome,  # outcome
                    result.exit_price,  # exit_price
                    result.exit_idx,  # exit_idx
                    result.bars_held,  # bars_held
                    "BTCUSDT"  # symbol (paskutinis)
                ])
                f.flush()

            i += 1


if __name__ == "__main__":
    main()
