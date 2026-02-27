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
                "signal_idx"
            ])

        i = 0
        while (candle := engine.next_candle()) is not None:
            ctx = context_builder.build(i)
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

                notes = notes_raw.replace(",", ";")  # kad nesugadintų CSV
                rr = float(ctx.get("rr", 0.0))

                writer.writerow([
                    entry_idx, ts, reason, side,
                    entry, sl, tp, rr, float(score), notes,
                    result.outcome, result.exit_price, result.exit_idx, result.bars_held,
                    i
                ])
                f.flush()

            i += 1


if __name__ == "__main__":
    main()
