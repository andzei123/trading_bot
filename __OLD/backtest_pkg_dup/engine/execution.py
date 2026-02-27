
from dataclasses import dataclass
import math


@dataclass
class SimResult:
    outcome: str        # "WIN" | "LOSS" | "NO_HIT"
    exit_price: float
    exit_idx: int
    bars_held: int


class ExecutionSimulator:
    def __init__(self, df_15m):
        self.df = df_15m

    def simulate(
        self,
        entry_idx: int,
        side: str,
        entry: float,
        sl: float,
        tp: float,        # šitą ignoruosim ir perskaičiuosim į 2R
        max_bars: int = 200,
        be_at_r: float = 1.0,
        tp_r: float = 2.0,
    ) -> SimResult:
        """
        Variant 1:
          - TP = 2R
          - kai pasiekia +1R -> SL perkeliam į entry (BE)
          - grąžina WIN/LOSS/NO_HIT
        """
        side = side.upper()

        # 1R dydis (kiek kainos vienetų iki SL)
        risk = abs(entry - sl)
        if risk <= 0 or not math.isfinite(risk):
            return SimResult("NO_HIT", float(entry), int(entry_idx), 0)

        if side == "LONG":
            tp2 = entry + tp_r * risk
            be_trigger = entry + be_at_r * risk
        else:  # SHORT
            tp2 = entry - tp_r * risk
            be_trigger = entry - be_at_r * risk

        moved_to_be = False
        cur_sl = sl

        last_idx = min(entry_idx + max_bars, len(self.df) - 1)

        for j in range(entry_idx + 1, last_idx + 1):
            high = float(self.df.iloc[j]["high"])
            low = float(self.df.iloc[j]["low"])

            # 1) jei pasiekė +1R -> perkeliam SL į BE
            if not moved_to_be:
                if side == "LONG":
                    if high >= be_trigger:
                        cur_sl = entry
                        moved_to_be = True
                else:
                    if low <= be_trigger:
                        cur_sl = entry
                        moved_to_be = True

            # 2) tikrinam hit'us (SL/TP)
            # Pastaba: jei vienoje žvakėje pasiekia ir SL ir TP, tvarka čia svarbi.
            # Aš imu konservatyviai: pirma SL, tada TP.
            if side == "LONG":
                if low <= cur_sl:
                    return SimResult("LOSS", float(cur_sl), int(j), int(j - entry_idx))
                if high >= tp2:
                    return SimResult("WIN", float(tp2), int(j), int(j - entry_idx))
            else:  # SHORT
                if high >= cur_sl:
                    return SimResult("LOSS", float(cur_sl), int(j), int(j - entry_idx))
                if low <= tp2:
                    return SimResult("WIN", float(tp2), int(j), int(j - entry_idx))

        # nieko nepasiekė per max_bars
        exit_idx = last_idx
        exit_price = float(self.df.iloc[exit_idx]["close"])
        return SimResult("NO_HIT", exit_price, int(exit_idx), int(exit_idx - entry_idx))

