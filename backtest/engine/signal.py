# backtest/engine/signal.py
class SignalEngine:
    def __init__(self):
        pass

    def evaluate(self, ctx):
        # base
        score = 0.50

        notes = str(ctx.get("notes", "") or "")

        # existing flags (paliekam)
        if "sweep=True" in notes:
            score += 0.10
        if "ob=True" in notes:
            score += 0.10
        if "dev=True" in notes:
            score += 0.05
        if "range=True" in notes:
            score -= 0.05

        # rr bonus (paliekam)
        rr = float(ctx.get("rr", 0.0) or 0.0)
        if rr >= 3.0:
            score += 0.05
        elif rr >= 2.0:
            score += 0.03

        # ✅ NEW: atr_pct (turi duoti variaciją)
        atr_pct = float(ctx.get("atr_pct", 0.0) or 0.0)
        if atr_pct > 0:
            import math

            # platesnis working range (crypto 15m)
            target = 0.010  # 1.0%
            minp = 0.002  # 0.2%
            maxp = 0.030  # 3.0%

            ap = max(minp, min(maxp, atr_pct))

            # log-distance -> 1 prie target, mažėja tolstant
            dist = abs(math.log(ap / target))
            maxdist = max(abs(math.log(minp / target)), abs(math.log(maxp / target)))
            bonus = max(0.0, 1.0 - dist / maxdist)

            score += 0.20 * bonus
        # ✅ NEW: phase (optional, jei turi)
        phase = str(ctx.get("phase", "") or "").upper()
        if "TREND" in phase or phase in ("LONG", "SHORT", "PHASE_TREND_UP", "PHASE_TREND_DOWN"):
            score += 0.05

        # clamp
        if score < 0.0:
            score = 0.0
        if score > 1.0:
            score = 1.0

        return score