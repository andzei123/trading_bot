class SignalEngine:
    def evaluate(self, ctx: dict) -> float:
        score = 0.0

        if ctx.get("deviation", False):
            score += 0.20
        if ctx.get("liquidity_sweep", False):
            score += 0.20
        if ctx.get("wyckoff_event", None):
            score += 0.15

        # Wick confirmations (optional)
        if ctx.get("stop_hunt_wick", False):
            score += 0.10
        if ctx.get("rejection_wick", False):
            score += 0.15

        if ctx.get("ob_before_sweep", False):
            score += 0.15
        if ctx.get("pattern", None):
            score += 0.05

        return float(score)
