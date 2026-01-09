class HardRules:
    def check(self, ctx):
        if not ctx["wyckoff_range"]:
            return False, "NO_RANGE"

        if not ctx["deviation"]:
            return False, "NO_DEVIATION"

        if not ctx["liquidity_sweep"]:
            return False, "NO_SWEEP"

        if not ctx["ob_before_sweep"]:
            return False, "INVALID_OB"

        if ctx["rr"] < 3:
            return False, "RR_TOO_LOW"

        if ctx["high_impact_news"]:
            return False, "NEWS_BLOCK"

        return True, "OK"
