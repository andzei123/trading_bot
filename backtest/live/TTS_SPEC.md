# TTS PLAYBOOK v1.0 (LOCKED)

## PURPOSE
TTS is a **trend-continuation context module**, not a standalone signal.
It is used to ALLOW or BLOCK entries based on structure and higher-timeframe context.

---

## TIMEFRAMES
- Entry TF: **15m**
- Authority TFs: **1M → 1W → 1D → 4H**

Higher TFs always have veto power.

---

## CORE IDEA
Impulse → Correction (Range) → Liquidity Sweep → Return → Retest → Trend Continuation

Exit ONLY when context breaks.

---

## STATES (STATE MACHINE)
IMPULSE  
→ RANGE_FORMED  
→ LIQUIDITY_SWEEP  
→ RETURN_TO_RANGE  
→ RETEST_READY (ENTRY)  
→ IN_TRADE  
→ EXIT (HTF FLIP)

---

## CONTEXT RULES (MANDATORY)
- Trade only in HTF trend direction
- Phase must be TREND (never RANGE)
- No counter-trend TTS
- Larger TF = higher confidence

---

## ENTRY RULE (15m)
Entry is allowed ONLY when:
- Retest B occurs (touch + confirm)
- HTF trend matches direction
- Phase == TREND

---

## EXIT RULE (NO FIXED TP)
Exit when:
- 4H or 1D trend flips
- Phase changes to RANGE
- Structure invalidates (price returns deep into range)

---

## WHAT IS FORBIDDEN
- Fixed RR targets
- Range trading
- Counter-trend TTS
- Using TTS as a raw signal

---

## VERDICT
TTS is VIABLE only as a HTF-confirmed trend continuation framework.
