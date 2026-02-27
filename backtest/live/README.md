TTS PACKAGE (DEV #2)

Files:
- TTS_SPEC.md      -> Trading rules & framework (locked)
- tts_context.py   -> Research-only context module (no MVP impact)

HOW TO USE:
1. Merge HTF context (1M/1W/1D/4H) into your 15m candles.
2. Run build_tts_context() on the 15m dataframe.
3. Use tts_context_long / tts_context_short as ENTRY ALLOW flags.
4. EXIT trades only on HTF trend flip or phase invalidation.

This package is designed to be:
- Non-invasive
- Deterministic
- Aligned with discretionary logic

DO NOT:
- Add fixed TP
- Use without HTF confirmation
