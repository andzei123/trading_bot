from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from backtest.live.phase_router import decide_phase


@dataclass
class RoutePhaseResult:
    ctx: pd.DataFrame
    phase_authority_source: str
    context_phase_pre_guard: str
    phase_scalar: str
    trend_phase_label: str
    tdir: str | None


def route_phase(
    candles: pd.DataFrame,
    *,
    ctx: pd.DataFrame,
    debug_regime: bool = False,
    symbol: str = "",
    now_utc_str_fn=None,
) -> RoutePhaseResult:
    """
    Stage 1 controlled extraction:
    - owns phase routing call
    - owns ctx phase mapping / normalization
    - owns trend_dir -> trend label extraction

    Macro-agnostic by design.
    """
    phase_authority_source = "decide_phase"
    context_phase_pre_guard = "PHASE_RANGE"
    trend_phase_label = "TREND_UNKNOWN"
    tdir = None

    try:
        ph_pre = decide_phase(
            candles=candles,
            trend_long_tag="TDP_REENTRY",
            trend_short_tag="TDP_REENTRY",
            range_short_tag="RANGE_TOP_SHORT_V2",
            allow_range_long=False,
        )

        ph_val = getattr(ph_pre, "phase", None)
        try:
            if isinstance(ph_val, pd.Series):
                ph_val2 = ph_val.dropna()
                ph_val = ph_val2.iloc[-1] if len(ph_val2) else None
            elif isinstance(ph_val, (list, tuple)):
                ph_val = ph_val[-1] if len(ph_val) else None
        except Exception:
            pass

        ph_raw = str(ph_val or "RANGE").upper()
        if ph_raw == "LONG":
            ctx["phase"] = "PHASE_TREND_UP"
        elif ph_raw == "SHORT":
            ctx["phase"] = "PHASE_TREND_DOWN"
        else:
            ctx["phase"] = "PHASE_RANGE"

        try:
            if isinstance(ctx, pd.DataFrame) and ("phase" in ctx.columns):
                context_phase_pre_guard = str(ctx["phase"].iloc[-1])
            else:
                context_phase_pre_guard = str(ctx.get("phase", "PHASE_RANGE"))
        except Exception:
            context_phase_pre_guard = "PHASE_RANGE"

        if debug_regime and now_utc_str_fn is not None:
            print(
                f"[{now_utc_str_fn()}] [PHASE_PRE][{symbol}] "
                f"{ph_raw} | {getattr(ph_pre, 'reason', '')} macro_bias={ctx.get('macro_bias').iloc[-1] if isinstance(ctx, pd.DataFrame) and 'macro_bias' in ctx.columns and len(ctx) else 'NEUTRAL'}"
            )
    except Exception as _e:
        ctx["phase"] = "PHASE_RANGE"
        context_phase_pre_guard = "PHASE_RANGE"
        phase_authority_source = "decide_phase_fallback"
        if debug_regime and now_utc_str_fn is not None:
            print(
                f"[{now_utc_str_fn()}] [PHASE_PRE][{symbol}] "
                f"fallback PHASE_RANGE (error={repr(_e)})"
            )

    phase_scalar = "PHASE_RANGE"
    try:
        if isinstance(ctx, pd.DataFrame) and ("phase" in ctx.columns):
            s = ctx["phase"].dropna()
            if len(s):
                phase_scalar = str(s.iloc[-1]).upper()
        else:
            v = ctx.get("phase", None) if hasattr(ctx, "get") else None
            phase_scalar = str(v or "PHASE_RANGE").upper()
    except Exception:
        phase_scalar = "PHASE_RANGE"

    if phase_scalar not in {"PHASE_TREND_UP", "PHASE_TREND_DOWN", "PHASE_RANGE"}:
        phase_scalar = "PHASE_RANGE"

    try:
        if isinstance(ctx, pd.DataFrame) and "trend_dir" in ctx.columns:
            s = ctx["trend_dir"].dropna()
            tdir = str(s.iloc[-1]).upper() if len(s) else None
        trend_phase_label = f"TREND_{tdir}" if tdir else "TREND_UNKNOWN"
    except Exception:
        tdir = None
        trend_phase_label = "TREND_UNKNOWN"

    return RoutePhaseResult(
        ctx=ctx,
        phase_authority_source=phase_authority_source,
        context_phase_pre_guard=context_phase_pre_guard,
        phase_scalar=phase_scalar,
        trend_phase_label=trend_phase_label,
        tdir=tdir,
    )


def route_model(
    ctx: pd.DataFrame,
    *,
    generate_entries_from_ctx: Callable,
    symbol: str,
    rr: float,
    sl_atr_buffer: float,
    require_impulse_before_tdp: bool,
    impulse_lookback: int,
    impulse_size_atr: float,
    tdp_dev_lookback: int,
    tts_retest_lookback: int,
    debug_entry_filters: bool = False,
) -> list[Any]:
    """
    Stage 1 controlled extraction:
    - owns entry-model invocation
    """
    entries = generate_entries_from_ctx(
        ctx,
        symbol=symbol,
        enable_trend=True,
        enable_range_short=True,
        enable_range_long=False,
        rr=rr,
        sl_atr_buffer=sl_atr_buffer,
        require_impulse_before_tdp=require_impulse_before_tdp,
        impulse_lookback=impulse_lookback,
        impulse_size_atr=impulse_size_atr,
        tdp_dev_lookback=tdp_dev_lookback,
        tts_retest_lookback=tts_retest_lookback,
        debug_entry_filters=bool(debug_entry_filters),
        debug_long_funnel=bool(debug_entry_filters),
    )
    return list(entries or [])
