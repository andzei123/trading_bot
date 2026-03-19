from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd


@dataclass
class MacroEvaluation:
    ctx: pd.DataFrame
    macro_dec: dict[str, Any]
    macro_bias_hint: str
    macro_phase_hint: str
    macro_strength_hint: Any
    cross_asset_regime: str
    cross_asset_strength: float
    cross_asset_reason: str
    cross_asset_status: str


def _compute_macro_dec(compute_macro_gate: Callable | None) -> dict[str, Any]:
    macro_dec: dict[str, Any] = {}

    try:
        if compute_macro_gate is not None:
            try:
                macro_dec = compute_macro_gate(macro_dir="data/macro")  # type: ignore[arg-type]
            except TypeError:
                macro_dec = compute_macro_gate()  # type: ignore[call-arg]
            except Exception:
                macro_dec = compute_macro_gate(macro_dir="data")  # type: ignore[arg-type]
    except Exception:
        macro_dec = {}

    return macro_dec


def evaluate_macro(
    ctx: pd.DataFrame,
    *,
    compute_macro_gate: Callable | None = None,
    compute_cross_asset_regime: Callable | None = None,
) -> MacroEvaluation:
    """
    Stage 1 controlled extraction:
    - macro gate evaluation
    - macro hint extraction
    - ctx macro enrichment
    - cross-asset enrichment

    No routing ownership here.
    """
    macro_dec = _compute_macro_dec(compute_macro_gate)

    macro_bias_hint = "NEUTRAL"
    macro_phase_hint = "NA"
    macro_strength_hint = None

    try:
        macro_bias_hint = str(macro_dec.get("macro_bias") or "NEUTRAL").upper()
        macro_phase_hint = str(macro_dec.get("macro_phase") or "NA")
        macro_strength_hint = macro_dec.get("macro_strength", None)
    except Exception:
        macro_bias_hint, macro_phase_hint, macro_strength_hint = "NEUTRAL", "NA", None

    try:
        ctx["macro_bias"] = macro_bias_hint
        ctx["macro_phase"] = macro_phase_hint
        ctx["macro_strength"] = macro_strength_hint
    except Exception:
        pass

    cross_asset_regime = "NEUTRAL"
    cross_asset_strength = 0.0
    cross_asset_reason = "fallback"
    cross_asset_status = "DISABLED"

    try:
        if compute_cross_asset_regime is not None:
            res = compute_cross_asset_regime(
                btc_trend=macro_dec.get("btc_trend"),
                eth_trend=macro_dec.get("eth_trend"),
                total3_trend=macro_dec.get("total3_trend"),
                btcd_trend=macro_dec.get("btcd_trend"),
                dxy_trend=macro_dec.get("dxy_trend", None),
                emit_telemetry=False,
            )

            cross_asset_regime = str(getattr(res, "cross_asset_regime", "NEUTRAL")).upper()
            cross_asset_strength = float(getattr(res, "strength", 0.0))
            cross_asset_reason = str(getattr(res, "reason", "") or "")
            cross_asset_status = "OK"

    except Exception as e:
        cross_asset_regime = "NEUTRAL"
        cross_asset_strength = 0.0
        cross_asset_reason = f"exception={type(e).__name__}"
        cross_asset_status = "DISABLED"

    try:
        ctx["cross_asset_regime"] = cross_asset_regime
        ctx["cross_asset_strength"] = cross_asset_strength
        ctx["cross_asset_reason"] = cross_asset_reason
    except Exception:
        pass

    return MacroEvaluation(
        ctx=ctx,
        macro_dec=macro_dec,
        macro_bias_hint=macro_bias_hint,
        macro_phase_hint=macro_phase_hint,
        macro_strength_hint=macro_strength_hint,
        cross_asset_regime=cross_asset_regime,
        cross_asset_strength=cross_asset_strength,
        cross_asset_reason=cross_asset_reason,
        cross_asset_status=cross_asset_status,
    )
