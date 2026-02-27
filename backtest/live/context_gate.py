from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Any
import os

from backtest.live.macro_gate import macro_gate
from backtest.live.news_gate import load_news_events_csv, news_gate


def _coerce_pathlike(v: Any, default: str) -> Path:
    """Best-effort conversion to Path.

    This function exists because we previously had cases where a wrong object
    (e.g., a GateDecision) was accidentally passed into Path(). That produces:
      TypeError: argument should be a str or an os.PathLike object
    In that case we fall back to a sensible default.
    """
    if v is None:
        return Path(default)
    if isinstance(v, Path):
        return v
    # os.PathLike is supported by Path() directly
    if isinstance(v, (str, bytes)):
        s = str(v).strip()
        return Path(s if s else default)
    try:
        return Path(v)  # may work for PathLike
    except TypeError:
        return Path(default)

@dataclass
class GateDecision:
    allow_trade: bool
    risk_multiplier: float
    reason: str = ""

    # --- OPTIONAL DEBUG BREAKDOWN ---
    macro_allow: Optional[bool] = None
    macro_reason: Optional[str] = None
    news_allow: Optional[bool] = None
    news_reason: Optional[str] = None
    liq_allow: Optional[bool] = None
    liq_reason: Optional[str] = None


def aggregate_gates(*gates: GateDecision) -> GateDecision:
    if not gates:
        return GateDecision(True, 1.0, "CONTEXT")

    allow = True
    risk = 1.0
    tags: List[str] = []

    for g in gates:
        if g is None:
            continue
        allow = allow and bool(g.allow_trade)
        try:
            risk = min(float(risk), float(g.risk_multiplier))
        except Exception:
            pass

        r = str(getattr(g, "reason", "") or "")
        if r:
            tag = r.split(":", 1)[0].strip()
            tags.append(tag or r)

    seen = set()
    uniq = []
    for t in tags:
        if t not in seen:
            uniq.append(t)
            seen.add(t)

    return GateDecision(allow_trade=allow, risk_multiplier=float(risk), reason="+".join(uniq) or "CONTEXT")


def compute_context_gate(
    *,
    macro_dir: str | Path,
    news_events_csv: str | Path,
    liq_gate: Optional[GateDecision] = None,
    now_utc=None,
) -> GateDecision:
    # Be defensive about path inputs – prevents CONTEXT_FALLBACK TypeError.
    macro_path = _coerce_pathlike(macro_dir, default="data")

    # news_events_csv can be either a path (str/Path) OR a preloaded DataFrame/list (dashboard usage)
    if isinstance(news_events_csv, (str, Path, os.PathLike)):
        news_path = _coerce_pathlike(news_events_csv, default="data/news_events.csv")
        events = load_news_events_csv(news_path)
    else:
        news_path = None
        events = news_events_csv

    m = macro_gate(macro_path)
    n = news_gate(events, now_utc=now_utc)

    # Normalize to GateDecision (so aggregate_gates is stable).
    g_macro = GateDecision(bool(m.allow_trade), float(m.risk_multiplier), str(getattr(m, "reason", "")))
    g_news = GateDecision(bool(n.allow_trade), float(n.risk_multiplier), str(getattr(n, "reason", "")))
    gates: List[GateDecision] = [g_macro, g_news]
    if liq_gate is not None:
        # Allow passing in any object that has allow_trade/risk_multiplier/reason.
        try:
            gates.append(
                GateDecision(
                    bool(getattr(liq_gate, "allow_trade")),
                    float(getattr(liq_gate, "risk_multiplier", 1.0)),
                    str(getattr(liq_gate, "reason", "")),
                )
            )
        except Exception:
            # If liq_gate is malformed, ignore it rather than hard-failing context.
            pass

    out = aggregate_gates(*gates)

    # Attach breakdown fields for dashboard/audit (B2)
    out.macro_allow = g_macro.allow_trade
    out.macro_reason = g_macro.reason
    out.news_allow = g_news.allow_trade
    out.news_reason = g_news.reason
    if liq_gate is not None:
        out.liq_allow = bool(getattr(liq_gate, "allow_trade", None))
        out.liq_reason = str(getattr(liq_gate, "reason", "")) or None
    else:
        out.liq_allow = None
        out.liq_reason = None
    return out
