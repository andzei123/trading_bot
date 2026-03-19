from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

log = logging.getLogger(__name__)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _entry_field(entry: Any, field: str, default: Any = None) -> Any:
    try:
        if isinstance(entry, dict):
            return entry.get(field, default)
        return getattr(entry, field, default)
    except Exception:
        return default


def _emit_cluster_decision(entry: Any, *, ranking_source: str, outcome: str, enabled: bool) -> None:
    if not enabled:
        return
    try:
        print(
            f"[CLUSTER_DECISION] symbol={_extract_symbol(entry) or 'UNKNOWN'} "
            f"model={_entry_field(entry, 'model', '')} "
            f"score={_safe_float(_entry_field(entry, 'score', rr_score(entry))):.6f} "
            f"signal_score={_safe_float(_entry_field(entry, 'signal_score', 0.0)):.6f} "
            f"ranking_source={ranking_source} "
            f"outcome={outcome}"
        )
    except Exception:
        return

# ----------------------------
# Asset groups
# ----------------------------
GROUP_BTC = "BTC"
GROUP_ALT_L1 = "ALT_L1"
GROUP_MEME = "MEME"
GROUP_OTHER = "OTHER"

# Minimal defaults (praplėskit pagal jūsų universą)
ALT_L1_BASE = {
    "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "DOT", "MATIC", "LINK", "TRX", "TON",
}
MEME_BASE = {
    "DOGE", "SHIB", "PEPE", "WIF", "BONK", "FLOKI", "BRETT", "MEME",
}


# ----------------------------
# Helpers: symbol / group
# ----------------------------
_SYMBOL_RE = re.compile(r"(?:symbol|asset)\s*=\s*([A-Z0-9_:\-]+)", re.IGNORECASE)

def _extract_symbol(entry: Any) -> Optional[str]:
    """
    Best-effort symbol extraction from Entry object or dict.
    Returns normalized base asset (e.g., BTC from BTCUSDT).
    """
    sym = None

    # dict-like
    if isinstance(entry, dict):
        sym = entry.get("symbol") or entry.get("asset") or entry.get("instrument")

    # object-like
    if sym is None:
        sym = getattr(entry, "symbol", None) or getattr(entry, "asset", None) or getattr(entry, "instrument", None)

    # meta parsing
    if sym is None:
        meta = getattr(entry, "meta", None)
        if isinstance(meta, str) and meta:
            m = _SYMBOL_RE.search(meta)
            if m:
                sym = m.group(1)

    if not sym:
        return None

    s = str(sym).upper().strip()

    # Normalize common quote formats -> base
    # BTCUSDT, BTC/USDT, BTC-USDT, BTC:USDT
    for sep in ("/", "-", ":"):
        if sep in s:
            s = s.split(sep, 1)[0]
            break

    # If endswith common quote currency
    for quote in ("USDT", "USDC", "USD", "PERP"):
        if s.endswith(quote) and len(s) > len(quote):
            s = s[: -len(quote)]
            break

    return s or None

def _extract_side(entry: Any) -> Optional[str]:
    """Best-effort side extraction (LONG/SHORT) from Entry object or dict."""
    side = None
    if isinstance(entry, dict):
        side = entry.get("side") or entry.get("direction")
        if side is None and isinstance(entry.get("signal"), dict):
            side = entry["signal"].get("side") or entry["signal"].get("direction")
    if side is None:
        side = getattr(entry, "side", None) or getattr(entry, "direction", None)
    if side is None:
        sig = getattr(entry, "signal", None)
        side = getattr(sig, "side", None) or getattr(sig, "direction", None) if sig is not None else None
    if not side:
        return None
    s = str(side).upper().strip()
    if s in {"BUY", "LONG"}:
        return "LONG"
    if s in {"SELL", "SHORT"}:
        return "SHORT"
    return s



def default_group_for_symbol(base: Optional[str]) -> str:
    if not base:
        return GROUP_OTHER
    b = base.upper()
    if b == "BTC":
        return GROUP_BTC
    if b in MEME_BASE:
        return GROUP_MEME
    if b in ALT_L1_BASE:
        return GROUP_ALT_L1
    return GROUP_OTHER


# ----------------------------
# Scoring (keep best)
# ----------------------------
def rr_score(entry: Any) -> float:
    """
    Score by RR = abs(tp-entry)/abs(entry-sl). Higher is better.
    Falls back to 0 if missing.
    """
    try:
        entry_px = float(getattr(entry, "entry", entry["entry"]))
        sl = float(getattr(entry, "sl", entry["sl"]))
        tp = float(getattr(entry, "tp", entry["tp"]))
        risk = abs(entry_px - sl)
        if risk <= 0:
            return 0.0
        return abs(tp - entry_px) / risk
    except Exception:
        return 0.0


def model_score_from_meta(entry: Any, default: float = 0.0) -> float:
    """
    Optional: parse model score from meta like 'score=1.23' if you have it.
    """
    try:
        meta = getattr(entry, "meta", None)
        if not isinstance(meta, str):
            return default
        m = re.search(r"score\s*=\s*([-+]?\d*\.?\d+)", meta, re.IGNORECASE)
        return float(m.group(1)) if m else default
    except Exception:
        return default


def signal_score_value(entry: Any) -> float:
    """Best-effort Signal Scoring V1 accessor. Higher is better."""
    try:
        if isinstance(entry, dict):
            v = entry.get("signal_score", entry.get("score", 0.0))
        else:
            v = getattr(entry, "signal_score", getattr(entry, "score", 0.0))
        return float(v)
    except Exception:
        return 0.0


def signal_score_with_fallback(entry: Any) -> float:
    """Opt-in cluster ranking: prefer signal_score, then score, then RR."""
    try:
        if isinstance(entry, dict):
            if entry.get("signal_score") is not None:
                return float(entry.get("signal_score"))
            if entry.get("score") is not None:
                return float(entry.get("score"))
        else:
            v_signal = getattr(entry, "signal_score", None)
            if v_signal is not None:
                return float(v_signal)
            v_score = getattr(entry, "score", None)
            if v_score is not None:
                return float(v_score)
    except Exception:
        pass
    return rr_score(entry)


# ----------------------------
# Main filter
# ----------------------------
@dataclass
class ClusterFilterResult:
    kept: List[Any]
    dropped: List[Any]


def cluster_filter_entries(
    entries: List[Any],
    *,
    max_per_group: int = 1,
    group_fn: Callable[[Optional[str]], str] = default_group_for_symbol,
    score_fn: Callable[[Any], float] = rr_score,
    debug: bool = False,
    ranking_source: str = "RR",
) -> ClusterFilterResult:
    """
    Group entries by asset group (BTC / ALT_L1 / MEME / OTHER).
    If group has more than max_per_group entries in a cycle:
        keep top max_per_group by score_fn, drop the rest.
    """
    if not entries or max_per_group <= 0:
        return ClusterFilterResult(kept=[], dropped=list(entries or []))

    buckets: Dict[str, List[Any]] = {}
    for e in entries:
        base = _extract_symbol(e)
        grp = group_fn(base)
        buckets.setdefault(grp, []).append(e)

    kept: List[Any] = []
    dropped: List[Any] = []

    for grp, lst in buckets.items():
        if len(lst) <= max_per_group:
            kept.extend(lst)
            for e in lst:
                _emit_cluster_decision(e, ranking_source=ranking_source, outcome="kept", enabled=debug)
            log.debug("[CLUSTER_FILTER] group=%s kept=%d dropped=%d", grp, len(lst), 0)
            continue

        ranked = sorted(lst, key=score_fn, reverse=True)
        k = ranked[:max_per_group]
        d = ranked[max_per_group:]
        kept.extend(k)
        dropped.extend(d)

        for e in k:
            _emit_cluster_decision(e, ranking_source=ranking_source, outcome="kept", enabled=debug)
        for e in d:
            _emit_cluster_decision(e, ranking_source=ranking_source, outcome="dropped", enabled=debug)

        log.debug("[CLUSTER_FILTER] group=%s kept=%d dropped=%d", grp, len(k), len(d))

    return ClusterFilterResult(kept=kept, dropped=dropped)


# Convenience wrapper if runner expects just list
def apply_signal_cluster_filter(
    entries: List[Any],
    *,
    max_per_group: int = 1,
    score: str = "RR",  # "RR" or "MODEL_SCORE"; opt-in: "SIGNAL_SCORE"
    phase: Optional[str] = None,
    debug: bool = False,
) -> Tuple[List[Any], List[Any]]:
    score_fn = (
        signal_score_with_fallback
        if score.upper() == "SIGNAL_SCORE"
        else (rr_score if score.upper() == "RR" else (lambda e: model_score_from_meta(e, default=0.0)))
    )
    # Keep `phase` in the wrapper signature for backward compatibility with callers
    # (e.g. live_signal_runner), but do not forward it because
    # `cluster_filter_entries(...)` does not accept or use this argument.
    _ = phase

    res = cluster_filter_entries(
        entries,
        max_per_group=max_per_group,
        score_fn=score_fn,
        debug=debug,
        ranking_source=str(score).upper(),
    )
    return res.kept, res.dropped