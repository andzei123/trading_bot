from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class PortfolioExposure:
    positions: List[Dict[str, Any]]
    bucket_used: Dict[str, float]
    last_signal_ts: Dict[str, str]
    last_signal_candle_ts: Dict[str, str]


_DEFAULT_BUCKET_USED: Dict[str, float] = {
    "BTC": 0.0,
    "ALT": 0.0,
    "MEME": 0.0,
    "GLOBAL": 0.0,
}


def _default_state() -> Dict[str, Any]:
    return {
        "positions": [],
        "bucket_used": dict(_DEFAULT_BUCKET_USED),
        "last_signal_ts": {},
        "last_signal_candle_ts": {},
    }


def ensure_portfolio_state_file(path: str | Path) -> Path:
    """
    Non-destructive ensure:
    - If file does not exist -> create with default structure.
    - If exists -> do NOTHING.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        p.write_text(json.dumps(_default_state(), indent=2), encoding="utf-8")
    return p


def load_portfolio_exposure(path: str | Path) -> Dict[str, Any]:
    """
    Read-only loader:
    - Always returns a dict with keys: positions, bucket_used, last_signal_ts, last_signal_candle_ts
    - Fills missing keys with defaults
    - Never overwrites file on disk
    """
    p = Path(path)
    if not p.exists():
        # Do not create here automatically; caller can ensure if needed.
        return _default_state()

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return _default_state()
    except Exception:
        return _default_state()

    # normalize positions
    positions = data.get("positions", [])
    if not isinstance(positions, list):
        positions = []

    # normalize bucket_used
    bu_in = data.get("bucket_used", {})
    if not isinstance(bu_in, dict):
        bu_in = {}

    bucket_used: Dict[str, float] = {}
    for k, default in _DEFAULT_BUCKET_USED.items():
        try:
            bucket_used[k] = float(bu_in.get(k, default) or 0.0)
        except Exception:
            bucket_used[k] = float(default)

    # normalize last_signal maps
    last_signal_ts = data.get("last_signal_ts", {})
    if not isinstance(last_signal_ts, dict):
        last_signal_ts = {}

    last_signal_candle_ts = data.get("last_signal_candle_ts", {})
    if not isinstance(last_signal_candle_ts, dict):
        last_signal_candle_ts = {}

    return {
        "positions": positions,
        "bucket_used": bucket_used,
        "last_signal_ts": dict(last_signal_ts),
        "last_signal_candle_ts": dict(last_signal_candle_ts),
    }


def save_portfolio_state(path: str | Path, state: Dict[str, Any]) -> None:
    """
    Atomic-ish save: write to tmp then replace.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")

    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(p)


def upsert_portfolio_state(
    path: str | Path,
    *,
    positions: Optional[List[Dict[str, Any]]] = None,
    bucket_used: Optional[Dict[str, float]] = None,
    last_signal_ts: Optional[Dict[str, str]] = None,
    last_signal_candle_ts: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Merge-update state and persist.
    - Only overwrites the keys you pass.
    - Preserves existing values for other keys.
    """
    ensure_portfolio_state_file(path)
    state = load_portfolio_exposure(path)

    if positions is not None:
        state["positions"] = positions

    if bucket_used is not None:
        # merge bucket_used with defaults and incoming
        merged = dict(_DEFAULT_BUCKET_USED)
        merged.update(state.get("bucket_used", {}) or {})
        merged.update(bucket_used)
        # force floats
        out_bu: Dict[str, float] = {}
        for k, v in merged.items():
            try:
                out_bu[k] = float(v or 0.0)
            except Exception:
                out_bu[k] = 0.0
        state["bucket_used"] = out_bu

    if last_signal_ts is not None:
        cur = state.get("last_signal_ts", {}) or {}
        if not isinstance(cur, dict):
            cur = {}
        cur.update(last_signal_ts)
        state["last_signal_ts"] = cur

    if last_signal_candle_ts is not None:
        cur = state.get("last_signal_candle_ts", {}) or {}
        if not isinstance(cur, dict):
            cur = {}
        cur.update(last_signal_candle_ts)
        state["last_signal_candle_ts"] = cur

    save_portfolio_state(path, state)
    return state