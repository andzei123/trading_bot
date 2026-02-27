from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any
import os
import time
import csv
import math
import json
from datetime import datetime, timezone

@dataclass(frozen=True)
class GateDecision:
    allow_trade: bool
    risk_multiplier: float
    reason: str
    bias: str = "NEUTRAL"


# ============================================================
# Macro loader (CWD-proof)
#   Feeder output contract: data/macro/{NAME}_{TF}.csv
#     TF examples: 1D, 1W, 4h
# ============================================================

def _default_macro_dir() -> Path:
    """Find <repo_root>/data/macro regardless of current working directory."""
    here = Path(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        cand = p / "data" / "macro"
        if cand.exists() and cand.is_dir():
            return cand
    # fallback: relative to cwd
    return Path("data") / "macro"


def _normalize_macro_dir(macro_dir: str | Path | None) -> Path:
    """Accept folder, 'data', or accidental file paths (incl. file:/.../_meta.json)."""
    if macro_dir is None:
        return _default_macro_dir()
    s = str(macro_dir)
    if s.startswith("file:"):
        s = s.replace("file:///", "").replace("file://", "").replace("file:", "")
    p = Path(s)
    # if a file is passed, use its parent dir
    if p.suffix.lower() in {".json", ".csv"}:
        p = p.parent
    # allow passing 'data' to mean 'data/macro'
    if p.name.lower() == "data":
        cand = p / "macro"
        if cand.exists() and cand.is_dir():
            p = cand
    # if not already macro dir, try appending /data/macro relative to this location
    if p.name.lower() != "macro":
        cand = p / "data" / "macro"
        if cand.exists() and cand.is_dir():
            p = cand
        else:
            cand2 = p / "macro"
            if cand2.exists() and cand2.is_dir():
                p = cand2
    return p


def _load_meta(macro_dir: Path) -> dict:
    try:
        meta_path = macro_dir / "_meta.json"
        if not meta_path.exists():
            return {}
        with meta_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _tf_candidates(tf: str) -> List[str]:
    t = (tf or "").strip()
    tl = t.lower()
    if tl in {"1d", "d", "1day", "day"}:
        return ["1D", "1d"]
    if tl in {"1w", "w", "1week", "week"}:
        return ["1W", "1w"]
    if tl in {"4h", "h4", "4hr", "4hours"}:
        return ["4h", "4H"]
    # unknown: try as-is plus common case flips
    out = [t]
    if t.upper() != t:
        out.append(t.upper())
    if t.lower() != t:
        out.append(t.lower())
    # de-dupe keep order
    seen = set()
    uniq = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _read_close_series(csv_path: Path, *, max_rows: int = 600) -> List[Tuple[str, float]]:
    """Read up to last max_rows of (timestamp, close). Handles common column names."""
    if not csv_path.exists():
        return []
    rows: List[Tuple[str, float]] = []
    try:
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                ts = (r.get("timestamp") or r.get("ts") or r.get("time") or r.get("date") or "").strip()
                c = r.get("close") or r.get("Close") or r.get("c")
                if c is None:
                    continue
                try:
                    close = float(c)
                except Exception:
                    continue
                rows.append((ts, close))
        if len(rows) > max_rows:
            rows = rows[-max_rows:]
        return rows
    except Exception:
        return []



def _is_stale(path: Path, tf: str) -> bool:
    """Staleness check that avoids 'flapping' (meta vs csv mismatch)."""
    if not path.exists():
        return True

    # thresholds (seconds) — tolerate feeder not running exactly on TF boundary
    tl = (tf or "").lower()
    if "4h" in tl:
        max_age = 16 * 3600        # 16h
    elif "1w" in tl or tl == "w":
        max_age = 21 * 24 * 3600   # 21d
    else:
        max_age = 72 * 3600        # 72h for 1D/unknown

    macro_dir = path.parent
    meta = _load_meta(macro_dir)

    fresh_candidates: list[float] = []

    # meta.generated_at_utc
    try:
        gen = meta.get("generated_at_utc")
        if gen:
            gen_dt = datetime.fromisoformat(str(gen).replace("Z", "+00:00"))
            fresh_candidates.append(gen_dt.timestamp())
    except Exception:
        pass

    # meta.files[...] last_ts
    try:
        files = meta.get("files") or {}
        key = path.name
        info = files.get(key) or files.get(key.replace(".csv", "")) or {}
        last_ts = info.get("last_ts") or info.get("last_timestamp") or info.get("last_dt")
        if last_ts:
            # accept "YYYY-MM-DD HH:MM:SS" or iso
            s = str(last_ts).replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            fresh_candidates.append(dt.timestamp())
    except Exception:
        pass

    # file mtime
    try:
        fresh_candidates.append(path.stat().st_mtime)
    except Exception:
        pass

    if not fresh_candidates:
        return True

    freshest = max(fresh_candidates)
    age = time.time() - freshest
    return age > max_age



def load_macro_series(name: str, tf: str, macro_dir: str | Path | None = None):
    """Load feeder macro series from data/macro/*.csv.

    Returns: (series_rows, status, path_used)
      series_rows: list[(ts, close)]
      status: OK / MISSING / STALE
    """
    base = _normalize_macro_dir(macro_dir)
    sym = (name or "").strip().upper()
    used: Optional[Path] = None

    for t in _tf_candidates(tf):
        p = base / f"{sym}_{t}.csv"
        if p.exists():
            used = p
            break

    if used is None:
        return [], "MISSING", str(base / f"{sym}_{_tf_candidates(tf)[0]}.csv")

    status = "OK"
    if _is_stale(used, tf):
        status = "STALE"

    series = _read_close_series(used)
    if not series:
        # treat empty as stale (safer)
        status = "STALE"
    return series, status, str(used)


# ============================================================
# Macro rule engine
# ============================================================

def _trend_from_close(series: List[Tuple[str, float]]) -> str:
    """Return UP/DOWN/FLAT using last close vs SMA50 with small deadzone.

    DEV4/DoD requirement: never return None (fail-open to FLAT).
    """
    if len(series) < 80:
        return "FLAT"
    closes = [c for _, c in series[-80:]]
    last = closes[-1]
    sma50 = sum(closes[-50:]) / 50.0
    if sma50 <= 0:
        return "FLAT"
    # 0.2% deadzone
    if last > sma50 * 1.002:
        return "UP"
    if last < sma50 * 0.998:
        return "DOWN"
    return "FLAT"


def _bias_from_signals(btc_d_trend: str, stables_trend: str) -> str:
    # Simple macro: BTC.D up or stables dom up => ALT_SHORT (risk-off)
    # BTC.D down and stables not up => ALT_LONG (risk-on)
    if btc_d_trend == "UP" or stables_trend == "UP":
        return "ALT_SHORT"
    if btc_d_trend == "DOWN" and stables_trend != "UP":
        return "ALT_LONG"
    return "NEUTRAL"


def _strong_up_from_close(series: List[Tuple[str, float]]) -> bool:
    """Return True if last close is meaningfully above SMA50 (strong up move)."""
    if len(series) < 80:
        return False
    closes = [c for _, c in series[-80:]]
    last = closes[-1]
    sma50 = sum(closes[-50:]) / 50.0
    if sma50 <= 0:
        return False
    return last > sma50 * 1.01  # 1% above SMA50 = strong up


def compute_rotation_phase(
    btc_d_trend_1d: str,
    total2_series_1d: List[Tuple[str, float]],
) -> str:
    """
    Rotation Phase Classifier.

    Rules (priority):
      - TOTAL2 strong up -> ALT_MID
      - BTC.D up & TOTAL2 flat -> BTC_PHASE
      - BTC.D down & TOTAL2 up -> ALT_EARLY
    """
    total2_tr = _trend_from_close(total2_series_1d)  # UP/DOWN/FLAT
    if _strong_up_from_close(total2_series_1d):
        phase = "ALT_MID"
    elif btc_d_trend_1d == "UP" and total2_tr == "FLAT":
        phase = "BTC_PHASE"
    elif btc_d_trend_1d == "DOWN" and total2_tr == "UP":
        phase = "ALT_EARLY"
    else:
        phase = "ROTATION_NA"

    print(f"[ROTATION_PHASE] {phase}")
    return phase


def apply_rotation_allocation(rotation_phase: str) -> Tuple[float, float, float]:
    """
    Portfolio Rotation Allocator.

    Returns:
      alt_multiplier, btc_multiplier, rotation_risk_multiplier (context-level)
    """
    alt_multiplier = 1.0
    btc_multiplier = 1.0

    if rotation_phase == "BTC_PHASE":
        btc_multiplier = 1.2
    elif rotation_phase == "ALT_EARLY":
        alt_multiplier = 1.2
    elif rotation_phase == "ALT_MID":
        alt_multiplier = 1.5

    rotation_risk_multiplier = max(alt_multiplier, btc_multiplier)
    print(f"[ROTATION_ALLOC] phase={rotation_phase} alt_multiplier={alt_multiplier} btc_multiplier={btc_multiplier}")
    return alt_multiplier, btc_multiplier, rotation_risk_multiplier

def compute_macro_gate(macro_dir: str | Path | None = None) -> Dict[str, Any]:
    """
    Returns:
      macro_phase, macro_bias, macro_strength, risk_multiplier, macro_reason
    """
    loader_tag = "loader=macro_gate_loader_v3"

    # --- Load series ---

    btc_1d, s_btc_1d, _ = load_macro_series("BTC", "1D", macro_dir)
    btc_1w, s_btc_1w, _ = load_macro_series("BTC", "1W", macro_dir)

    eth_1d, s_eth_1d, _ = load_macro_series("ETH", "1D", macro_dir)
    eth_1w, s_eth_1w, _ = load_macro_series("ETH", "1W", macro_dir)

    dxy_1d, s_dxy_1d, _ = load_macro_series("DXY", "1D", macro_dir)
    dxy_1w, s_dxy_1w, _ = load_macro_series("DXY", "1W", macro_dir)

    total3_1d, s_total3_1d, _ = load_macro_series("TOTAL3", "1D", macro_dir)
    total3_1w, s_total3_1w, _ = load_macro_series("TOTAL3", "1W", macro_dir)
    btc_d_1d, s_btc_d_1d, _ = load_macro_series("BTC.D", "1D", macro_dir)
    btc_d_1w, s_btc_d_1w, _ = load_macro_series("BTC.D", "1W", macro_dir)

    usdt_d_1d, s_usdt_d_1d, _ = load_macro_series("USDT.D", "1D", macro_dir)
    usdc_d_1d, s_usdc_d_1d, _ = load_macro_series("USDC.D", "1D", macro_dir)

    total2_1d, s_total2_1d, _ = load_macro_series("TOTAL2", "1D", macro_dir)
    total2_1w, s_total2_1w, _ = load_macro_series("TOTAL2", "1W", macro_dir)

    total3_4h, s_total3_4h, _ = load_macro_series("TOTAL3", "4h", macro_dir)
    ethbtc_1d, s_ethbtc_1d, _ = load_macro_series("ETHBTC", "1D", macro_dir)
    ethbtc_1w, s_ethbtc_1w, _ = load_macro_series("ETHBTC", "1W", macro_dir)

    # --- Trends ---

    btc_tr_1d = _trend_from_close(btc_1d)
    btc_tr_1w = _trend_from_close(btc_1w)

    eth_tr_1d = _trend_from_close(eth_1d)
    eth_tr_1w = _trend_from_close(eth_1w)

    dxy_tr_1d = _trend_from_close(dxy_1d)
    dxy_tr_1w = _trend_from_close(dxy_1w)

    total3_tr_1d = _trend_from_close(total3_1d)
    total3_tr_1w = _trend_from_close(total3_1w)
    btc_d_tr_1d = _trend_from_close(btc_d_1d)
    btc_d_tr_1w = _trend_from_close(btc_d_1w)

    # stables dom combined: if either is UP => stables UP
    usdt_tr = _trend_from_close(usdt_d_1d)
    usdc_tr = _trend_from_close(usdc_d_1d)
    # DEV4/DoD: never None in trends -> fail-open to FLAT
    stables_tr = (
        "UP"
        if (usdt_tr == "UP" or usdc_tr == "UP")
        else ("DOWN" if (usdt_tr == "DOWN" and usdc_tr == "DOWN") else "FLAT")
    )

    # TOTAL2 breakout heuristic: last close vs SMA50
    total2_tr_1d = _trend_from_close(total2_1d)
    total2_tr_1w = _trend_from_close(total2_1w)

    # --- Bias 1D and 1W ---
    bias_1d = _bias_from_signals(btc_d_tr_1d, stables_tr)
    bias_1w = _bias_from_signals(btc_d_tr_1w, "FLAT")

    # --- Phase + strength + risk ---
    macro_phase = "NA"
    if bias_1d == "ALT_LONG":
        macro_phase = "RISK_ON"
    elif bias_1d == "ALT_SHORT":
        macro_phase = "RISK_OFF"

    else:
        macro_phase = "NEUTRAL"

    strength = 0.50
    risk_multiplier = 0.50  # default conservative

    tags: List[str] = []
    # Weekly confirm layer
    if bias_1d in {"ALT_LONG", "ALT_SHORT"} and bias_1w == bias_1d:
        strength += 0.15
        tags.append("WEEKLY_CONFIRM")
    elif bias_1d in {"ALT_LONG", "ALT_SHORT"} and bias_1w in {"ALT_LONG", "ALT_SHORT"} and bias_1w != bias_1d:
        strength -= 0.15
        risk_multiplier = min(risk_multiplier, 0.50)
        tags.append("WEEKLY_CONFLICT")
    else:
        tags.append("WEEKLY_MISSING")

    # Risk multiplier tuning (still conservative)
    if bias_1d == "ALT_LONG" and stables_tr != "UP":
        risk_multiplier = 1.00
    elif bias_1d == "ALT_SHORT":
        risk_multiplier = 0.50


    # Rotation phase + allocation (context-level risk modifier)
    rotation_phase = compute_rotation_phase(btc_d_tr_1d, total2_1d)
    _alt_mult, _btc_mult, rotation_risk_multiplier = apply_rotation_allocation(rotation_phase)
    risk_multiplier *= float(rotation_risk_multiplier)
    # safety caps
    risk_multiplier = max(0.10, min(2.00, float(risk_multiplier)))


    # Strength label for logs/DoD
    strength_label = "HIGH" if strength >= 0.65 else "LOW"

    # Data missing tags (based on statuses)
    missing = []
    for nm, st in [
        ("BTC.D_1D", s_btc_d_1d),
        ("BTC.D_1W", s_btc_d_1w),
        ("BTC_1D", s_btc_1d),
        ("BTC_1W", s_btc_1w),
        ("ETH_1D", s_eth_1d),
        ("ETH_1W", s_eth_1w),
        ("DXY_1D", s_dxy_1d),
        ("DXY_1W", s_dxy_1w),
        ("TOTAL3_1D", s_total3_1d),
        ("TOTAL3_1W", s_total3_1w),
        ("USDT.D_1D", s_usdt_d_1d),
        ("USDC.D_1D", s_usdc_d_1d),
        ("TOTAL2_1D", s_total2_1d),
        ("TOTAL2_1W", s_total2_1w),
                ("ETHBTC_1D", s_ethbtc_1d),
        ("ETHBTC_1W", s_ethbtc_1w),
    ]:
        if st != "OK":
            missing.append(f"{nm}={st}")
    if missing:
        tags.append("DATA_MISSING:" + ",".join(missing))

    macro_reason = (
        "MACRO_GATE: "
        + f"BTC.D(tr1d={btc_d_tr_1d},tr1w={btc_d_tr_1w})"
        + f" | STABLES(tr={stables_tr})"
        + f" | TOTAL2(tr1d={total2_tr_1d},tr1w={total2_tr_1w})"
+ f" | BTC(tr1d={btc_tr_1d},tr1w={btc_tr_1w})"
+ f" | ETH(tr1d={eth_tr_1d},tr1w={eth_tr_1w})"
+ f" | DXY(tr1d={dxy_tr_1d},tr1w={dxy_tr_1w})"
+ f" | TOTAL3(tr1d={total3_tr_1d},tr1w={total3_tr_1w},st4h={s_total3_4h})"
        + f" | ETHBTC(st1d={s_ethbtc_1d},st1w={s_ethbtc_1w})"
        + f" | bias_1d={bias_1d} bias_1w={bias_1w}"
        + f" | macro_strength={strength_label}"
        + f" | ROTATION={rotation_phase}"
        + (f" | tags={','.join(tags)}" if tags else "")
        + f" | {loader_tag}"
        + f" -> bias={bias_1d} risk={risk_multiplier}"
    )

    return {
        "macro_phase": macro_phase,
        "macro_bias": bias_1d,
        "macro_strength": strength_label,
        "btc_trend": btc_tr_1d,
        "eth_trend": eth_tr_1d,
        "total2_trend": total2_tr_1d,
        "btcd_trend": btc_d_tr_1d,
        "dxy_trend": dxy_tr_1d,
        "total3_trend": total3_tr_1d,
        "rotation_phase": rotation_phase,
        "risk_multiplier": float(risk_multiplier),
        "macro_reason": macro_reason,
    }


# ============================================================
# Compatibility: existing context_gate expects macro_gate() -> GateDecision
# ============================================================

def macro_gate(
    macro_dir: str | Path,
    *,
    default_allow_trade: bool = True,
    default_risk_multiplier: float = 0.5,
) -> GateDecision:
    mg = compute_macro_gate(macro_dir)
    # allow_trade stays fail-open unless explicit blocks are introduced later
    allow_trade = bool(default_allow_trade)
    risk = float(mg.get("risk_multiplier", default_risk_multiplier))
    bias = str(mg.get("macro_bias", "NEUTRAL"))
    reason = str(mg.get("macro_reason", "MACRO_GATE: NA"))
    return GateDecision(allow_trade=allow_trade, risk_multiplier=risk, reason=reason, bias=bias)
