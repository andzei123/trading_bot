from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import os
import json
import time
from datetime import datetime, timezone

_TRUE = {"1","true","yes","on","y","t"}

def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUE

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip()=="":
        return default
    try:
        return int(float(v))
    except Exception:
        return default

def _read_meta_generated_at(meta_path: Path) -> Optional[float]:
    try:
        if not meta_path.exists():
            return None
        with meta_path.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        gen = meta.get("generated_at_utc")
        if not gen:
            return None
        dt = datetime.fromisoformat(str(gen).replace("Z","+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

@dataclass(frozen=True)
class WatchdogDecision:
    freeze_new_signals: bool
    reason: str
    lag_s: Optional[float]
    macro_age_s: Optional[float]
    log_line: str

def check_feed_watchdog(
    *,
    latest_candle_ts: Optional[datetime] = None,
    macro_meta_path: str | Path = Path("data")/"macro"/"_meta.json",
    now_ts: Optional[float] = None,
) -> WatchdogDecision:
    """Fail-open watchdog. If cannot compute -> ok.

    Env overrides:
      WATCHDOG_FORCE_FREEZE: 1/true/yes/on -> freeze
      WATCHDOG_LAG_THRESHOLD_S: default 1800
      WATCHDOG_MACRO_META_THRESHOLD_S: default 43200 (12h)
    """
    if _env_bool("WATCHDOG_FORCE_FREEZE", False):
        return WatchdogDecision(
            freeze_new_signals=True,
            reason="FORCED",
            lag_s=None,
            macro_age_s=None,
            log_line="[WATCHDOG] FREEZE lag_s=None reason=FORCED",
        )

    try:
        now = float(now_ts if now_ts is not None else time.time())
        lag_threshold = float(_env_int("WATCHDOG_LAG_THRESHOLD_S", 1800))
        meta_threshold = float(_env_int("WATCHDOG_MACRO_META_THRESHOLD_S", 43200))

        lag_s: Optional[float] = None
        if latest_candle_ts is not None:
            dt = latest_candle_ts
            if isinstance(dt, datetime):
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                lag_s = max(0.0, now - dt.timestamp())

        gen_ts = _read_meta_generated_at(Path(macro_meta_path))
        macro_age_s: Optional[float] = None
        if gen_ts is not None:
            macro_age_s = max(0.0, now - gen_ts)

        if lag_s is not None and lag_s > lag_threshold:
            return WatchdogDecision(True, "CANDLE_STALE", lag_s, macro_age_s,
                                    f"[WATCHDOG] FREEZE lag_s={lag_s:.3f} reason=CANDLE_STALE")
        if macro_age_s is not None and macro_age_s > meta_threshold:
            # SOFT WARNING – nefreeze'inam
            ls = "None" if lag_s is None else f"{lag_s:.3f}"
            return WatchdogDecision(
                False,
                "MACRO_META_STALE",
                lag_s,
                macro_age_s,
                f"[WATCHDOG] ok lag_s={ls} threshold_s={lag_threshold:.0f} "
                f"macro_age_s={macro_age_s:.0f} macro_meta=STALE"
            )

        ls = "None" if lag_s is None else f"{lag_s:.3f}"
        ms = "None" if macro_age_s is None else f"{macro_age_s:.3f}"
        return WatchdogDecision(False, "OK", lag_s, macro_age_s,
                                f"[WATCHDOG] ok lag_s={ls} threshold_s={lag_threshold:.0f} macro_age_s={ms}")
    except Exception:
        return WatchdogDecision(False, "OK", None, None, "[WATCHDOG] ok lag_s=None")
