from __future__ import annotations

import csv
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional


EVENT_FIELDS = [
    "timestamp_utc",
    "run_id",
    "event_type",
    "symbol",
    "reason",
    "details_json",
    "risk_multiplier",
    "blocked_by_news",
    "blocked_by_macro",
    "blocked_by_liqmap",
]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_error_logger(errors_log_path: str | Path) -> logging.Logger:
    p = Path(errors_log_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("live_errors")
    logger.setLevel(logging.INFO)

    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        handler = RotatingFileHandler(
            str(p), maxBytes=2_000_000, backupCount=5, encoding="utf-8"
        )
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger


def ensure_events_csv(events_csv_path: str | Path) -> None:
    p = Path(events_csv_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists() and p.stat().st_size > 0:
        return
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
        w.writeheader()


def append_event(
    events_csv_path: str | Path,
    *,
    run_id: str,
    event_type: str,
    symbol: str = "",
    reason: str = "",
    details: Optional[Dict[str, Any]] = None,
    risk_multiplier: float = 1.0,
    blocked_by_news: bool = False,
    blocked_by_macro: bool = False,
    blocked_by_liqmap: bool = False,
) -> None:
    ensure_events_csv(events_csv_path)
    p = Path(events_csv_path)
    row = {
        "timestamp_utc": now_utc_iso(),
        "run_id": run_id,
        "event_type": str(event_type),
        "symbol": str(symbol),
        "reason": str(reason),
        "details_json": json.dumps(details or {}, ensure_ascii=False),
        "risk_multiplier": float(risk_multiplier),
        "blocked_by_news": bool(blocked_by_news),
        "blocked_by_macro": bool(blocked_by_macro),
        "blocked_by_liqmap": bool(blocked_by_liqmap),
    }
    with p.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
        w.writerow(row)
