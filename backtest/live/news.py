from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple, Optional


@dataclass(frozen=True)
class NewsEvent:
    title: str
    start_utc: datetime


def _parse_dt_utc(s: str) -> datetime:
    """Parse ISO8601 string into timezone-aware UTC datetime."""
    s = (s or "").strip()
    if not s:
        raise ValueError("empty datetime")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        # treat naive as UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_events(path: str | Path) -> List[NewsEvent]:
    p = Path(path)
    if not p.exists():
        return []
    data = json.loads(p.read_text(encoding="utf-8"))
    events: List[NewsEvent] = []
    if not isinstance(data, list):
        return []
    for item in data:
        try:
            title = str(item.get("title", ""))
            start = _parse_dt_utc(str(item.get("start_utc", "")))
            events.append(NewsEvent(title=title, start_utc=start))
        except Exception:
            continue
    events.sort(key=lambda e: e.start_utc)
    return events


def is_blackout(
    now_utc: datetime,
    events: List[NewsEvent],
    minutes_before: int = 20,
    minutes_after: int = 20,
) -> Tuple[bool, str]:
    """Return (blocked, reason) if now_utc is within blackout window around any event."""
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    now_utc = now_utc.astimezone(timezone.utc)

    before = timedelta(minutes=int(minutes_before))
    after = timedelta(minutes=int(minutes_after))

    for ev in events:
        if (ev.start_utc - before) <= now_utc <= (ev.start_utc + after):
            return True, f"NEWS_BLACKOUT: {ev.title} @ {ev.start_utc.isoformat()}"
    return False, ""
