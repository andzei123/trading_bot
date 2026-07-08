from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping


EVENT_COLUMNS = [
    "event_ts",
    "event_type",
    "schema_version",
    "symbol",
    "model",
    "side",
    "canonical_setup_key",
    "setup_id",
    "execution_rank",
    "client_order_id",
    "reason",
]

BUILD_COLUMNS = [
    "event_ts",
    "client_order_id",
    "symbol",
    "side",
    "order_type",
    "qty",
    "entry",
    "sl",
    "tp",
    "notional",
    "risk_usd_after_rounding",
    "risk_not_increased",
    "send_enabled",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_csv(path: str | Path, row: Mapping[str, object], columns: list[str]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    exists = target.exists() and target.stat().st_size > 0
    with target.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        if not exists:
            writer.writeheader()
        writer.writerow({column: row.get(column, "") for column in columns})
