from __future__ import annotations

import argparse
import csv
from decimal import Decimal
from pathlib import Path

from .intent import ExecutionIntent, IntentValidationError
from .quantity_converter import QuantityConversionError, convert_intent_to_dry_run_order
from .telemetry import BUILD_COLUMNS, EVENT_COLUMNS, append_csv, utc_now


def process_csv(input_csv: Path, telemetry_dir: Path, qty_step: Decimal) -> tuple[int, int]:
    accepted = 0
    rejected = 0
    events_path = telemetry_dir / "execution_intent_events.csv"
    builds_path = telemetry_dir / "dry_run_order_builds.csv"

    with input_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            event_ts = utc_now()
            try:
                intent = ExecutionIntent.from_row(row)
                order = convert_intent_to_dry_run_order(intent, qty_step=qty_step)
            except (IntentValidationError, QuantityConversionError) as exc:
                rejected += 1
                append_csv(
                    events_path,
                    {
                        "event_ts": event_ts,
                        "event_type": "INTENT_REJECTED_PRE_EXCHANGE",
                        "schema_version": row.get("schema_version", ""),
                        "symbol": row.get("symbol", ""),
                        "model": row.get("model", ""),
                        "side": row.get("side", ""),
                        "canonical_setup_key": row.get("canonical_setup_key", ""),
                        "setup_id": row.get("setup_id", ""),
                        "execution_rank": row.get("execution_rank", ""),
                        "client_order_id": "",
                        "reason": str(exc),
                    },
                    EVENT_COLUMNS,
                )
                continue

            accepted += 1
            append_csv(
                events_path,
                {
                    "event_ts": event_ts,
                    "event_type": "ORDER_BUILD_READY",
                    "schema_version": intent.schema_version,
                    "symbol": intent.symbol,
                    "model": intent.model,
                    "side": intent.side,
                    "canonical_setup_key": intent.canonical_setup_key,
                    "setup_id": intent.setup_id,
                    "execution_rank": intent.execution_rank,
                    "client_order_id": order.client_order_id,
                    "reason": "dry_run_only_no_exchange_submit",
                },
                EVENT_COLUMNS,
            )
            append_csv(
                builds_path,
                {"event_ts": event_ts, **order.to_row()},
                BUILD_COLUMNS,
            )

    return accepted, rejected


def main() -> None:
    parser = argparse.ArgumentParser(description="ATS executor E1 dry-run intent build")
    parser.add_argument("--input_csv", required=True)
    parser.add_argument("--telemetry_dir", required=True)
    parser.add_argument("--qty_step", default="0.000001")
    args = parser.parse_args()

    accepted, rejected = process_csv(
        input_csv=Path(args.input_csv),
        telemetry_dir=Path(args.telemetry_dir),
        qty_step=Decimal(args.qty_step),
    )
    print(f"E1_DRY_RUN accepted={accepted} rejected={rejected}")


if __name__ == "__main__":
    main()
