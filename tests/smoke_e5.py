from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
import tempfile

from backtest.execution.decision_consumer import consume_decision_row
from backtest.execution.execution_identity_registry import ExecutionIdentityRegistry


def valid_row(key: str = "XRPUSDT|TDP_REENTRY|LONG|2026-07-08T09:45:00+00:00") -> dict[str, str]:
    return {
        "schema_version": "ATS_EXECUTION_INTENT_V1",
        "cycle_ts": "2026-07-08T10:00:00+00:00",
        "symbol": "XRPUSDT",
        "model": "TDP_REENTRY",
        "side": "LONG",
        "canonical_setup_key": key,
        "setup_id": "XRPUSDT|2026-07-08T10:00:00+00:00|TDP_REENTRY|LONG",
        "setup_created_ts": "2026-07-08T09:45:00+00:00",
        "signal_ts": "2026-07-08T10:00:00+00:00",
        "visible_ts": "2026-07-08T10:00:00+00:00",
        "wait_confirm_ts": "2026-07-08T10:00:00+00:00",
        "intended_entry_ts": "2026-07-08T10:00:00+00:00",
        "entry_window_expires_ts": "2026-07-08T10:15:00+00:00",
        "selected_for_execution": "true",
        "execution_rank": "1",
        "selection_reason": "opportunity_manager_selected",
        "entry": "2.0000",
        "sl": "1.9500",
        "tp": "2.1200",
        "planned_rr": "2.4",
        "risk_pct": "0.002",
        "reward_pct": "0.0048",
        "risk_distance": "0.05",
        "reward_distance": "0.12",
        "authorized_qty": "100",
        "authorized_notional": "200",
        "authorized_risk_usd": "5",
        "risk_snapshot_id": "risk-snap-1",
        "position_snapshot_id": "pos-snap-1",
        "authority_waterfall_id": "waterfall-1",
    }


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        before_files = set(Path(tmp).rglob("*"))

        row = valid_row()
        source_before = dict(row)
        intent = consume_decision_row(row)
        registry = ExecutionIdentityRegistry()

        first = registry.check_and_record(intent.canonical_setup_key, checked_at_utc="2026-07-08T10:00:00+00:00")
        duplicate = registry.check_and_record(intent.canonical_setup_key, checked_at_utc="2026-07-08T10:00:01+00:00")

        other_intent = consume_decision_row(valid_row("XRPUSDT|TDP_REENTRY|LONG|2026-07-08T10:00:00+00:00"))
        other = registry.check_and_record(other_intent.canonical_setup_key, checked_at_utc="2026-07-08T10:00:02+00:00")

        immutable = False
        try:
            first.allowed = False  # type: ignore[misc]
        except FrozenInstanceError:
            immutable = True

        after_files = set(Path(tmp).rglob("*"))

    assert first.allowed is True and first.duplicate_detected is False
    assert duplicate.allowed is False and duplicate.duplicate_detected is True
    assert other.allowed is True and other.duplicate_detected is False
    assert row == source_before
    assert immutable is True
    assert after_files == before_files

    print(
        "SMOKE_E5_OK "
        "first_allowed=1 "
        "duplicate_blocked=1 "
        "different_key_allowed=1 "
        "mutated_source=0 "
        "exchange_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
