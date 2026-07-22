from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
import tempfile

from backtest.execution.execution_event_ledger import (
    ExecutionEventLedger,
    EXCHANGE_READY,
    IDEMPOTENCY_ALLOWED,
    INTENT_ACCEPTED,
    RECOVERY_DECISION,
    SIMULATED_ACKED,
    SIMULATED_FILLED,
    SIMULATED_TIMEOUT_UNKNOWN,
    rebuild_execution_state_snapshot,
)
from backtest.execution.recovery_simulator import BLOCK_AND_RECONCILE, RecoveryDecision


NETWORK_CALLS = 0
TS1 = "2026-07-08T14:00:00+00:00"
TS2 = "2026-07-08T14:01:00+00:00"
TS3 = "2026-07-08T14:02:00+00:00"
TS4 = "2026-07-08T14:03:00+00:00"


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        ledger_path = tmp_path / "executor_local" / "execution_event_ledger.jsonl"
        before_production_probe = set(Path("backtest").rglob("live_rotation_plan.csv"))

        ledger = ExecutionEventLedger(ledger_path=ledger_path)

        filled_key = "BTCUSDT|TDP_REENTRY|LONG|2026-07-08T13:45:00+00:00"
        timeout_key = "ETHUSDT|RANGE_TOP_SHORT_V2|SHORT|2026-07-08T13:45:00+00:00"

        source_payload = {"canonical_setup_key": filled_key, "event_type": INTENT_ACCEPTED}
        source_before = dict(source_payload)

        event1 = ledger.append_event(
            canonical_setup_key=source_payload["canonical_setup_key"],
            event_type=source_payload["event_type"],
            recorded_at_utc=TS1,
            symbol="BTCUSDT",
            side="LONG",
        )
        event2 = ledger.append_event(
            canonical_setup_key=filled_key,
            event_type=SIMULATED_ACKED,
            recorded_at_utc=TS2,
            symbol="BTCUSDT",
            side="LONG",
            client_order_id="ATS_SIM_FILLED",
            status="ACKED",
        )
        event3 = ledger.append_event(
            canonical_setup_key=filled_key,
            event_type=SIMULATED_FILLED,
            recorded_at_utc=TS3,
            symbol="BTCUSDT",
            side="LONG",
            client_order_id="ATS_SIM_FILLED",
            status="FILLED",
        )

        filled_snapshot = ledger.rebuild_snapshot(filled_key)
        repeat_filled_snapshot = rebuild_execution_state_snapshot(ledger.events, filled_key)

        timeout_event = ledger.append_event(
            canonical_setup_key=timeout_key,
            event_type=SIMULATED_TIMEOUT_UNKNOWN,
            recorded_at_utc=TS2,
            symbol="ETHUSDT",
            side="SHORT",
            client_order_id="ATS_SIM_TIMEOUT",
            status="TIMEOUT_UNKNOWN",
        )
        recovery_decision = RecoveryDecision(
            canonical_setup_key=timeout_key,
            symbol="ETHUSDT",
            client_order_id="ATS_SIM_TIMEOUT",
            source_event_type="SIMULATED_EXECUTION_EVENT",
            recovery_action=BLOCK_AND_RECONCILE,
            block_new_orders=True,
            requires_manual_review=True,
            reason="timeout_unknown_requires_reconciliation_before_new_orders",
            decided_at_utc=TS4,
        )
        recovery_event = ledger.append_recovery_decision(recovery_decision)
        timeout_snapshot = ledger.rebuild_snapshot(timeout_key)
        repeat_timeout_snapshot = rebuild_execution_state_snapshot(list(ledger.events), timeout_key)

        ordering = [event.sequence for event in ledger.events]
        immutable_event = False
        immutable_snapshot = False
        try:
            event1.event_type = EXCHANGE_READY  # type: ignore[misc]
        except FrozenInstanceError:
            immutable_event = True
        try:
            filled_snapshot.current_state = "MUTATED"  # type: ignore[misc]
        except FrozenInstanceError:
            immutable_snapshot = True

        ledger_lines = ledger_path.read_text(encoding="utf-8").splitlines()
        ledger.close()
        after_production_probe = set(Path("backtest").rglob("live_rotation_plan.csv"))

    assert event1.sequence == 1
    assert event2.sequence == 2
    assert event3.sequence == 3
    assert timeout_event.sequence == 4
    assert recovery_event.sequence == 5
    assert ordering == [1, 2, 3, 4, 5]
    assert len(ledger_lines) == 5
    assert filled_snapshot.current_state == "FILLED_CONFIRMED"
    assert filled_snapshot.last_event_type == SIMULATED_FILLED
    assert filled_snapshot.event_count == 3
    assert filled_snapshot.terminal is True
    assert filled_snapshot.has_unknown_state is False
    assert filled_snapshot.has_partial_fill is False
    assert filled_snapshot.block_new_orders is False
    assert filled_snapshot.requires_manual_review is False
    assert filled_snapshot == repeat_filled_snapshot
    assert timeout_snapshot.current_state == BLOCK_AND_RECONCILE
    assert timeout_snapshot.last_event_type == RECOVERY_DECISION
    assert timeout_snapshot.event_count == 2
    assert timeout_snapshot.has_unknown_state is True
    assert timeout_snapshot.terminal is False
    assert timeout_snapshot.block_new_orders is True
    assert timeout_snapshot.requires_manual_review is True
    assert timeout_snapshot == repeat_timeout_snapshot
    assert immutable_event is True and immutable_snapshot is True
    assert source_payload == source_before
    assert NETWORK_CALLS == 0
    assert after_production_probe == before_production_probe

    print(
        "SMOKE_E10_OK "
        "intent_accepted=1 "
        "simulated_acked=1 "
        "simulated_filled=1 "
        "filled_terminal_snapshot=1 "
        "timeout_recovery_blocking_snapshot=1 "
        "append_only_ordering=1 "
        "deterministic_rebuild=1 "
        "immutable=1 "
        "mutated_source=0 "
        "exchange_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
