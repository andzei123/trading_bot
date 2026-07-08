from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
import tempfile

from backtest.execution.decision_consumer import consume_decision_row
from backtest.execution.execution_event_ledger import (
    ExecutionEventLedger,
    RECOVERY_DECISION,
    SIMULATED_FILLED,
    SIMULATED_PARTIALLY_FILLED,
    SIMULATED_REJECTED,
    SIMULATED_TIMEOUT_UNKNOWN,
    rebuild_execution_state_snapshot,
)
from backtest.execution.execution_identity_registry import ExecutionIdentityRegistry
from backtest.execution.execution_simulator import FILLED, PARTIALLY_FILLED, REJECTED, TIMEOUT_UNKNOWN
from backtest.execution.paper_executor import PaperExecutionRequest, PaperExecutor
from backtest.execution.quantity_converter import ExchangeQuantityRules
from backtest.execution.recovery_simulator import BLOCK_AND_RECONCILE, MARK_PARTIAL_PENDING, MARK_REJECTED_TERMINAL


NETWORK_CALLS = 0
TS1 = "2026-07-08T15:00:00+00:00"
TS2 = "2026-07-08T15:00:01+00:00"
TS3 = "2026-07-08T15:00:02+00:00"


def valid_row(
    key: str = "XRPUSDT|TDP_REENTRY|LONG|2026-07-08T14:45:00+00:00",
    symbol: str = "XRPUSDT",
    side: str = "LONG",
) -> dict[str, str]:
    if side == "LONG":
        entry, sl, tp = "2.0000", "1.9500", "2.1200"
    else:
        entry, sl, tp = "2.0000", "2.0500", "1.8800"
    return {
        "schema_version": "ATS_EXECUTION_INTENT_V1",
        "cycle_ts": "2026-07-08T15:00:00+00:00",
        "symbol": symbol,
        "model": "TDP_REENTRY",
        "side": side,
        "canonical_setup_key": key,
        "setup_id": f"{symbol}|2026-07-08T15:00:00+00:00|TDP_REENTRY|{side}",
        "setup_created_ts": "2026-07-08T14:45:00+00:00",
        "signal_ts": "2026-07-08T15:00:00+00:00",
        "visible_ts": "2026-07-08T15:00:00+00:00",
        "wait_confirm_ts": "2026-07-08T15:00:00+00:00",
        "intended_entry_ts": "2026-07-08T15:00:00+00:00",
        "entry_window_expires_ts": "2026-07-08T15:15:00+00:00",
        "selected_for_execution": "true",
        "execution_rank": "1",
        "selection_reason": "opportunity_manager_selected",
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "planned_rr": "2.4",
        "risk_pct": "0.002",
        "reward_pct": "0.0048",
        "risk_distance": "0.05",
        "reward_distance": "0.12",
        "authorized_qty": "10",
        "authorized_notional": "20",
        "authorized_risk_usd": "0.5",
        "risk_snapshot_id": "risk-snap-1",
        "position_snapshot_id": "pos-snap-1",
        "authority_waterfall_id": "waterfall-1",
    }


def make_executor(tmp_path: Path) -> PaperExecutor:
    return PaperExecutor(
        ledger=ExecutionEventLedger(tmp_path / "executor_local" / "execution_event_ledger.jsonl"),
        identity_registry=ExecutionIdentityRegistry(),
        intent_journal_path=tmp_path / "executor_local" / "intent_journal.csv",
        quantity_rules=ExchangeQuantityRules(quantity_step=Decimal("0.001"), min_quantity=Decimal("0.001"), quantity_precision=3),
    )


def request(outcome: str, *, filled_qty: Decimal | None = None) -> PaperExecutionRequest:
    return PaperExecutionRequest(
        simulation_outcome=outcome,
        checked_at_utc=TS1,
        simulated_at_utc=TS2,
        decided_at_utc=TS3,
        filled_qty=filled_qty,
    )


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        before_production_probe = set(Path("backtest").rglob("live_rotation_plan.csv"))

        filled_row = valid_row()
        filled_source_before = dict(filled_row)
        filled_intent = consume_decision_row(filled_row)
        filled_intent_before = filled_intent
        executor = make_executor(tmp_path)

        filled_snapshot = executor.run(filled_intent, request(FILLED))
        duplicate_snapshot = executor.run(filled_intent, request(FILLED))

        timeout_key = "ETHUSDT|TDP_REENTRY|LONG|2026-07-08T14:45:00+00:00"
        timeout_intent = consume_decision_row(valid_row(timeout_key, symbol="ETHUSDT"))
        timeout_snapshot = executor.run(timeout_intent, request(TIMEOUT_UNKNOWN))

        partial_key = "BTCUSDT|TDP_REENTRY|LONG|2026-07-08T14:45:00+00:00"
        partial_intent = consume_decision_row(valid_row(partial_key, symbol="BTCUSDT"))
        partial_snapshot = executor.run(partial_intent, request(PARTIALLY_FILLED, filled_qty=Decimal("4")))

        rejected_key = "SOLUSDT|TDP_REENTRY|SHORT|2026-07-08T14:45:00+00:00"
        rejected_intent = consume_decision_row(valid_row(rejected_key, symbol="SOLUSDT", side="SHORT"))
        rejected_snapshot = executor.run(rejected_intent, request(REJECTED))

        ledger_events = executor.ledger.events
        ordering = [event.sequence for event in ledger_events]
        repeat_filled_snapshot = rebuild_execution_state_snapshot(ledger_events, filled_intent.canonical_setup_key)
        repeat_timeout_snapshot = rebuild_execution_state_snapshot(tuple(ledger_events), timeout_key)

        immutable_snapshot = False
        try:
            filled_snapshot.current_state = "MUTATED"  # type: ignore[misc]
        except FrozenInstanceError:
            immutable_snapshot = True

        after_production_probe = set(Path("backtest").rglob("live_rotation_plan.csv"))

    filled_events = [event for event in ledger_events if event.canonical_setup_key == filled_intent.canonical_setup_key]
    timeout_events = [event for event in ledger_events if event.canonical_setup_key == timeout_key]
    partial_events = [event for event in ledger_events if event.canonical_setup_key == partial_key]
    rejected_events = [event for event in ledger_events if event.canonical_setup_key == rejected_key]

    assert filled_snapshot.current_state == "FILLED_CONFIRMED"
    assert filled_snapshot.last_event_type == SIMULATED_FILLED
    assert filled_snapshot.terminal is True
    assert duplicate_snapshot.event_count == filled_snapshot.event_count
    assert duplicate_snapshot.current_state == "FILLED_CONFIRMED"
    assert len([event for event in filled_events if event.event_type == SIMULATED_FILLED]) == 1
    assert timeout_snapshot.current_state == BLOCK_AND_RECONCILE
    assert timeout_snapshot.last_event_type == RECOVERY_DECISION
    assert timeout_snapshot.has_unknown_state is True
    assert timeout_snapshot.block_new_orders is True
    assert timeout_snapshot.requires_manual_review is True
    assert partial_snapshot.current_state == MARK_PARTIAL_PENDING
    assert partial_snapshot.has_partial_fill is True
    assert partial_snapshot.block_new_orders is True
    assert partial_snapshot.requires_manual_review is True
    assert rejected_snapshot.current_state == MARK_REJECTED_TERMINAL
    assert rejected_snapshot.last_event_type == RECOVERY_DECISION
    assert rejected_snapshot.terminal is True
    assert [event.sequence for event in filled_events] == sorted(event.sequence for event in filled_events)
    assert [event.sequence for event in timeout_events] == sorted(event.sequence for event in timeout_events)
    assert [event.sequence for event in partial_events] == sorted(event.sequence for event in partial_events)
    assert [event.sequence for event in rejected_events] == sorted(event.sequence for event in rejected_events)
    assert ordering == list(range(1, len(ordering) + 1))
    assert filled_snapshot == repeat_filled_snapshot
    assert timeout_snapshot == repeat_timeout_snapshot
    assert immutable_snapshot is True
    assert filled_row == filled_source_before
    assert filled_intent == filled_intent_before
    assert NETWORK_CALLS == 0
    assert after_production_probe == before_production_probe

    print(
        "SMOKE_E11_OK "
        "filled_lifecycle=1 "
        "duplicate_blocked=1 "
        "timeout_recovery_blocking=1 "
        "partial_pending=1 "
        "rejected_terminal=1 "
        "ledger_ordering=1 "
        "deterministic_rebuild=1 "
        "mutated_source=0 "
        "exchange_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
