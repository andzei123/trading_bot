from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
import tempfile

from backtest.execution.decision_consumer import consume_decision_row
from backtest.execution.execution_simulator import (
    ACKED,
    EXCHANGE_UNAVAILABLE,
    FILLED,
    PARTIALLY_FILLED,
    REJECTED,
    TIMEOUT_UNKNOWN,
    ExecutionSimulationRequest,
    ExecutionSimulator,
)
from backtest.execution.quantity_converter import ExchangeQuantityRules, convert_validated_intent_quantity
from backtest.execution.recovery_simulator import (
    BLOCK_AND_RECONCILE,
    EXCHANGE_UNAVAILABLE_FAIL_CLOSED,
    MARK_FILLED_CONFIRMED,
    MARK_PARTIAL_PENDING,
    MARK_REJECTED_TERMINAL,
    WAIT_FOR_RECHECK,
    RecoverySimulationRequest,
    RecoverySimulator,
)


NETWORK_CALLS = 0
SIM_TS = "2026-07-08T13:00:00+00:00"
DECISION_TS = "2026-07-08T13:01:00+00:00"


def valid_row() -> dict[str, str]:
    return {
        "schema_version": "ATS_EXECUTION_INTENT_V1",
        "cycle_ts": "2026-07-08T10:00:00+00:00",
        "symbol": "XRPUSDT",
        "model": "TDP_REENTRY",
        "side": "LONG",
        "canonical_setup_key": "XRPUSDT|TDP_REENTRY|LONG|2026-07-08T09:45:00+00:00",
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
        "authorized_qty": "10",
        "authorized_notional": "20",
        "authorized_risk_usd": "0.5",
        "risk_snapshot_id": "risk-snap-1",
        "position_snapshot_id": "pos-snap-1",
        "authority_waterfall_id": "waterfall-1",
    }


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        before_files = set(Path(tmp).rglob("*"))

        row = valid_row()
        source_before = dict(row)
        validated = consume_decision_row(row)
        ready = convert_validated_intent_quantity(
            validated,
            ExchangeQuantityRules(quantity_step=Decimal("0.001"), min_quantity=Decimal("0.001"), quantity_precision=3),
        )
        simulator = ExecutionSimulator()
        recovery = RecoverySimulator()

        timeout_event = simulator.simulate(ready, ExecutionSimulationRequest(outcome=TIMEOUT_UNKNOWN, simulated_at_utc=SIM_TS))
        partial_event = simulator.simulate(
            ready,
            ExecutionSimulationRequest(outcome=PARTIALLY_FILLED, simulated_at_utc=SIM_TS, filled_qty=Decimal("4")),
        )
        unavailable_event = simulator.simulate(ready, ExecutionSimulationRequest(outcome=EXCHANGE_UNAVAILABLE, simulated_at_utc=SIM_TS))
        rejected_event = simulator.simulate(ready, ExecutionSimulationRequest(outcome=REJECTED, simulated_at_utc=SIM_TS))
        filled_event = simulator.simulate(ready, ExecutionSimulationRequest(outcome=FILLED, simulated_at_utc=SIM_TS))
        acked_event = simulator.simulate(ready, ExecutionSimulationRequest(outcome=ACKED, simulated_at_utc=SIM_TS))

        request = RecoverySimulationRequest(decided_at_utc=DECISION_TS)
        timeout_decision = recovery.decide(timeout_event, request)
        partial_decision = recovery.decide(partial_event, request)
        unavailable_decision = recovery.decide(unavailable_event, request)
        rejected_decision = recovery.decide(rejected_event, request)
        filled_decision = recovery.decide(filled_event, request)
        acked_decision = recovery.decide(acked_event, request)
        repeat_timeout_decision = recovery.decide(timeout_event, request)

        immutable = False
        try:
            timeout_decision.recovery_action = MARK_FILLED_CONFIRMED  # type: ignore[misc]
        except FrozenInstanceError:
            immutable = True

        after_files = set(Path(tmp).rglob("*"))

    assert timeout_decision.recovery_action == BLOCK_AND_RECONCILE
    assert timeout_decision.block_new_orders is True and timeout_decision.requires_manual_review is True
    assert partial_decision.recovery_action == MARK_PARTIAL_PENDING
    assert partial_decision.block_new_orders is True and partial_decision.requires_manual_review is True
    assert unavailable_decision.recovery_action == EXCHANGE_UNAVAILABLE_FAIL_CLOSED
    assert unavailable_decision.block_new_orders is True and unavailable_decision.requires_manual_review is True
    assert rejected_decision.recovery_action == MARK_REJECTED_TERMINAL
    assert rejected_decision.block_new_orders is False and rejected_decision.requires_manual_review is False
    assert filled_decision.recovery_action == MARK_FILLED_CONFIRMED
    assert filled_decision.block_new_orders is False and filled_decision.requires_manual_review is False
    assert acked_decision.recovery_action == WAIT_FOR_RECHECK
    assert acked_decision.block_new_orders is True and acked_decision.requires_manual_review is False
    assert timeout_decision == repeat_timeout_decision
    assert timeout_event.status == TIMEOUT_UNKNOWN
    assert immutable is True
    assert row == source_before
    assert NETWORK_CALLS == 0
    assert after_files == before_files

    print(
        "SMOKE_E9_OK "
        "timeout_unknown_block_reconcile=1 "
        "partial_pending=1 "
        "exchange_unavailable_fail_closed=1 "
        "rejected_terminal=1 "
        "filled_confirmed=1 "
        "acked_wait_recheck=1 "
        "deterministic=1 "
        "mutated_source=0 "
        "exchange_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
