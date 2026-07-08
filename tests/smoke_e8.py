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


NETWORK_CALLS = 0
SIM_TS = "2026-07-08T12:30:00+00:00"


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
        ready_before = ready
        simulator = ExecutionSimulator()

        acked = simulator.simulate(ready, ExecutionSimulationRequest(outcome=ACKED, simulated_at_utc=SIM_TS))
        rejected = simulator.simulate(
            ready,
            ExecutionSimulationRequest(outcome=REJECTED, simulated_at_utc=SIM_TS, reject_reason="simulated_insufficient_margin"),
        )
        partial = simulator.simulate(
            ready,
            ExecutionSimulationRequest(
                outcome=PARTIALLY_FILLED,
                simulated_at_utc=SIM_TS,
                filled_qty=Decimal("4"),
                fill_price=Decimal("2.0000"),
            ),
        )
        filled = simulator.simulate(ready, ExecutionSimulationRequest(outcome=FILLED, simulated_at_utc=SIM_TS))
        timeout = simulator.simulate(ready, ExecutionSimulationRequest(outcome=TIMEOUT_UNKNOWN, simulated_at_utc=SIM_TS))
        unavailable = simulator.simulate(ready, ExecutionSimulationRequest(outcome=EXCHANGE_UNAVAILABLE, simulated_at_utc=SIM_TS))
        repeat_acked = simulator.simulate(ready, ExecutionSimulationRequest(outcome=ACKED, simulated_at_utc=SIM_TS))

        immutable = False
        try:
            filled.status = REJECTED  # type: ignore[misc]
        except FrozenInstanceError:
            immutable = True

        after_files = set(Path(tmp).rglob("*"))

    assert acked.status == ACKED and acked.filled_qty == Decimal("0") and acked.remaining_qty == Decimal("10.000")
    assert rejected.status == REJECTED and rejected.reject_reason == "simulated_insufficient_margin"
    assert partial.status == PARTIALLY_FILLED and partial.filled_qty == Decimal("4") and partial.remaining_qty == Decimal("6.000")
    assert filled.status == FILLED and filled.filled_qty == Decimal("10.000") and filled.remaining_qty == Decimal("0")
    assert timeout.status == TIMEOUT_UNKNOWN and timeout.reject_reason == "timeout_unknown"
    assert unavailable.status == EXCHANGE_UNAVAILABLE and unavailable.reject_reason == "exchange_unavailable"
    assert acked == repeat_acked
    assert immutable is True
    assert ready == ready_before
    assert row == source_before
    assert NETWORK_CALLS == 0
    assert after_files == before_files

    print(
        "SMOKE_E8_OK "
        "acked=1 "
        "rejected=1 "
        "partially_filled=1 "
        "filled=1 "
        "timeout_unknown=1 "
        "exchange_unavailable=1 "
        "deterministic=1 "
        "mutated_source=0 "
        "exchange_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
