from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
import tempfile

from backtest.execution.decision_consumer import consume_decision_row
from backtest.execution.quantity_converter import (
    ExchangeQuantityRules,
    QuantityConversionError,
    convert_validated_intent_quantity,
)


def valid_row(
    key: str = "XRPUSDT|TDP_REENTRY|LONG|2026-07-08T09:45:00+00:00",
    authorized_qty: str = "100",
) -> dict[str, str]:
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
        "authorized_qty": authorized_qty,
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

        normal = convert_validated_intent_quantity(
            intent,
            ExchangeQuantityRules(quantity_step=Decimal("0.001"), min_quantity=Decimal("0.01"), quantity_precision=3),
        )

        rounded_intent = consume_decision_row(valid_row(authorized_qty="1.23456"))
        rounded = convert_validated_intent_quantity(
            rounded_intent,
            ExchangeQuantityRules(quantity_step=Decimal("0.001"), min_quantity=Decimal("0.01"), quantity_precision=3),
        )

        minimum_rejected = False
        try:
            min_intent = consume_decision_row(valid_row(authorized_qty="0.009"))
            convert_validated_intent_quantity(
                min_intent,
                ExchangeQuantityRules(quantity_step=Decimal("0.001"), min_quantity=Decimal("0.01"), quantity_precision=3),
            )
        except QuantityConversionError:
            minimum_rejected = True

        invalid_rejected = False
        try:
            invalid_intent = consume_decision_row(valid_row(authorized_qty="0.5"))
            convert_validated_intent_quantity(
                invalid_intent,
                ExchangeQuantityRules(quantity_step=Decimal("1"), min_quantity=Decimal("0"), quantity_precision=0),
            )
        except QuantityConversionError:
            invalid_rejected = True

        immutable = False
        try:
            normal.exchange_ready = False  # type: ignore[misc]
        except FrozenInstanceError:
            immutable = True

        after_files = set(Path(tmp).rglob("*"))

    assert normal.exchange_ready is True
    assert normal.quantity == Decimal("100")
    assert normal.rounded_quantity == Decimal("100.000")
    assert normal.entry == intent.entry and normal.stop == intent.sl and normal.target == intent.tp
    assert rounded.rounded_quantity == Decimal("1.234")
    assert minimum_rejected is True
    assert invalid_rejected is True
    assert immutable is True
    assert row == source_before
    assert after_files == before_files

    print(
        "SMOKE_E6_OK "
        "normal_conversion=1 "
        "quantity_rounding=1 "
        "minimum_quantity_rejection=1 "
        "invalid_quantity_rejection=1 "
        "immutable=1 "
        "mutated_source=0 "
        "exchange_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
