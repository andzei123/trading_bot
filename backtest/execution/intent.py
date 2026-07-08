from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable


class IntentValidationError(ValueError):
    """Raised when an ATS execution intent is not acceptable to E1 ingress."""


REQUIRED_FIELDS = (
    "schema_version",
    "cycle_ts",
    "symbol",
    "model",
    "side",
    "canonical_setup_key",
    "setup_id",
    "setup_created_ts",
    "signal_ts",
    "visible_ts",
    "wait_confirm_ts",
    "intended_entry_ts",
    "entry_window_expires_ts",
    "selected_for_execution",
    "execution_rank",
    "selection_reason",
    "entry",
    "sl",
    "tp",
    "planned_rr",
    "risk_pct",
    "reward_pct",
    "risk_distance",
    "reward_distance",
    "authorized_qty",
    "authorized_notional",
    "authorized_risk_usd",
    "risk_snapshot_id",
    "position_snapshot_id",
    "authority_waterfall_id",
)

SUPPORTED_SCHEMA_VERSIONS = {"ATS_EXECUTION_INTENT_V1"}
VALID_SIDES = {"LONG", "SHORT"}
_TRUE_VALUES = {"1", "true", "yes", "y"}


def _required_text(row: dict[str, str], field: str) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise IntentValidationError(f"missing required field: {field}")
    return value


def _decimal(row: dict[str, str], field: str) -> Decimal:
    raw = _required_text(row, field)
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise IntentValidationError(f"invalid decimal field {field}: {raw!r}") from exc
    if value <= 0:
        raise IntentValidationError(f"decimal field must be positive: {field}")
    return value


def _selected(value: str) -> bool:
    return value.strip().lower() in _TRUE_VALUES


@dataclass(frozen=True)
class ExecutionIntent:
    """Authoritative ATS-selected execution intent.

    The executor is downstream-only. This object validates presence and shape of
    ATS decisions; it does not generate signals, rank, size, or re-run risk.
    """

    schema_version: str
    cycle_ts: str
    symbol: str
    model: str
    side: str
    canonical_setup_key: str
    setup_id: str
    setup_created_ts: str
    signal_ts: str
    visible_ts: str
    wait_confirm_ts: str
    intended_entry_ts: str
    entry_window_expires_ts: str
    selected_for_execution: bool
    execution_rank: int
    selection_reason: str
    entry: Decimal
    sl: Decimal
    tp: Decimal
    planned_rr: Decimal
    risk_pct: Decimal
    reward_pct: Decimal
    risk_distance: Decimal
    reward_distance: Decimal
    authorized_qty: Decimal
    authorized_notional: Decimal
    authorized_risk_usd: Decimal
    risk_snapshot_id: str
    position_snapshot_id: str
    authority_waterfall_id: str

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "ExecutionIntent":
        missing_columns = [field for field in REQUIRED_FIELDS if field not in row]
        if missing_columns:
            raise IntentValidationError("missing required columns: " + ", ".join(missing_columns))

        schema_version = _required_text(row, "schema_version")
        if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            raise IntentValidationError(f"unsupported schema_version: {schema_version}")

        selected_for_execution = _selected(_required_text(row, "selected_for_execution"))
        if not selected_for_execution:
            raise IntentValidationError("intent is not selected_for_execution")

        side = _required_text(row, "side").upper()
        if side not in VALID_SIDES:
            raise IntentValidationError(f"unsupported side: {side}")

        execution_rank_raw = _required_text(row, "execution_rank")
        try:
            execution_rank = int(execution_rank_raw)
        except ValueError as exc:
            raise IntentValidationError(f"invalid execution_rank: {execution_rank_raw!r}") from exc
        if execution_rank < 1:
            raise IntentValidationError("execution_rank must be >= 1")

        entry = _decimal(row, "entry")
        sl = _decimal(row, "sl")
        tp = _decimal(row, "tp")
        if side == "LONG" and not (sl < entry < tp):
            raise IntentValidationError("LONG geometry must satisfy sl < entry < tp")
        if side == "SHORT" and not (tp < entry < sl):
            raise IntentValidationError("SHORT geometry must satisfy tp < entry < sl")

        return cls(
            schema_version=schema_version,
            cycle_ts=_required_text(row, "cycle_ts"),
            symbol=_required_text(row, "symbol").upper(),
            model=_required_text(row, "model"),
            side=side,
            canonical_setup_key=_required_text(row, "canonical_setup_key"),
            setup_id=_required_text(row, "setup_id"),
            setup_created_ts=_required_text(row, "setup_created_ts"),
            signal_ts=_required_text(row, "signal_ts"),
            visible_ts=_required_text(row, "visible_ts"),
            wait_confirm_ts=_required_text(row, "wait_confirm_ts"),
            intended_entry_ts=_required_text(row, "intended_entry_ts"),
            entry_window_expires_ts=_required_text(row, "entry_window_expires_ts"),
            selected_for_execution=True,
            execution_rank=execution_rank,
            selection_reason=_required_text(row, "selection_reason"),
            entry=entry,
            sl=sl,
            tp=tp,
            planned_rr=_decimal(row, "planned_rr"),
            risk_pct=_decimal(row, "risk_pct"),
            reward_pct=_decimal(row, "reward_pct"),
            risk_distance=_decimal(row, "risk_distance"),
            reward_distance=_decimal(row, "reward_distance"),
            authorized_qty=_decimal(row, "authorized_qty"),
            authorized_notional=_decimal(row, "authorized_notional"),
            authorized_risk_usd=_decimal(row, "authorized_risk_usd"),
            risk_snapshot_id=_required_text(row, "risk_snapshot_id"),
            position_snapshot_id=_required_text(row, "position_snapshot_id"),
            authority_waterfall_id=_required_text(row, "authority_waterfall_id"),
        )


def load_intents_csv(path: str | Path) -> list[ExecutionIntent]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return [ExecutionIntent.from_row(row) for row in csv.DictReader(handle)]


def validate_rows(rows: Iterable[dict[str, str]]) -> list[ExecutionIntent]:
    return [ExecutionIntent.from_row(row) for row in rows]
