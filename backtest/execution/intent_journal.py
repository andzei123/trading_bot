from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from .decision_consumer import ValidatedExecutionIntent


class IntentJournalError(ValueError):
    """Raised when the executor intent journal cannot accept a record."""


JOURNAL_FIELDS = (
    "executor_accepted_ts",
    "canonical_setup_key",
    "symbol",
    "side",
    "model",
    "entry",
    "stop",
    "target",
    "authorized_risk_usd",
)


@dataclass(frozen=True)
class IntentJournalRecord:
    """Immutable append-only executor journal record for one validated intent."""

    executor_accepted_ts: str
    canonical_setup_key: str
    symbol: str
    side: str
    model: str
    entry: Decimal
    stop: Decimal
    target: Decimal
    authorized_risk_usd: Decimal

    @classmethod
    def from_validated_intent(
        cls,
        validated_intent: ValidatedExecutionIntent,
        *,
        executor_accepted_ts: str | None = None,
    ) -> "IntentJournalRecord":
        if not isinstance(validated_intent, ValidatedExecutionIntent):
            raise IntentJournalError("intent journal requires ValidatedExecutionIntent")

        accepted_ts = executor_accepted_ts or datetime.now(timezone.utc).isoformat()
        return cls(
            executor_accepted_ts=accepted_ts,
            canonical_setup_key=validated_intent.canonical_setup_key,
            symbol=validated_intent.symbol,
            side=validated_intent.side,
            model=validated_intent.model,
            entry=validated_intent.entry,
            stop=validated_intent.sl,
            target=validated_intent.tp,
            authorized_risk_usd=validated_intent.authorized_risk_usd,
        )

    def as_row(self) -> dict[str, str]:
        return {
            "executor_accepted_ts": self.executor_accepted_ts,
            "canonical_setup_key": self.canonical_setup_key,
            "symbol": self.symbol,
            "side": self.side,
            "model": self.model,
            "entry": str(self.entry),
            "stop": str(self.stop),
            "target": str(self.target),
            "authorized_risk_usd": str(self.authorized_risk_usd),
        }


class IntentJournal:
    """Append-only executor diagnostics journal for validated intents.

    The journal is downstream-only. It records immutable validated intent data for
    replay/debugging and must not influence execution decisions.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def append(
        self,
        validated_intent: ValidatedExecutionIntent,
        *,
        executor_accepted_ts: str | None = None,
    ) -> IntentJournalRecord:
        record = IntentJournalRecord.from_validated_intent(
            validated_intent,
            executor_accepted_ts=executor_accepted_ts,
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self.path.exists() or self.path.stat().st_size == 0
        with self.path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=JOURNAL_FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow(record.as_row())
        return record


def load_intent_journal(path: str | Path) -> list[IntentJournalRecord]:
    records: list[IntentJournalRecord] = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            records.append(
                IntentJournalRecord(
                    executor_accepted_ts=row["executor_accepted_ts"],
                    canonical_setup_key=row["canonical_setup_key"],
                    symbol=row["symbol"],
                    side=row["side"],
                    model=row["model"],
                    entry=Decimal(row["entry"]),
                    stop=Decimal(row["stop"]),
                    target=Decimal(row["target"]),
                    authorized_risk_usd=Decimal(row["authorized_risk_usd"]),
                )
            )
    return records
