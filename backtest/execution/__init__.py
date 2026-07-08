"""ATS downstream execution package.

E1-E3 scope only: strict execution intent ingestion, deterministic dry-run
order build, decision consumption, and append-only intent journaling. This package must not import or mutate live_observation_shell.py.
"""

from .intent import ExecutionIntent, IntentValidationError, load_intents_csv
from .decision_consumer import DecisionConsumerError, ValidatedExecutionIntent, consume_decision, consume_decision_row
from .intent_journal import IntentJournal, IntentJournalError, IntentJournalRecord, load_intent_journal
from .quantity_converter import DryRunOrder, QuantityConversionError, convert_intent_to_dry_run_order

__all__ = [
    "ExecutionIntent",
    "IntentValidationError",
    "load_intents_csv",
    "DecisionConsumerError",
    "ValidatedExecutionIntent",
    "consume_decision",
    "consume_decision_row",
    "DryRunOrder",
    "QuantityConversionError",
    "convert_intent_to_dry_run_order",
    "IntentJournal",
    "IntentJournalError",
    "IntentJournalRecord",
    "load_intent_journal",
]
