"""ATS downstream execution package.

E1 scope only: strict execution intent ingestion and deterministic dry-run
order build. This package must not import or mutate live_observation_shell.py.
"""

from .intent import ExecutionIntent, IntentValidationError, load_intents_csv
from .decision_consumer import DecisionConsumerError, ValidatedExecutionIntent, consume_decision, consume_decision_row
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
]
