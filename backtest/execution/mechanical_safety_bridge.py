from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from .decision_consumer import ValidatedExecutionIntent


class MechanicalSafetyBridgeError(ValueError):
    """Raised when the mechanical safety bridge receives an invalid input type."""


INFO = "INFO"
BLOCK = "BLOCK"
CRITICAL = "CRITICAL"
ALLOWED_EXECUTOR_MODES = {"DRY_RUN", "MUTATION_ENABLED"}


@dataclass(frozen=True)
class MechanicalSafetyResult:
    """Immutable executor-local mechanical safety decision.

    This result is not ATS authority. It only reports whether executor-local
    mechanical safety conditions allow the already validated intent to proceed
    to a later executor stage.
    """

    allowed: bool
    reason: str
    severity: str
    checked_at_utc: str
    canonical_setup_key: str
    symbol: str
    side: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_utc(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _blocked(
    intent: ValidatedExecutionIntent,
    *,
    reason: str,
    severity: str,
    checked_at_utc: str,
) -> MechanicalSafetyResult:
    return MechanicalSafetyResult(
        allowed=False,
        reason=reason,
        severity=severity,
        checked_at_utc=checked_at_utc,
        canonical_setup_key=getattr(intent, "canonical_setup_key", ""),
        symbol=getattr(intent, "symbol", ""),
        side=getattr(intent, "side", ""),
    )


def check_mechanical_safety(
    validated_intent: ValidatedExecutionIntent,
    *,
    executor_mode: str = "DRY_RUN",
    kill_switch_path: str | Path | None = None,
    pending_identity_keys: Iterable[str] | None = None,
    checked_at_utc: str | None = None,
    max_authorized_risk_usd: Decimal | None = None,
    max_authorized_notional: Decimal | None = None,
) -> MechanicalSafetyResult:
    """Check executor-local mechanical safety for an already validated intent.

    The bridge does not calculate risk, quantity, RR, strategy authority, or read
    exchange/production state. It only checks local mechanical conditions.

    max_authorized_risk_usd and max_authorized_notional are optional
    executor-local upper-bound guardrails against malformed or operationally
    unsafe already-authorized intents. They are not ATS Risk Management,
    portfolio risk, correlation caps, risk recalculation, or strategy overrides.
    """

    if not isinstance(validated_intent, ValidatedExecutionIntent):
        raise MechanicalSafetyBridgeError("mechanical safety requires ValidatedExecutionIntent")

    checked = checked_at_utc or _utc_now()
    mode = executor_mode.strip().upper()

    if mode not in ALLOWED_EXECUTOR_MODES:
        return _blocked(
            validated_intent,
            reason=f"executor_mode_blocked:{mode or 'EMPTY'}",
            severity=CRITICAL,
            checked_at_utc=checked,
        )

    if kill_switch_path is not None and Path(kill_switch_path).exists():
        return _blocked(
            validated_intent,
            reason="kill_switch_present",
            severity=CRITICAL,
            checked_at_utc=checked,
        )

    if not validated_intent.canonical_setup_key.strip():
        return _blocked(
            validated_intent,
            reason="missing_canonical_setup_key",
            severity=BLOCK,
            checked_at_utc=checked,
        )
    if not validated_intent.symbol.strip():
        return _blocked(
            validated_intent,
            reason="missing_symbol",
            severity=BLOCK,
            checked_at_utc=checked,
        )
    if not validated_intent.model.strip():
        return _blocked(
            validated_intent,
            reason="missing_model",
            severity=BLOCK,
            checked_at_utc=checked,
        )
    if validated_intent.side not in {"LONG", "SHORT"}:
        return _blocked(
            validated_intent,
            reason=f"invalid_side:{validated_intent.side}",
            severity=BLOCK,
            checked_at_utc=checked,
        )

    for field_name, value in (
        ("entry", validated_intent.entry),
        ("stop", validated_intent.sl),
        ("target", validated_intent.tp),
    ):
        if not value.is_finite() or value <= 0:
            return _blocked(
                validated_intent,
                reason=f"invalid_{field_name}",
                severity=BLOCK,
                checked_at_utc=checked,
            )

    if validated_intent.side == "LONG" and not (validated_intent.sl < validated_intent.entry < validated_intent.tp):
        return _blocked(
            validated_intent,
            reason="invalid_long_geometry",
            severity=BLOCK,
            checked_at_utc=checked,
        )
    if validated_intent.side == "SHORT" and not (validated_intent.tp < validated_intent.entry < validated_intent.sl):
        return _blocked(
            validated_intent,
            reason="invalid_short_geometry",
            severity=BLOCK,
            checked_at_utc=checked,
        )

    if validated_intent.entry_window_expires_ts.strip():
        try:
            checked_dt = _parse_utc(checked)
            expires_dt = _parse_utc(validated_intent.entry_window_expires_ts)
        except ValueError:
            return _blocked(
                validated_intent,
                reason="invalid_expiry_timestamp",
                severity=BLOCK,
                checked_at_utc=checked,
            )
        if checked_dt > expires_dt:
            return _blocked(
                validated_intent,
                reason="intent_expired",
                severity=BLOCK,
                checked_at_utc=checked,
            )

    if pending_identity_keys is not None:
        pending = set(pending_identity_keys)
        if validated_intent.canonical_setup_key in pending:
            return _blocked(
                validated_intent,
                reason="pending_identity_already_present",
                severity=BLOCK,
                checked_at_utc=checked,
            )

    # Executor-local upper-bound guardrail only: this does not recalculate,
    # resize, approve, or override ATS-authorized strategy risk.
    if max_authorized_risk_usd is not None and validated_intent.authorized_risk_usd > max_authorized_risk_usd:
        return _blocked(
            validated_intent,
            reason="local_authorized_risk_cap_exceeded",
            severity=BLOCK,
            checked_at_utc=checked,
        )

    # Executor-local upper-bound guardrail only: this does not perform
    # portfolio risk management, correlation caps, or strategy evaluation.
    if max_authorized_notional is not None and validated_intent.authorized_notional > max_authorized_notional:
        return _blocked(
            validated_intent,
            reason="local_authorized_notional_cap_exceeded",
            severity=BLOCK,
            checked_at_utc=checked,
        )

    return MechanicalSafetyResult(
        allowed=True,
        reason="mechanical_safety_passed",
        severity=INFO,
        checked_at_utc=checked,
        canonical_setup_key=validated_intent.canonical_setup_key,
        symbol=validated_intent.symbol,
        side=validated_intent.side,
    )
