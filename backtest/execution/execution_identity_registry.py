from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


class ExecutionIdentityRegistryError(ValueError):
    """Raised when the executor-local identity registry receives invalid input."""


@dataclass(frozen=True)
class IdempotencyResult:
    """Immutable executor-local idempotency decision.

    This result is not ATS authority and does not inspect exchange or production
    state. It only records whether this executor session has already accepted
    the same canonical execution identity.
    """

    allowed: bool
    duplicate_detected: bool
    canonical_setup_key: str
    checked_at_utc: str
    reason: str


@dataclass
class ExecutionIdentityRegistry:
    """In-memory execution identity registry for E5 duplicate-attempt protection.

    E5 scope is intentionally session-local only. The registry does not write
    files, read production position_state.csv, call exchanges, reconcile orders,
    or persist identities across process restarts.
    """

    _accepted_identities: set[str] = field(default_factory=set)

    def check_and_record(
        self,
        canonical_setup_key: str,
        *,
        checked_at_utc: str | None = None,
    ) -> IdempotencyResult:
        """Record a canonical setup key once and block repeated session attempts."""

        key = _normalize_canonical_setup_key(canonical_setup_key)
        checked = checked_at_utc or _utc_now()

        if key in self._accepted_identities:
            return IdempotencyResult(
                allowed=False,
                duplicate_detected=True,
                canonical_setup_key=key,
                checked_at_utc=checked,
                reason="duplicate_execution_identity",
            )

        self._accepted_identities.add(key)
        return IdempotencyResult(
            allowed=True,
            duplicate_detected=False,
            canonical_setup_key=key,
            checked_at_utc=checked,
            reason="execution_identity_recorded",
        )

    def contains(self, canonical_setup_key: str) -> bool:
        """Return whether a canonical setup key has been recorded in this session."""

        return _normalize_canonical_setup_key(canonical_setup_key) in self._accepted_identities

    def snapshot(self) -> tuple[str, ...]:
        """Return a deterministic immutable snapshot of recorded identities."""

        return tuple(sorted(self._accepted_identities))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_canonical_setup_key(canonical_setup_key: str) -> str:
    if not isinstance(canonical_setup_key, str):
        raise ExecutionIdentityRegistryError("canonical_setup_key must be a string")
    key = canonical_setup_key.strip()
    if not key:
        raise ExecutionIdentityRegistryError("canonical_setup_key is required")
    return key


def check_execution_idempotency(
    canonical_setup_key: str,
    *,
    registry: ExecutionIdentityRegistry | None = None,
    checked_at_utc: str | None = None,
) -> IdempotencyResult:
    """Convenience wrapper for one registry idempotency check.

    Callers that need duplicate detection across repeated attempts must pass the
    same registry instance. If no registry is supplied, a new session-local
    registry is used for this single call only.
    """

    active_registry = registry if registry is not None else ExecutionIdentityRegistry()
    return active_registry.check_and_record(canonical_setup_key, checked_at_utc=checked_at_utc)
