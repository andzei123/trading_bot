from __future__ import annotations

import json
import hashlib
import os
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Callable, Iterable, Mapping, Protocol

from .recovery_simulator import RecoveryDecision


class ExecutionEventLedgerError(ValueError):
    """Raised when an executor-local event ledger operation is invalid."""


class ExecutionLifecycleTransitionError(ExecutionEventLedgerError):
    """Raised when an executor lifecycle stream violates canonical ordering."""


class PersistenceStateUnknownError(ExecutionEventLedgerError):
    """Raised after an indeterminate persistent write outcome latches fail closed."""


class LedgerWriterOwnershipError(ExecutionEventLedgerError):
    """Raised when another writer already owns the normalized ledger path."""


PERSISTENCE_HEALTHY = "PERSISTENCE_HEALTHY"
PERSISTENCE_STATE_UNKNOWN = "PERSISTENCE_STATE_UNKNOWN"

PERSISTENCE_VALID = "VALID"
PERSISTENCE_INCOMPLETE = "INCOMPLETE"
PERSISTENCE_CORRUPTED = "CORRUPTED"
PERSISTENCE_UNKNOWN = "UNKNOWN"

BEFORE_TEMP_CREATE = "BEFORE_TEMP_CREATE"
TEMP_WRITING = "TEMP_WRITING"
TEMP_FLUSHED = "TEMP_FLUSHED"
BEFORE_REPLACE = "BEFORE_REPLACE"
REPLACE_RETURNED = "REPLACE_RETURNED"
COMMITTED_FILE_FLUSHED = "COMMITTED_FILE_FLUSHED"
COMMIT_ACKNOWLEDGED = "COMMIT_ACKNOWLEDGED"


@dataclass(frozen=True)
class PersistenceIntegrityReport:
    """Deterministic startup classification for Executor-owned persistence."""

    status: str
    reason: str
    event_count: int
    main_path: str
    temporary_path: str



_PROCESS_WRITER_PATHS: set[str] = set()
_PROCESS_WRITER_PATHS_GUARD = threading.Lock()


class _LedgerWriterOwnership:
    """Mechanical, OS-released exclusive ownership for one ledger path."""

    def __init__(self, ledger_path: Path) -> None:
        self._normalized_path = os.path.normcase(str(ledger_path.resolve()))
        self._lock_path = Path(f"{self._normalized_path}.lock")
        self._handle = None
        self._released = False

        with _PROCESS_WRITER_PATHS_GUARD:
            if self._normalized_path in _PROCESS_WRITER_PATHS:
                raise LedgerWriterOwnershipError(
                    f"persistent execution ledger already has an active writer: {self._normalized_path}"
                )
            _PROCESS_WRITER_PATHS.add(self._normalized_path)

        try:
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = self._lock_path.open("a+b")
            self._acquire_os_lock()
        except Exception as exc:
            self.release()
            if isinstance(exc, LedgerWriterOwnershipError):
                raise
            raise LedgerWriterOwnershipError(
                f"failed to acquire persistent execution ledger ownership: {self._normalized_path}"
            ) from exc

    def _acquire_os_lock(self) -> None:
        assert self._handle is not None
        if os.name == "nt":
            import msvcrt

            self._handle.seek(0, os.SEEK_END)
            if self._handle.tell() == 0:
                self._handle.write(b"0")
                self._handle.flush()
            self._handle.seek(0)
            try:
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise LedgerWriterOwnershipError(
                    f"persistent execution ledger writer lock is held: {self._normalized_path}"
                ) from exc
        else:
            import fcntl

            try:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise LedgerWriterOwnershipError(
                    f"persistent execution ledger writer lock is held: {self._normalized_path}"
                ) from exc

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        handle = self._handle
        self._handle = None
        if handle is not None:
            try:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                handle.close()
            except Exception:
                pass
        with _PROCESS_WRITER_PATHS_GUARD:
            _PROCESS_WRITER_PATHS.discard(self._normalized_path)


INTENT_ACCEPTED = "INTENT_ACCEPTED"
MECHANICAL_SAFETY_PASSED = "MECHANICAL_SAFETY_PASSED"
IDEMPOTENCY_ALLOWED = "IDEMPOTENCY_ALLOWED"
EXCHANGE_READY = "EXCHANGE_READY"
SIMULATED_ACKED = "SIMULATED_ACKED"
SIMULATED_REJECTED = "SIMULATED_REJECTED"
SIMULATED_PARTIALLY_FILLED = "SIMULATED_PARTIALLY_FILLED"
SIMULATED_FILLED = "SIMULATED_FILLED"
SIMULATED_TIMEOUT_UNKNOWN = "SIMULATED_TIMEOUT_UNKNOWN"
SIMULATED_EXCHANGE_UNAVAILABLE = "SIMULATED_EXCHANGE_UNAVAILABLE"
RECOVERY_DECISION = "RECOVERY_DECISION"
RESERVED_PRE_SUBMIT = "RESERVED_PRE_SUBMIT"
SUBMIT_ACK = "SUBMIT_ACK"
SUBMIT_REJECT = "SUBMIT_REJECT"
SUBMIT_TIMEOUT_UNKNOWN = "SUBMIT_TIMEOUT_UNKNOWN"
SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
SUBMIT_BLOCKED = "SUBMIT_BLOCKED"
CLOSE_REQUESTED = "CLOSE_REQUESTED"
CLOSE_CONFIRMED = "CLOSE_CONFIRMED"
EXECUTION_COMPLETED = "EXECUTION_COMPLETED"

SUPPORTED_EXECUTION_LEDGER_EVENTS = frozenset(
    {
        INTENT_ACCEPTED,
        MECHANICAL_SAFETY_PASSED,
        IDEMPOTENCY_ALLOWED,
        EXCHANGE_READY,
        SIMULATED_ACKED,
        SIMULATED_REJECTED,
        SIMULATED_PARTIALLY_FILLED,
        SIMULATED_FILLED,
        SIMULATED_TIMEOUT_UNKNOWN,
        SIMULATED_EXCHANGE_UNAVAILABLE,
        RECOVERY_DECISION,
        RESERVED_PRE_SUBMIT,
        SUBMIT_ACK,
        SUBMIT_REJECT,
        SUBMIT_TIMEOUT_UNKNOWN,
        SUBMIT_UNKNOWN,
        SUBMIT_BLOCKED,
        CLOSE_REQUESTED,
        CLOSE_CONFIRMED,
        EXECUTION_COMPLETED,
    }
)

_TERMINAL_EVENTS = frozenset({SIMULATED_FILLED, SIMULATED_REJECTED})
_UNKNOWN_EVENTS = frozenset({SIMULATED_TIMEOUT_UNKNOWN, SIMULATED_EXCHANGE_UNAVAILABLE, SUBMIT_TIMEOUT_UNKNOWN, SUBMIT_UNKNOWN})


@dataclass(frozen=True)
class ExecutionLedgerEvent:
    """Immutable executor-local append-only event.

    E10 records local executor events only. It does not call exchanges, read
    exchange state, submit/cancel/amend orders, write ATS production files,
    scan restart persistence, reconcile exchange state, or mutate ATS telemetry.
    """

    sequence: int
    canonical_setup_key: str
    event_type: str
    recorded_at_utc: str
    symbol: str = ""
    side: str = ""
    client_order_id: str = ""
    status: str = ""
    block_new_orders: bool = False
    requires_manual_review: bool = False
    reason: str = ""


@dataclass(frozen=True)
class ExecutionStateSnapshot:
    """Immutable deterministic state reconstructed from local ledger events."""

    canonical_setup_key: str
    current_state: str
    last_event_type: str
    event_count: int
    has_unknown_state: bool
    has_partial_fill: bool
    terminal: bool
    block_new_orders: bool
    requires_manual_review: bool
    submit_state: str = ""
    submit_confirmed: bool = False
    submit_unknown: bool = False
    terminal_submit_failure: bool = False


@dataclass(frozen=True)
class ExecutionLifecycleSnapshot:
    """Immutable executor-only lifecycle projection.

    The projection observes executor ledger events only. It does not decide
    entries, exits, TP, SL, risk, selection, or Position State authority.
    """

    canonical_setup_key: str
    current_state: str
    transition_history: tuple[str, ...]
    event_count: int
    position_active: bool
    close_requested: bool
    close_confirmed: bool
    completed: bool


class ExecutionEventLedger:
    """Executor-local append-only ledger with deterministic reconstruction.

    A persistent ledger owns one normalized path for its active lifetime. Any
    indeterminate write result latches the instance into
    ``PERSISTENCE_STATE_UNKNOWN``; no later mutation or retry is allowed.
    """

    def __init__(
        self,
        ledger_path: str | Path | None = None,
        *,
        persistence_phase_observer: Callable[[str], None] | None = None,
    ) -> None:
        self._events: list[ExecutionLedgerEvent] = []
        self._ledger_path = Path(ledger_path).resolve() if ledger_path is not None else None
        self._writer_ownership: _LedgerWriterOwnership | None = None
        self._persistence_health = PERSISTENCE_HEALTHY
        self._unknown_persistence_sequence: int | None = None
        self._closed = False
        self._persistence_phase_observer = persistence_phase_observer
        if self._ledger_path is not None:
            self._writer_ownership = _LedgerWriterOwnership(self._ledger_path)
            try:
                self._ledger_path.parent.mkdir(parents=True, exist_ok=True)
                integrity = inspect_persisted_execution_integrity(self._ledger_path)
                if integrity.status != PERSISTENCE_VALID:
                    raise ExecutionEventLedgerError(
                        f"persistent execution ledger integrity is {integrity.status}: {integrity.reason}"
                    )
                if self._ledger_path.exists() and self._ledger_path.stat().st_size:
                    self._events = list(load_persisted_execution_events(self._ledger_path))
            except Exception:
                self.close()
                raise

    @property
    def events(self) -> tuple[ExecutionLedgerEvent, ...]:
        return tuple(self._events)

    @property
    def persistence_health(self) -> str:
        return self._persistence_health

    @property
    def unknown_persistence_sequence(self) -> int | None:
        return self._unknown_persistence_sequence

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._writer_ownership is not None:
            self._writer_ownership.release()
            self._writer_ownership = None

    def __enter__(self) -> "ExecutionEventLedger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _require_mutation_allowed(self) -> None:
        if self._closed:
            raise ExecutionEventLedgerError("execution event ledger is closed")
        if self._persistence_health != PERSISTENCE_HEALTHY:
            raise PersistenceStateUnknownError(
                "persistent execution ledger state is unknown; full reload required"
            )

    def _persist_events(self, events: Iterable[ExecutionLedgerEvent]) -> None:
        if self._ledger_path is None:
            return
        self._require_mutation_allowed()
        materialized = tuple(events)
        if not materialized:
            return
        try:
            _append_persisted_event_lines(
                self._ledger_path,
                tuple(self._events) + materialized,
                phase_observer=self._persistence_phase_observer,
            )
        except Exception as exc:
            self._persistence_health = PERSISTENCE_STATE_UNKNOWN
            self._unknown_persistence_sequence = materialized[0].sequence
            raise PersistenceStateUnknownError(
                "persistent execution ledger write outcome is unknown; mutation latched fail closed"
            ) from exc

    def append_event(
        self,
        *,
        canonical_setup_key: str,
        event_type: str,
        recorded_at_utc: str | None = None,
        symbol: str = "",
        side: str = "",
        client_order_id: str = "",
        status: str = "",
        block_new_orders: bool = False,
        requires_manual_review: bool = False,
        reason: str = "",
    ) -> ExecutionLedgerEvent:
        self._require_mutation_allowed()
        normalized_key = canonical_setup_key.strip()
        normalized_event_type = event_type.strip().upper()
        if not normalized_key:
            raise ExecutionEventLedgerError("canonical_setup_key is required")
        if normalized_event_type not in SUPPORTED_EXECUTION_LEDGER_EVENTS:
            raise ExecutionEventLedgerError(f"unsupported execution ledger event type: {event_type}")

        event = ExecutionLedgerEvent(
            sequence=len(self._events) + 1,
            canonical_setup_key=normalized_key,
            event_type=normalized_event_type,
            recorded_at_utc=recorded_at_utc or _utc_now(),
            symbol=symbol,
            side=side,
            client_order_id=client_order_id,
            status=status,
            block_new_orders=block_new_orders,
            requires_manual_review=requires_manual_review,
            reason=reason,
        )
        self._persist_events((event,))
        self._events.append(event)
        _emit_persistence_phase(self._persistence_phase_observer, COMMIT_ACKNOWLEDGED)
        return event

    def append_recovery_decision(
        self,
        decision: RecoveryDecision,
        *,
        recorded_at_utc: str | None = None,
    ) -> ExecutionLedgerEvent:
        if not isinstance(decision, RecoveryDecision):
            raise ExecutionEventLedgerError("append_recovery_decision requires RecoveryDecision")
        return self.append_event(
            canonical_setup_key=decision.canonical_setup_key,
            event_type=RECOVERY_DECISION,
            recorded_at_utc=recorded_at_utc or decision.decided_at_utc,
            symbol=decision.symbol,
            client_order_id=decision.client_order_id,
            status=decision.recovery_action,
            block_new_orders=decision.block_new_orders,
            requires_manual_review=decision.requires_manual_review,
            reason=decision.reason,
        )

    def rebuild_snapshot(self, canonical_setup_key: str) -> ExecutionStateSnapshot:
        return rebuild_execution_state_snapshot(self._events, canonical_setup_key)

    def rebuild_lifecycle_snapshot(self, canonical_setup_key: str) -> ExecutionLifecycleSnapshot:
        return rebuild_execution_lifecycle_snapshot(self._events, canonical_setup_key)

    def append_lifecycle_event(
        self,
        *,
        canonical_setup_key: str,
        event_type: str,
        recorded_at_utc: str | None = None,
        reason: str = "",
    ) -> ExecutionLedgerEvent:
        self._require_mutation_allowed()
        validate_next_lifecycle_event(self._events, canonical_setup_key, event_type)
        return self.append_event(
            canonical_setup_key=canonical_setup_key,
            event_type=event_type,
            recorded_at_utc=recorded_at_utc,
            reason=reason,
        )

    def append_lifecycle_events(
        self,
        *,
        canonical_setup_key: str,
        event_types: Iterable[str],
        recorded_at_utc: str | None = None,
        reason: str = "",
    ) -> tuple[ExecutionLedgerEvent, ...]:
        self._require_mutation_allowed()
        normalized_key = canonical_setup_key.strip()
        if not normalized_key:
            raise ExecutionEventLedgerError("canonical_setup_key is required")
        normalized_types = tuple(str(value).strip().upper() for value in event_types)
        if not normalized_types:
            raise ExecutionEventLedgerError("event_types is required")
        prospective_events = list(self._events)
        batch: list[ExecutionLedgerEvent] = []
        timestamp = recorded_at_utc or _utc_now()
        next_sequence = len(self._events) + 1
        for offset, event_type in enumerate(normalized_types):
            if event_type not in SUPPORTED_EXECUTION_LEDGER_EVENTS:
                raise ExecutionEventLedgerError(
                    f"unsupported execution ledger event type: {event_type}"
                )
            validate_next_lifecycle_event(prospective_events, normalized_key, event_type)
            event = ExecutionLedgerEvent(
                sequence=next_sequence + offset,
                canonical_setup_key=normalized_key,
                event_type=event_type,
                recorded_at_utc=timestamp,
                reason=reason,
            )
            prospective_events.append(event)
            batch.append(event)

        expected_sequences = tuple(range(next_sequence, next_sequence + len(batch)))
        actual_sequences = tuple(event.sequence for event in batch)
        if actual_sequences != expected_sequences:
            raise ExecutionEventLedgerError("non-contiguous lifecycle batch sequence")

        self._persist_events(batch)
        self._extend_events_atomically(batch)
        _emit_persistence_phase(self._persistence_phase_observer, COMMIT_ACKNOWLEDGED)
        return tuple(batch)

    def _extend_events_atomically(self, events: list[ExecutionLedgerEvent]) -> None:
        self._events.extend(events)


_EXECUTION_EVENT_FIELDS = frozenset(ExecutionLedgerEvent.__dataclass_fields__)

def _persistence_temp_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.tmp")


def inspect_persisted_execution_integrity(path: str | Path) -> PersistenceIntegrityReport:
    """Classify persistent state without synthesizing or repairing history."""

    ledger_path = Path(path)
    temp_path = _persistence_temp_path(ledger_path)
    if temp_path.exists():
        return PersistenceIntegrityReport(
            status=PERSISTENCE_INCOMPLETE,
            reason="interrupted durable replacement artifact exists",
            event_count=0,
            main_path=str(ledger_path),
            temporary_path=str(temp_path),
        )
    if not ledger_path.exists():
        return PersistenceIntegrityReport(
            status=PERSISTENCE_VALID,
            reason="no persisted history",
            event_count=0,
            main_path=str(ledger_path),
            temporary_path=str(temp_path),
        )
    try:
        raw = ledger_path.read_bytes()
    except OSError as exc:
        return PersistenceIntegrityReport(
            status=PERSISTENCE_UNKNOWN,
            reason=f"persistent history could not be read: {type(exc).__name__}",
            event_count=0,
            main_path=str(ledger_path),
            temporary_path=str(temp_path),
        )
    try:
        events = _parse_persisted_execution_events(raw)
    except ExecutionEventLedgerError as exc:
        status = (
            PERSISTENCE_INCOMPLETE
            if raw and not raw.endswith(b"\n")
            else PERSISTENCE_CORRUPTED
        )
        return PersistenceIntegrityReport(
            status=status,
            reason=str(exc),
            event_count=0,
            main_path=str(ledger_path),
            temporary_path=str(temp_path),
        )
    return PersistenceIntegrityReport(
        status=PERSISTENCE_VALID,
        reason="validated durable history",
        event_count=len(events),
        main_path=str(ledger_path),
        temporary_path=str(temp_path),
    )


def load_persisted_execution_events(path: str | Path) -> tuple[ExecutionLedgerEvent, ...]:
    """Load one complete, schema-exact and lifecycle-valid durable ledger."""

    ledger_path = Path(path)
    integrity = inspect_persisted_execution_integrity(ledger_path)
    if integrity.status != PERSISTENCE_VALID:
        raise ExecutionEventLedgerError(
            f"persistent execution ledger integrity is {integrity.status}: {integrity.reason}"
        )
    if not ledger_path.exists():
        return ()
    return _parse_persisted_execution_events(ledger_path.read_bytes())


def _parse_persisted_execution_events(raw: bytes) -> tuple[ExecutionLedgerEvent, ...]:
    if not raw:
        return ()
    if not raw.endswith(b"\n"):
        raise ExecutionEventLedgerError("truncated persisted execution ledger write")

    events: list[ExecutionLedgerEvent] = []
    seen_records: set[tuple[object, ...]] = set()
    for line_number, raw_line in enumerate(raw.splitlines(), start=1):
        if not raw_line.strip():
            raise ExecutionEventLedgerError(
                f"blank persisted execution ledger record at line {line_number}"
            )
        try:
            decoded = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExecutionEventLedgerError(
                f"malformed persisted execution ledger record at line {line_number}"
            ) from exc
        if not isinstance(decoded, Mapping):
            raise ExecutionEventLedgerError(
                f"persisted execution ledger record must be an object at line {line_number}"
            )
        if frozenset(decoded) != _EXECUTION_EVENT_FIELDS:
            raise ExecutionEventLedgerError(
                f"persisted execution ledger schema mismatch at line {line_number}"
            )

        event = _execution_event_from_persisted_record(decoded, line_number)
        expected_sequence = len(events) + 1
        if event.sequence != expected_sequence:
            raise ExecutionEventLedgerError(
                f"persisted execution ledger sequence gap/conflict at line {line_number}: "
                f"expected {expected_sequence}, got {event.sequence}"
            )
        fingerprint = tuple(asdict(event).items())
        if fingerprint in seen_records:
            raise ExecutionEventLedgerError(
                f"duplicate persisted execution ledger record at line {line_number}"
            )
        seen_records.add(fingerprint)
        events.append(event)

    keys = sorted({event.canonical_setup_key for event in events})
    for key in keys:
        rebuild_execution_lifecycle_snapshot(events, key)
        rebuild_execution_state_snapshot(events, key)
    return tuple(events)


def _execution_event_from_persisted_record(
    record: Mapping[str, object], line_number: int
) -> ExecutionLedgerEvent:
    sequence = record.get("sequence")
    if type(sequence) is not int or sequence < 1:
        raise ExecutionEventLedgerError(
            f"invalid persisted sequence at line {line_number}"
        )
    key = record.get("canonical_setup_key")
    if not isinstance(key, str) or not key.strip() or key != key.strip():
        raise ExecutionEventLedgerError(
            f"invalid persisted canonical identity at line {line_number}"
        )
    event_type = record.get("event_type")
    if not isinstance(event_type, str) or event_type not in SUPPORTED_EXECUTION_LEDGER_EVENTS:
        raise ExecutionEventLedgerError(
            f"unknown persisted execution event type at line {line_number}"
        )
    timestamp = record.get("recorded_at_utc")
    if not isinstance(timestamp, str) or not timestamp:
        raise ExecutionEventLedgerError(
            f"invalid persisted timestamp at line {line_number}"
        )
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExecutionEventLedgerError(
            f"malformed persisted timestamp at line {line_number}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExecutionEventLedgerError(
            f"timezone-naive persisted timestamp at line {line_number}"
        )

    text_fields = ("symbol", "side", "client_order_id", "status", "reason")
    for field_name in text_fields:
        if not isinstance(record.get(field_name), str):
            raise ExecutionEventLedgerError(
                f"invalid persisted {field_name} at line {line_number}"
            )
    bool_fields = ("block_new_orders", "requires_manual_review")
    for field_name in bool_fields:
        if type(record.get(field_name)) is not bool:
            raise ExecutionEventLedgerError(
                f"invalid persisted {field_name} at line {line_number}"
            )
    return ExecutionLedgerEvent(**dict(record))


class _DurabilityAdapter(Protocol):
    """Platform boundary for durable file flush and atomic replacement."""

    runtime_evidence: str

    def flush_file(self, handle) -> None: ...

    def replace(self, temp_path: Path, destination_path: Path) -> None: ...

    def flush_directory(self, directory_path: Path) -> None: ...


class _PosixDurabilityAdapter:
    runtime_evidence = "POSIX RUNTIME EVIDENCE"

    def flush_file(self, handle) -> None:
        handle.flush()
        os.fsync(handle.fileno())

    def replace(self, temp_path: Path, destination_path: Path) -> None:
        os.replace(temp_path, destination_path)

    def flush_directory(self, directory_path: Path) -> None:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        descriptor = os.open(directory_path, flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class _WindowsDurabilityAdapter:
    """Minimal Win32 durability adapter.

    Uses FlushFileBuffers for file handles and MoveFileExW with
    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH for same-volume atomic
    namespace replacement. Win32 success is checked strictly; failures include
    GetLastError through ctypes.WinError. Device/controller caches may still
    require hardware support and external hard-power-off validation.
    """

    runtime_evidence = "WINDOWS RUNTIME EVIDENCE"
    MOVEFILE_REPLACE_EXISTING = 0x00000001
    MOVEFILE_WRITE_THROUGH = 0x00000008

    def __init__(self) -> None:
        if os.name != "nt":
            raise OSError("Win32 durability adapter requires Windows")
        import ctypes
        from ctypes import wintypes

        self._ctypes = ctypes
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._flush_file_buffers = self._kernel32.FlushFileBuffers
        self._flush_file_buffers.argtypes = [wintypes.HANDLE]
        self._flush_file_buffers.restype = wintypes.BOOL
        self._move_file_ex = self._kernel32.MoveFileExW
        self._move_file_ex.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        self._move_file_ex.restype = wintypes.BOOL

    def flush_file(self, handle) -> None:
        import msvcrt

        handle.flush()
        os_handle = msvcrt.get_osfhandle(handle.fileno())
        if not self._flush_file_buffers(os_handle):
            raise self._ctypes.WinError(self._ctypes.get_last_error())

    def replace(self, temp_path: Path, destination_path: Path) -> None:
        flags = self.MOVEFILE_REPLACE_EXISTING | self.MOVEFILE_WRITE_THROUGH
        if not self._move_file_ex(str(temp_path), str(destination_path), flags):
            raise self._ctypes.WinError(self._ctypes.get_last_error())

    def flush_directory(self, directory_path: Path) -> None:
        # MoveFileExW(MOVEFILE_WRITE_THROUGH) is the documented Windows
        # namespace write-through primitive used here. Windows directory
        # handles are not treated as a portable FlushFileBuffers contract.
        return None


def _platform_durability_adapter() -> _DurabilityAdapter:
    if os.name == "nt":
        return _WindowsDurabilityAdapter()
    return _PosixDurabilityAdapter()


def _emit_persistence_phase(
    observer: Callable[[str], None] | None, phase: str
) -> None:
    if observer is not None:
        observer(phase)


def _same_directory_same_volume(temp_path: Path, destination_path: Path) -> None:
    if temp_path.parent.resolve() != destination_path.parent.resolve():
        raise ExecutionEventLedgerError(
            "durable replacement requires the same directory"
        )
    if os.path.splitdrive(str(temp_path.resolve()))[0].lower() != os.path.splitdrive(
        str(destination_path.resolve())
    )[0].lower():
        raise ExecutionEventLedgerError(
            "durable replacement requires the same volume"
        )


def _durable_replace(
    temp_path: Path,
    destination_path: Path,
    *,
    adapter: _DurabilityAdapter | None = None,
    phase_observer: Callable[[str], None] | None = None,
) -> None:
    """Atomically replace one complete ledger and acknowledge durability."""

    _same_directory_same_volume(temp_path, destination_path)
    durability = adapter or _platform_durability_adapter()
    _emit_persistence_phase(phase_observer, BEFORE_REPLACE)
    durability.replace(temp_path, destination_path)
    _emit_persistence_phase(phase_observer, REPLACE_RETURNED)
    # FlushFileBuffers requires a Windows handle opened with write access.
    # The file contents are not modified; r+b only supplies the required
    # handle capability for the post-replace durability acknowledgement.
    with destination_path.open("r+b") as committed:
        durability.flush_file(committed)
    _emit_persistence_phase(phase_observer, COMMITTED_FILE_FLUSHED)
    durability.flush_directory(destination_path.parent)


def _append_persisted_event_lines(
    path: Path,
    events: Iterable[ExecutionLedgerEvent],
    *,
    phase_observer: Callable[[str], None] | None = None,
    adapter: _DurabilityAdapter | None = None,
) -> None:
    """Durably replace the whole ledger through a same-directory temp file."""

    materialized = tuple(events)
    payload = "".join(
        json.dumps(asdict(event), sort_keys=True) + "\n" for event in materialized
    )
    temp_path = _persistence_temp_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if temp_path.exists():
        raise ExecutionEventLedgerError(
            "interrupted durable replacement artifact already exists"
        )

    _same_directory_same_volume(temp_path, path)
    _emit_persistence_phase(phase_observer, BEFORE_TEMP_CREATE)
    with temp_path.open("x", encoding="utf-8", newline="") as handle:
        _emit_persistence_phase(phase_observer, TEMP_WRITING)
        written = handle.write(payload)
        if written != len(payload):
            raise ExecutionEventLedgerError("incomplete durable ledger replacement write")
        (adapter or _platform_durability_adapter()).flush_file(handle)
    _emit_persistence_phase(phase_observer, TEMP_FLUSHED)
    _durable_replace(
        temp_path,
        path,
        adapter=adapter,
        phase_observer=phase_observer,
    )



def write_durable_json_metadata(path: str | Path, payload: Mapping[str, object]) -> None:
    """Durably replace one machine-readable harness metadata document."""

    destination = Path(path)
    temp_path = destination.with_name(destination.name + ".tmp")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if temp_path.exists():
        raise ExecutionEventLedgerError(
            f"interrupted durable metadata artifact already exists: {temp_path}"
        )
    serialized = json.dumps(dict(payload), sort_keys=True) + "\n"
    adapter = _platform_durability_adapter()
    _same_directory_same_volume(temp_path, destination)
    with temp_path.open("x", encoding="utf-8", newline="") as handle:
        written = handle.write(serialized)
        if written != len(serialized):
            raise ExecutionEventLedgerError("incomplete durable metadata write")
        adapter.flush_file(handle)
    _durable_replace(temp_path, destination, adapter=adapter)

def rebuild_execution_state_snapshot(
    events: Iterable[ExecutionLedgerEvent],
    canonical_setup_key: str,
) -> ExecutionStateSnapshot:
    """Rebuild one identity state deterministically from ordered local events."""

    key = canonical_setup_key.strip()
    if not key:
        raise ExecutionEventLedgerError("canonical_setup_key is required")

    identity_events = [event for event in events if event.canonical_setup_key == key]
    if not identity_events:
        return ExecutionStateSnapshot(
            canonical_setup_key=key,
            current_state="NO_EVENTS",
            last_event_type="",
            event_count=0,
            has_unknown_state=False,
            has_partial_fill=False,
            terminal=False,
            block_new_orders=False,
            requires_manual_review=False,
            submit_state="NO_SUBMIT",
            submit_confirmed=False,
            submit_unknown=False,
            terminal_submit_failure=False,
        )

    last_event_type = ""
    current_state = "UNKNOWN"
    has_unknown_state = False
    has_partial_fill = False
    terminal = False
    block_new_orders = False
    requires_manual_review = False
    submit_state = "NO_SUBMIT"
    submit_confirmed = False
    submit_unknown = False
    terminal_submit_failure = False

    for event in identity_events:
        last_event_type = event.event_type
        if event.event_type == INTENT_ACCEPTED:
            current_state = "INTENT_ACCEPTED"
        elif event.event_type == MECHANICAL_SAFETY_PASSED:
            current_state = "MECHANICAL_SAFETY_PASSED"
        elif event.event_type == IDEMPOTENCY_ALLOWED:
            current_state = "IDEMPOTENCY_ALLOWED"
        elif event.event_type == EXCHANGE_READY:
            current_state = "EXCHANGE_READY"
        elif event.event_type == SIMULATED_ACKED:
            current_state = "ACKED_PENDING_FILL"
            block_new_orders = True
        elif event.event_type == SIMULATED_REJECTED:
            current_state = "REJECTED_TERMINAL"
            terminal = True
        elif event.event_type == SIMULATED_PARTIALLY_FILLED:
            current_state = "PARTIAL_FILL_PENDING"
            has_partial_fill = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == SIMULATED_FILLED:
            current_state = "FILLED_CONFIRMED"
            terminal = True
            block_new_orders = False
            requires_manual_review = False
        elif event.event_type == SIMULATED_TIMEOUT_UNKNOWN:
            current_state = "UNKNOWN_STATE"
            has_unknown_state = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == SIMULATED_EXCHANGE_UNAVAILABLE:
            current_state = "EXCHANGE_UNAVAILABLE"
            has_unknown_state = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == RECOVERY_DECISION:
            current_state = event.status or "RECOVERY_DECISION"
            block_new_orders = event.block_new_orders
            requires_manual_review = event.requires_manual_review
        elif event.event_type == RESERVED_PRE_SUBMIT:
            current_state = "PRE_SUBMIT_RESERVED"
            block_new_orders = True
            requires_manual_review = event.requires_manual_review
        elif event.event_type == SUBMIT_ACK:
            current_state = "SUBMIT_ACK_CONFIRMED"
            submit_state = "ACK_CONFIRMED"
            submit_confirmed = True
            submit_unknown = False
            terminal_submit_failure = False
            block_new_orders = True
            requires_manual_review = event.requires_manual_review
        elif event.event_type == SUBMIT_REJECT:
            current_state = "SUBMIT_REJECT_TERMINAL"
            submit_state = "REJECTED_TERMINAL"
            submit_confirmed = False
            submit_unknown = False
            terminal_submit_failure = True
            terminal = True
            block_new_orders = event.block_new_orders
            requires_manual_review = event.requires_manual_review
        elif event.event_type == SUBMIT_TIMEOUT_UNKNOWN:
            current_state = "SUBMIT_TIMEOUT_UNKNOWN"
            submit_state = "TIMEOUT_UNKNOWN"
            submit_confirmed = False
            submit_unknown = True
            terminal_submit_failure = False
            has_unknown_state = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == SUBMIT_UNKNOWN:
            current_state = "SUBMIT_UNKNOWN"
            submit_state = "UNKNOWN"
            submit_confirmed = False
            submit_unknown = True
            terminal_submit_failure = False
            has_unknown_state = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == SUBMIT_BLOCKED:
            current_state = "SUBMIT_BLOCKED"
            submit_state = "BLOCKED_NOT_SUBMITTED"
            submit_confirmed = False
            submit_unknown = False
            terminal_submit_failure = False
            block_new_orders = event.block_new_orders
            requires_manual_review = event.requires_manual_review
        elif event.event_type == CLOSE_REQUESTED:
            current_state = "CLOSE_REQUESTED"
            block_new_orders = True
        elif event.event_type == CLOSE_CONFIRMED:
            current_state = "CLOSE_CONFIRMED"
            block_new_orders = True
        elif event.event_type == EXECUTION_COMPLETED:
            current_state = "EXECUTION_COMPLETED"
            terminal = True
            block_new_orders = False
            requires_manual_review = False
        else:  # pragma: no cover - append_event validates supported event types
            raise ExecutionEventLedgerError(f"unsupported execution ledger event type: {event.event_type}")

        if event.event_type in _UNKNOWN_EVENTS:
            has_unknown_state = True
        if event.event_type == SIMULATED_PARTIALLY_FILLED:
            has_partial_fill = True
        if event.event_type in _TERMINAL_EVENTS:
            terminal = True
        if event.event_type == SUBMIT_REJECT:
            terminal = True

    return ExecutionStateSnapshot(
        canonical_setup_key=key,
        current_state=current_state,
        last_event_type=last_event_type,
        event_count=len(identity_events),
        has_unknown_state=has_unknown_state,
        has_partial_fill=has_partial_fill,
        terminal=terminal,
        block_new_orders=block_new_orders,
        requires_manual_review=requires_manual_review,
        submit_state=submit_state,
        submit_confirmed=submit_confirmed,
        submit_unknown=submit_unknown,
        terminal_submit_failure=terminal_submit_failure,
    )


def rebuild_execution_lifecycle_snapshot(
    events: Iterable[ExecutionLedgerEvent],
    canonical_setup_key: str,
) -> ExecutionLifecycleSnapshot:
    """Rebuild the executor lifecycle and fail closed on malformed ordering."""

    key = canonical_setup_key.strip()
    if not key:
        raise ExecutionEventLedgerError("canonical_setup_key is required")

    identity_events = [event for event in events if event.canonical_setup_key == key]
    history: tuple[str, ...] = ()
    for event in identity_events:
        history = _apply_lifecycle_event(history, event.event_type)

    current_state = history[-1] if history else "NO_EVENTS"
    return ExecutionLifecycleSnapshot(
        canonical_setup_key=key,
        current_state=current_state,
        transition_history=history,
        event_count=len(identity_events),
        position_active=current_state == "POSITION_ACTIVE",
        close_requested="CLOSE_REQUESTED" in history,
        close_confirmed="CLOSE_CONFIRMED" in history,
        completed=current_state == "EXECUTION_COMPLETED",
    )


def validate_next_lifecycle_event(
    events: Iterable[ExecutionLedgerEvent],
    canonical_setup_key: str,
    event_type: str,
) -> None:
    """Validate one prospective lifecycle event without mutating the ledger."""

    snapshot = rebuild_execution_lifecycle_snapshot(events, canonical_setup_key)
    _apply_lifecycle_event(snapshot.transition_history, event_type.strip().upper())


def _apply_lifecycle_event(history: tuple[str, ...], event_type: str) -> tuple[str, ...]:
    """Canonical E23 lifecycle transition authority.

    Certified E11/E22 non-lifecycle events are ignored. Lifecycle-bearing
    open/fill events may project more than one state because the certified
    simulator records a single terminal fill event rather than separate
    exchange acknowledgements.
    """

    current = history[-1] if history else "NO_EVENTS"
    lifecycle_event = event_type.strip().upper()

    ignored_events = {
        IDEMPOTENCY_ALLOWED,
        SIMULATED_REJECTED,
        SIMULATED_TIMEOUT_UNKNOWN,
        SIMULATED_EXCHANGE_UNAVAILABLE,
        RECOVERY_DECISION,
        RESERVED_PRE_SUBMIT,
        SUBMIT_ACK,
        SUBMIT_REJECT,
        SUBMIT_TIMEOUT_UNKNOWN,
        SUBMIT_UNKNOWN,
        SUBMIT_BLOCKED,
    }
    if lifecycle_event in ignored_events:
        return history

    if current == "EXECUTION_COMPLETED":
        raise ExecutionLifecycleTransitionError(
            f"lifecycle transition {lifecycle_event} is forbidden after EXECUTION_COMPLETED"
        )

    def require(expected: tuple[str, ...], transition: str) -> None:
        if current not in expected:
            raise ExecutionLifecycleTransitionError(
                f"lifecycle transition {transition} requires {expected}; current state is {current}"
            )

    if lifecycle_event == INTENT_ACCEPTED:
        require(("NO_EVENTS",), "INTENT_ACCEPTED")
        return history + ("INTENT_ACCEPTED",)
    if lifecycle_event == MECHANICAL_SAFETY_PASSED:
        require(("INTENT_ACCEPTED",), "MECHANICAL_ALLOWED")
        return history + ("MECHANICAL_ALLOWED",)
    if lifecycle_event == EXCHANGE_READY:
        require(("MECHANICAL_ALLOWED",), "OPEN_REQUESTED")
        return history + ("OPEN_REQUESTED",)
    if lifecycle_event == SIMULATED_ACKED:
        require(("OPEN_REQUESTED",), "OPEN_CONFIRMED")
        return history + ("OPEN_CONFIRMED",)
    if lifecycle_event == SIMULATED_PARTIALLY_FILLED:
        require(("OPEN_REQUESTED", "OPEN_CONFIRMED", "PARTIALLY_FILLED"), "PARTIALLY_FILLED")
        additions = () if current == "PARTIALLY_FILLED" else (("OPEN_CONFIRMED",) if current == "OPEN_REQUESTED" else ())
        return history + additions + ("PARTIALLY_FILLED",)
    if lifecycle_event == SIMULATED_FILLED:
        require(("OPEN_REQUESTED", "OPEN_CONFIRMED", "PARTIALLY_FILLED"), "FULLY_FILLED")
        additions = ("OPEN_CONFIRMED",) if current == "OPEN_REQUESTED" else ()
        return history + additions + ("FULLY_FILLED", "POSITION_ACTIVE")
    if lifecycle_event == CLOSE_REQUESTED:
        require(("POSITION_ACTIVE",), "CLOSE_REQUESTED")
        return history + ("CLOSE_REQUESTED",)
    if lifecycle_event == CLOSE_CONFIRMED:
        require(("CLOSE_REQUESTED",), "CLOSE_CONFIRMED")
        return history + ("CLOSE_CONFIRMED",)
    if lifecycle_event == EXECUTION_COMPLETED:
        require(("CLOSE_CONFIRMED",), "EXECUTION_COMPLETED")
        return history + ("EXECUTION_COMPLETED",)

    return history

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

# R1 prospective authority-event domain. Legacy APIs above remain byte/behavior compatible.
R1_V1_SCHEMA_VERSION = "ATS_R1_AUTHORITY_EVENT_V1"
LEGACY_V0_FIELDS = frozenset({"sequence","canonical_setup_key","event_type","recorded_at_utc","symbol","side","client_order_id","status","block_new_orders","requires_manual_review","reason"})
R1_V1_FIELDS = frozenset({"schema_version","sequence","operation_identity","canonical_setup_key","event_type","recorded_at_utc","environment","origin","account_identity","client_order_id","client_identity_digest","request_fingerprint","request_fingerprint_version","reservation_id","attempt_id","symbol","operation","exchange_order_id","proof_bundle_digest","classification","block_mutations","query_required","exchange_ret_code","exchange_ret_message","reason"})
R2_V1_SCHEMA_VERSION = "ATS_R2_STARTUP_EVENT_V1"
R2_V1_FIELDS = frozenset({"schema_version","sequence","startup_epoch_id","event_type","recorded_at_utc","origin","account_uid","parent_uid","credential_fingerprint","category","settle_coin","configuration_digest","local_snapshot_digest","local_classification","discovery_bundle_digest","discovery_cutoff_utc","reconciliation_classification","reconstructed_state_digest","block_mutations","reason_code","proof_manifest","proof_manifest_digest","disposition_manifest","disposition_manifest_digest","process_session_identity_digest","local_pre_tip_sequence","local_pre_tip_digest","activation_entry_digest","registry_root_digest","chief_activation_authority_reference","r3_execution_authority_reference"})
R2_EVENT_TYPES = frozenset({"R2_LOCAL_CLASSIFIED","R2_DISCOVERY_STARTED","R2_ACTIVATION_CONSUMPTION_STARTED","R2_DISCOVERY_COMPLETED","R2_RECONCILED_DISARMED","R2_RECONCILIATION_UNKNOWN","R2_STARTUP_ELIGIBILITY_REVOKED","R2_OPERATOR_ARMED"})
R2_CONSUMPTION_FIELDS = ("process_session_identity_digest","local_pre_tip_sequence","local_pre_tip_digest","activation_entry_digest","registry_root_digest","chief_activation_authority_reference","r3_execution_authority_reference")
R2_EMPTY_LEDGER_SENTINEL_DIGEST = hashlib.sha256(b"").hexdigest()

R2_PROOF_MANIFEST_FIELDS=frozenset({"manifest_schema","startup_epoch_id","server_time_start_utc","server_time_end_utc","epoch_monotonic_duration_ms","current_barrier_duration_ms","account_proof","registry_proof","retention_proof","surface_proofs","rules_proofs"})
R2_ACCOUNT_PROOF_FIELDS=frozenset({"origin","account_uid","parent_uid","credential_fingerprint","account_mode","read_only","permissions_digest","request_digest","normalized_response_digest","observed_after_s0","observed_before_s1"})
R2_REGISTRY_PROOF_FIELDS=frozenset({"identity_version","activation_utc","entry_digest","registry_root_digest","chief_authority_reference"})
R2_RETENTION_PROOF_FIELDS=frozenset({"documented_horizon_seconds","safety_margin_seconds","maximum_age_seconds","age_at_s1_microseconds","oldest_request_send_offset_ms","oldest_response_receive_offset_ms","oldest_window_first","continuous_local_authority","retention_sufficient"})
R2_SURFACE_PROOF_FIELDS=frozenset({"surface_id","http_method","endpoint_path","canonical_parameters_digest","coverage_start_utc","coverage_end_utc","first_request_offset_ms","last_response_offset_ms","request_count","page_count","record_count","ordered_page_manifest_digest","normalized_content_digest","cursor_termination","schema_contract_id","complete"})
R2_RULES_PROOF_FIELDS=frozenset({"symbol","rules_schema_version","provenance_digest","normalized_rules_digest","observed_at_utc","expires_at_utc"})
R2_DISPOSITION_MANIFEST_FIELDS=frozenset({"manifest_schema","proof_event_sequence","proof_manifest_digest","local_projection_digest","operation_dispositions","identity_class_counts","order_state_counts","execution_count","nonzero_position_count","open_order_count","unknown_count","blocking_reason_codes","result"})
R2_OPERATION_DISPOSITION_FIELDS=frozenset({"local_dispatch_sequence","operation_kind","client_identity_digest","exchange_order_id_digest","remote_disposition","correlation_proof_digest"})

def _r2_canonical_digest(value): return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode("utf-8")).hexdigest()
def _r2_exact_obj(value,fields,name):
    if not isinstance(value,Mapping) or frozenset(value)!=fields: raise ExecutionEventLedgerError(name+" exact schema mismatch")
def _validate_r2_proof_manifest(m):
    _r2_exact_obj(m,R2_PROOF_MANIFEST_FIELDS,"proof_manifest");
    if m.get("manifest_schema")!="ATS_R2_DISCOVERY_PROOF_MANIFEST_V1": raise ExecutionEventLedgerError("proof manifest version mismatch")
    _r2_exact_obj(m.get("account_proof"),R2_ACCOUNT_PROOF_FIELDS,"account_proof"); _r2_exact_obj(m.get("registry_proof"),R2_REGISTRY_PROOF_FIELDS,"registry_proof"); _r2_exact_obj(m.get("retention_proof"),R2_RETENTION_PROOF_FIELDS,"retention_proof")
    rp=m["retention_proof"]
    if (rp.get("documented_horizon_seconds"),rp.get("safety_margin_seconds"),rp.get("maximum_age_seconds"))!=(86400,300,86100): raise ExecutionEventLedgerError("retention constants mismatch")
    if not isinstance(m.get("surface_proofs"),list) or not m["surface_proofs"]: raise ExecutionEventLedgerError("surface proofs required")
    for x in m["surface_proofs"]: _r2_exact_obj(x,R2_SURFACE_PROOF_FIELDS,"surface_proof")
    if not isinstance(m.get("rules_proofs"),list): raise ExecutionEventLedgerError("rules proofs array required")
    for x in m["rules_proofs"]: _r2_exact_obj(x,R2_RULES_PROOF_FIELDS,"rules_proof")
    ids=[x["surface_id"] for x in m["surface_proofs"]]
    mandatory={"SERVER_TIME_S0","API_KEY_INFO","REALTIME_OPEN_ORDERS","POSITIONS","ORDER_HISTORY_OVERLAP","EXECUTION_HISTORY_OVERLAP","SERVER_TIME_S1"}
    if not mandatory.issubset(set(ids)) or len(ids)!=len(set(ids)) or not all(x.get("complete") is True for x in m["surface_proofs"]): raise ExecutionEventLedgerError("proof mandatory surface completeness failure")
    order_idx=[i for i,x in enumerate(ids) if x.startswith("ORDER_HISTORY:")]
    exec_idx=[i for i,x in enumerate(ids) if x.startswith("EXECUTION_HISTORY:")]
    if not order_idx or not exec_idx: raise ExecutionEventLedgerError("proof historical windows required")
    order_names=[ids[i] for i in order_idx]; exec_names=[ids[i] for i in exec_idx]
    if order_names!=[f"ORDER_HISTORY:{i}" for i in range(len(order_names))] or exec_names!=[f"EXECUTION_HISTORY:{i}" for i in range(len(exec_names))]: raise ExecutionEventLedgerError("proof historical window order invalid")
    expected=["SERVER_TIME_S0","API_KEY_INFO"]+order_names+exec_names+["SERVER_TIME_S1","ORDER_HISTORY_OVERLAP","EXECUTION_HISTORY_OVERLAP","REALTIME_OPEN_ORDERS","POSITIONS"]
    if ids!=expected: raise ExecutionEventLedgerError("proof surface order invalid")
    for x in m["surface_proofs"]:
        if x.get("http_method")!="GET" or type(x.get("request_count")) is not int or x["request_count"]<1 or type(x.get("page_count")) is not int or x["page_count"]<1 or x["request_count"]!=x["page_count"]: raise ExecutionEventLedgerError("surface proof request/page contract invalid")
        if type(x.get("first_request_offset_ms")) is not int or type(x.get("last_response_offset_ms")) is not int or x["first_request_offset_ms"]>x["last_response_offset_ms"]: raise ExecutionEventLedgerError("surface proof offsets invalid")
        if not isinstance(x.get("cursor_termination"),str) or x["cursor_termination"] not in {"EMPTY_CURSOR","NO_CURSOR"}: raise ExecutionEventLedgerError("surface proof cursor termination invalid")
    rules=m["rules_proofs"]; syms=[x.get("symbol") for x in rules]
    if syms!=sorted(set(syms)): raise ExecutionEventLedgerError("rules proof order/uniqueness invalid")
    try:
        s0=datetime.fromisoformat(str(m["server_time_start_utc"]).replace("Z","+00:00")); s1=datetime.fromisoformat(str(m["server_time_end_utc"]).replace("Z","+00:00"))
    except Exception as exc: raise ExecutionEventLedgerError("proof server time invalid") from exc
    if s1<s0 or type(m.get("epoch_monotonic_duration_ms")) is not int or not (0<=m["epoch_monotonic_duration_ms"]<=120000) or type(m.get("current_barrier_duration_ms")) is not int or not (0<=m["current_barrier_duration_ms"]<=30000): raise ExecutionEventLedgerError("proof freshness bounds invalid")
    by_id={x["surface_id"]:x for x in m["surface_proofs"]}
    c=(s0-timedelta(seconds=60)).isoformat(timespec="microseconds").replace("+00:00","Z")
    for sid in ("ORDER_HISTORY_OVERLAP","EXECUTION_HISTORY_OVERLAP"):
        x=by_id[sid]
        if x.get("coverage_start_utc")!=c or x.get("coverage_end_utc")!=m["server_time_end_utc"]: raise ExecutionEventLedgerError("proof overlap coverage invalid")
        if x["first_request_offset_ms"] < by_id["SERVER_TIME_S1"]["last_response_offset_ms"]: raise ExecutionEventLedgerError("proof overlap precedes S1")
    overlap_end=max(by_id["ORDER_HISTORY_OVERLAP"]["last_response_offset_ms"],by_id["EXECUTION_HISTORY_OVERLAP"]["last_response_offset_ms"])
    if by_id["REALTIME_OPEN_ORDERS"]["first_request_offset_ms"] < overlap_end or by_id["POSITIONS"]["first_request_offset_ms"] < overlap_end: raise ExecutionEventLedgerError("proof current snapshot precedes overlap completion")
    if rp.get("retention_sufficient") is True and (type(rp.get("age_at_s1_microseconds")) is not int or not (0<=rp["age_at_s1_microseconds"]<86100*1000000) or rp.get("oldest_window_first") is not True): raise ExecutionEventLedgerError("retention success predicate invalid")
    for x in rules:
        try: exp=datetime.fromisoformat(str(x["expires_at_utc"]).replace("Z","+00:00"))
        except Exception as exc: raise ExecutionEventLedgerError("rules expiry invalid") from exc
        if exp < s1+timedelta(seconds=60): raise ExecutionEventLedgerError("rules proof does not cover capability lifetime")
def _validate_r2_disposition_manifest(m):
    _r2_exact_obj(m,R2_DISPOSITION_MANIFEST_FIELDS,"disposition_manifest")
    if m.get("manifest_schema")!="ATS_R2_RECONCILIATION_DISPOSITION_V1": raise ExecutionEventLedgerError("disposition manifest version mismatch")
    if not isinstance(m.get("operation_dispositions"),list) or not isinstance(m.get("blocking_reason_codes"),list): raise ExecutionEventLedgerError("disposition arrays invalid")
    for x in m["operation_dispositions"]: _r2_exact_obj(x,R2_OPERATION_DISPOSITION_FIELDS,"operation_disposition")

@dataclass(frozen=True)
class R2StartupEvent:
    schema_version:str; sequence:int; startup_epoch_id:str; event_type:str; recorded_at_utc:str; origin:str; account_uid:int; parent_uid:int; credential_fingerprint:str; category:str; settle_coin:str; configuration_digest:str
    local_snapshot_digest:object; local_classification:object; discovery_bundle_digest:object; discovery_cutoff_utc:object; reconciliation_classification:object; reconstructed_state_digest:object; block_mutations:bool; reason_code:str
    proof_manifest:object; proof_manifest_digest:object; disposition_manifest:object; disposition_manifest_digest:object
    process_session_identity_digest:object; local_pre_tip_sequence:object; local_pre_tip_digest:object; activation_entry_digest:object; registry_root_digest:object; chief_activation_authority_reference:object; r3_execution_authority_reference:object

def _validate_r2_event_obj(obj,line_no=0,prior=(),expected_pre_tip_digest=None):
    loc=f" at line {line_no}" if line_no else ""
    if frozenset(obj)!=R2_V1_FIELDS: raise ExecutionEventLedgerError("R2 exact field set mismatch"+loc)
    if obj.get("schema_version")!=R2_V1_SCHEMA_VERSION or obj.get("event_type") not in R2_EVENT_TYPES: raise ExecutionEventLedgerError("R2 schema/event mismatch"+loc)
    if type(obj.get("sequence")) is not int or obj["sequence"]<1 or type(obj.get("account_uid")) is not int or type(obj.get("parent_uid")) is not int or type(obj.get("block_mutations")) is not bool: raise ExecutionEventLedgerError("R2 scalar type mismatch"+loc)
    if obj.get("origin")!="https://api-testnet.bybit.eu" or obj.get("account_uid")!=107087555 or obj.get("parent_uid")!=0 or obj.get("category")!="linear" or obj.get("settle_coin")!="USDT": raise ExecutionEventLedgerError("R2 authority context mismatch"+loc)
    for f in ("startup_epoch_id","recorded_at_utc","credential_fingerprint","configuration_digest","reason_code"):
        if not isinstance(obj.get(f),str) or not obj[f]: raise ExecutionEventLedgerError("R2 required string invalid: "+f+loc)
    proof=obj.get("proof_manifest"); pd=obj.get("proof_manifest_digest"); disp=obj.get("disposition_manifest"); dd=obj.get("disposition_manifest_digest")
    if (proof is None)!=(pd is None) or (disp is None)!=(dd is None): raise ExecutionEventLedgerError("R2 manifest/digest nullability mismatch"+loc)
    if proof is not None:
        _validate_r2_proof_manifest(proof)
        if not isinstance(pd,str) or pd!=_r2_canonical_digest(proof): raise ExecutionEventLedgerError("proof manifest digest mismatch"+loc)
    if disp is not None:
        _validate_r2_disposition_manifest(disp)
        if not isinstance(dd,str) or dd!=_r2_canonical_digest(disp): raise ExecutionEventLedgerError("disposition manifest digest mismatch"+loc)
    if obj["event_type"]=="R2_DISCOVERY_COMPLETED" and (proof is None or disp is not None): raise ExecutionEventLedgerError("discovery-completed manifest contract invalid"+loc)
    if obj["event_type"]=="R2_RECONCILED_DISARMED" and (proof is None or disp is None or disp.get("result")!="RECONCILED_CLEAN"): raise ExecutionEventLedgerError("reconciled-disarmed manifest contract invalid"+loc)
    if obj["event_type"]=="R2_RECONCILED_DISARMED":
        same=[e for e in prior if getattr(e,"startup_epoch_id",None)==obj["startup_epoch_id"]]
        pe=[e for e in same if getattr(e,"sequence",None)==disp.get("proof_event_sequence") and getattr(e,"event_type",None)=="R2_DISCOVERY_COMPLETED"]
        if len(pe)!=1 or getattr(pe[0],"proof_manifest_digest",None)!=pd or disp.get("proof_manifest_digest")!=pd: raise ExecutionEventLedgerError("reconciled proof reference invalid"+loc)
        if disp.get("unknown_count")!=0 or disp.get("open_order_count")!=0 or disp.get("nonzero_position_count")!=0 or disp.get("blocking_reason_codes")!=[] or obj.get("reconciliation_classification") not in (None,"RECONCILED_CLEAN"): raise ExecutionEventLedgerError("reconciled clean predicate invalid"+loc)
    is_c=obj["event_type"]=="R2_ACTIVATION_CONSUMPTION_STARTED"
    if is_c:
        if any(obj.get(f) is not None for f in ("proof_manifest","proof_manifest_digest","disposition_manifest","disposition_manifest_digest")): raise ExecutionEventLedgerError("consumption manifest fields must be null"+loc)
        import re as _re
        for f in ("process_session_identity_digest","local_pre_tip_digest","activation_entry_digest","registry_root_digest"):
            if not isinstance(obj.get(f),str) or not _re.fullmatch(r"[0-9a-f]{64}",obj[f]): raise ExecutionEventLedgerError("consumption digest invalid: "+f+loc)
        if type(obj.get("local_pre_tip_sequence")) is not int or obj["local_pre_tip_sequence"]<0 or obj["local_pre_tip_sequence"]!=obj["sequence"]-1: raise ExecutionEventLedgerError("consumption pre-tip sequence invalid"+loc)
        for f in ("chief_activation_authority_reference","r3_execution_authority_reference"):
            if not isinstance(obj.get(f),str) or not obj[f] or obj[f].strip()!=obj[f]: raise ExecutionEventLedgerError("consumption reference invalid"+loc)
        if obj.get("block_mutations") is not True or obj.get("reason_code")!="ACTIVATION_CONSUMED_BEFORE_DISCOVERY": raise ExecutionEventLedgerError("consumption disposition invalid"+loc)
        same=[e for e in prior if getattr(e,"startup_epoch_id",None)==obj["startup_epoch_id"]]
        if any(getattr(e,"event_type","")=="R2_ACTIVATION_CONSUMPTION_STARTED" for e in same): raise ExecutionEventLedgerError("duplicate activation consumption"+loc)
        if any(getattr(e,"event_type","") in {"R2_DISCOVERY_COMPLETED","R2_RECONCILED_DISARMED","R2_OPERATOR_ARMED"} for e in same): raise ExecutionEventLedgerError("illegal pre-consumption ordering"+loc)
        if obj["local_pre_tip_sequence"]==0 and obj["local_pre_tip_digest"]!=R2_EMPTY_LEDGER_SENTINEL_DIGEST: raise ExecutionEventLedgerError("empty-ledger sentinel mismatch"+loc)
        if expected_pre_tip_digest is not None and obj["local_pre_tip_digest"]!=expected_pre_tip_digest: raise ExecutionEventLedgerError("consumption physical pre-tip digest mismatch"+loc)
    else:
        if any(obj.get(f) is not None for f in R2_CONSUMPTION_FIELDS): raise ExecutionEventLedgerError("non-consumption fields must be null"+loc)
        if obj["event_type"] in {"R2_DISCOVERY_STARTED","R2_DISCOVERY_COMPLETED","R2_RECONCILED_DISARMED","R2_RECONCILIATION_UNKNOWN","R2_STARTUP_ELIGIBILITY_REVOKED","R2_OPERATOR_ARMED"}:
            same=[e for e in prior if getattr(e,"startup_epoch_id",None)==obj["startup_epoch_id"]]
            if len([e for e in same if getattr(e,"event_type",None)=="R2_ACTIVATION_CONSUMPTION_STARTED"])!=1: raise ExecutionEventLedgerError("R2 event requires earlier same-epoch activation consumption"+loc)
        if obj["event_type"]=="R2_RECONCILED_DISARMED":
            same=[e for e in prior if getattr(e,"startup_epoch_id",None)==obj["startup_epoch_id"]]
            proofs=[e for e in same if getattr(e,"event_type",None)=="R2_DISCOVERY_COMPLETED"]
            if len(proofs)!=1 or disp.get("proof_event_sequence")!=proofs[0].sequence or disp.get("proof_manifest_digest")!=proofs[0].proof_manifest_digest: raise ExecutionEventLedgerError("reconciled proof reference mismatch"+loc)

R1_EVENT_TYPES = frozenset({"CREATE_RESERVED","CREATE_RESERVATION_EXPIRED","CREATE_DISPATCHING","CREATE_ACK_PENDING_QUERY","CREATE_REJECT_CONFIRMED","CREATE_UNKNOWN","QUERY_OBSERVED","CANCEL_DISPATCHING","CANCEL_ACK_PENDING_QUERY","CANCEL_CONFIRMED","CANCEL_NOT_EFFECTIVE_TERMINAL","CANCEL_NOT_CONFIRMED","CANCEL_UNKNOWN","AUTHORITY_BLOCKED","IDENTITY_CONFLICT"})

@dataclass(frozen=True)
class R1AuthorityEvent:
    schema_version: str; sequence: int; operation_identity: str; canonical_setup_key: str; event_type: str; recorded_at_utc: str
    environment: str; origin: str; account_identity: str; client_order_id: str; client_identity_digest: str; request_fingerprint: str
    request_fingerprint_version: str; reservation_id: str; attempt_id: str; symbol: str; operation: str; exchange_order_id: str
    proof_bundle_digest: str; classification: str; block_mutations: bool; query_required: bool; exchange_ret_code: str; exchange_ret_message: str; reason: str

@dataclass(frozen=True)
class R1LedgerProjection:
    events: tuple[R1AuthorityEvent,...]
    block_mutations: bool
    unresolved_dispatch: bool
    persistence_unknown: bool=False

def _r1_pairs_object(pairs):
    d={}
    for k,v in pairs:
        if k in d: raise ExecutionEventLedgerError(f"duplicate JSON key: {k}")
        d[k]=v
    return d

def _parse_union_records(raw: bytes):
    if raw and not raw.endswith(b"\n"): raise ExecutionEventLedgerError("truncated persisted execution ledger write")
    union=[]; legacy=[]; r1=[]; r2=[]; physical_prefix=b""
    for line_no,line_with_newline in enumerate(raw.splitlines(keepends=True),1):
        line=line_with_newline[:-1] if line_with_newline.endswith(b"\n") else line_with_newline
        if not line.strip(): raise ExecutionEventLedgerError(f"blank persisted execution ledger record at line {line_no}")
        expected_pre_tip_digest=hashlib.sha256(physical_prefix).hexdigest()
        try: obj=json.loads(line.decode("utf-8"),object_pairs_hook=_r1_pairs_object)
        except (UnicodeDecodeError,json.JSONDecodeError) as exc: raise ExecutionEventLedgerError(f"malformed persisted execution ledger record at line {line_no}") from exc
        if not isinstance(obj,Mapping): raise ExecutionEventLedgerError(f"persisted execution ledger record must be an object at line {line_no}")
        keys=frozenset(obj)
        if keys==LEGACY_V0_FIELDS:
            ev=_execution_event_from_persisted_record(obj,line_no); legacy.append(ev); parsed=ev
        elif keys==R1_V1_FIELDS:
            if obj.get("schema_version")!=R1_V1_SCHEMA_VERSION: raise ExecutionEventLedgerError(f"unsupported R1 schema at line {line_no}")
            if type(obj.get("sequence")) is not int or obj["sequence"]<1: raise ExecutionEventLedgerError(f"invalid R1 sequence at line {line_no}")
            if obj.get("event_type") not in R1_EVENT_TYPES: raise ExecutionEventLedgerError(f"invalid R1 event type at line {line_no}")
            for f in R1_V1_FIELDS-{"sequence","block_mutations","query_required"}:
                if not isinstance(obj.get(f),str): raise ExecutionEventLedgerError(f"invalid R1 {f} at line {line_no}")
            if type(obj.get("block_mutations")) is not bool or type(obj.get("query_required")) is not bool: raise ExecutionEventLedgerError(f"invalid R1 boolean at line {line_no}")
            parsed=R1AuthorityEvent(**obj); r1.append(parsed)
        elif keys==R2_V1_FIELDS:
            _validate_r2_event_obj(obj,line_no,r2,expected_pre_tip_digest); parsed=R2StartupEvent(**obj); r2.append(parsed)
        else: raise ExecutionEventLedgerError(f"persisted execution ledger schema mismatch at line {line_no}")
        if parsed.sequence!=len(union)+1: raise ExecutionEventLedgerError(f"persisted execution ledger sequence gap/conflict at line {line_no}: expected {len(union)+1}, got {parsed.sequence}")
        union.append(parsed); physical_prefix += line_with_newline
    # Preserve legacy projection semantics by validating V0 relative order with sequence-renumbered copies.
    normalized=[ExecutionLedgerEvent(**{**asdict(e),"sequence":i}) for i,e in enumerate(legacy,1)]
    for key in sorted({e.canonical_setup_key for e in normalized}):
        rebuild_execution_lifecycle_snapshot(normalized,key); rebuild_execution_state_snapshot(normalized,key)
    return tuple(union),tuple(legacy),tuple(r1),tuple(r2)

class R1AuthorityLedger:
    def __init__(self, path:str|Path, *, persistence_phase_observer:Callable[[str],None]|None=None):
        self._path=Path(path).resolve(); self._path.parent.mkdir(parents=True,exist_ok=True); self._owner=_LedgerWriterOwnership(self._path); self._observer=persistence_phase_observer; self._health=PERSISTENCE_HEALTHY
        try:
            if _persistence_temp_path(self._path).exists(): raise ExecutionEventLedgerError("interrupted durable replacement artifact exists")
            raw=self._path.read_bytes() if self._path.exists() else b""; self._union,self._legacy,self._r1,self._r2=_parse_union_records(raw)
        except Exception: self.close(); raise
    @property
    def events(self): return self._r1
    @property
    def r2_events(self): return self._r2
    @property
    def union_events(self): return self._union
    @property
    def persistence_health(self): return self._health
    def physical_tip(self):
        raw=self._path.read_bytes() if self._path.exists() else b""
        return len(self._union), hashlib.sha256(raw).hexdigest()
    def exact_reread(self):
        raw=self._path.read_bytes() if self._path.exists() else b""
        return _parse_union_records(raw)
    def projection(self)->R1LedgerProjection:
        unresolved=False; block=False
        by_attempt={}
        for e in self._r1:
            if e.attempt_id: by_attempt.setdefault(e.attempt_id,[]).append(e.event_type)
            block=block or e.event_type == "IDENTITY_CONFLICT"
        for hist in by_attempt.values():
            if any(x in hist for x in ("CREATE_DISPATCHING","CANCEL_DISPATCHING")) and not any(x in hist for x in ("CREATE_ACK_PENDING_QUERY","CREATE_REJECT_CONFIRMED","CREATE_UNKNOWN","CANCEL_ACK_PENDING_QUERY","CANCEL_CONFIRMED","CANCEL_NOT_EFFECTIVE_TERMINAL","CANCEL_NOT_CONFIRMED","CANCEL_UNKNOWN")): unresolved=True
            if "CREATE_UNKNOWN" in hist and "CREATE_REJECT_CONFIRMED" not in hist: block=True
            if "CANCEL_UNKNOWN" in hist and not any(x in hist for x in ("CANCEL_CONFIRMED","CANCEL_NOT_EFFECTIVE_TERMINAL","CANCEL_NOT_CONFIRMED")): block=True
        return R1LedgerProjection(self._r1,block or unresolved,unresolved,self._health!=PERSISTENCE_HEALTHY)
    def append(self, **fields)->R1AuthorityEvent:
        if self._health!=PERSISTENCE_HEALTHY: raise PersistenceStateUnknownError("R1 persistence state unknown")
        ev=R1AuthorityEvent(schema_version=R1_V1_SCHEMA_VERSION,sequence=len(self._union)+1,**fields)
        if ev.event_type not in R1_EVENT_TYPES: raise ExecutionEventLedgerError("unsupported R1 event")
        all_records=[asdict(x) for x in self._union]+[asdict(ev)]
        payload="".join(json.dumps(x,sort_keys=True)+"\n" for x in all_records)
        tmp=_persistence_temp_path(self._path)
        try:
            _same_directory_same_volume(tmp,self._path); _emit_persistence_phase(self._observer,BEFORE_TEMP_CREATE)
            with tmp.open("x",encoding="utf-8",newline="") as h:
                _emit_persistence_phase(self._observer,TEMP_WRITING); h.write(payload); _platform_durability_adapter().flush_file(h)
            _emit_persistence_phase(self._observer,TEMP_FLUSHED); _durable_replace(tmp,self._path,phase_observer=self._observer)
        except Exception as exc:
            self._health=PERSISTENCE_STATE_UNKNOWN; raise PersistenceStateUnknownError("R1 persistent write outcome unknown") from exc
        self._union=tuple(list(self._union)+[ev]); self._r1=tuple(list(self._r1)+[ev]); _emit_persistence_phase(self._observer,COMMIT_ACKNOWLEDGED); return ev
    def append_r2(self, **fields)->R2StartupEvent:
        if self._health!=PERSISTENCE_HEALTHY: raise PersistenceStateUnknownError("R2 persistence state unknown")
        ev=R2StartupEvent(schema_version=R2_V1_SCHEMA_VERSION,sequence=len(self._union)+1,**fields)
        _validate_r2_event_obj(asdict(ev),0,self._r2,self.physical_tip()[1] if ev.event_type=="R2_ACTIVATION_CONSUMPTION_STARTED" else None)
        all_records=[asdict(x) for x in self._union]+[asdict(ev)]
        payload="".join(json.dumps(x,sort_keys=True)+"\n" for x in all_records)
        tmp=_persistence_temp_path(self._path)
        try:
            _same_directory_same_volume(tmp,self._path); _emit_persistence_phase(self._observer,BEFORE_TEMP_CREATE)
            with tmp.open("x",encoding="utf-8",newline="") as h:
                _emit_persistence_phase(self._observer,TEMP_WRITING); h.write(payload); _platform_durability_adapter().flush_file(h)
            _emit_persistence_phase(self._observer,TEMP_FLUSHED); _durable_replace(tmp,self._path,phase_observer=self._observer)
        except Exception as exc:
            self._health=PERSISTENCE_STATE_UNKNOWN; raise PersistenceStateUnknownError("R2 persistent write outcome unknown") from exc
        self._union=tuple(list(self._union)+[ev]); self._r2=tuple(list(self._r2)+[ev]); _emit_persistence_phase(self._observer,COMMIT_ACKNOWLEDGED); return ev
    def close(self):
        if getattr(self,"_owner",None): self._owner.release(); self._owner=None
    def __enter__(self): return self
    def __exit__(self,*args): self.close()
