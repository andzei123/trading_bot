from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
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
