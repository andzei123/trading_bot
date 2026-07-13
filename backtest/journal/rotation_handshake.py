from __future__ import annotations

"""Explicit R7 writer/rotation handshake state machine.

Test-harness only. No production runtime integration.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List


class HandshakeState(str, Enum):
    WRITER_RUNNING = "WRITER_RUNNING"
    ROTATION_REQUESTED = "ROTATION_REQUESTED"
    PAUSE_REQUESTED = "PAUSE_REQUESTED"
    WRITER_FLUSHED = "WRITER_FLUSHED"
    WRITER_CLOSED = "WRITER_CLOSED"
    ROTATION_EXECUTED = "ROTATION_EXECUTED"
    ACTIVE_RECREATED = "ACTIVE_RECREATED"
    WRITER_REOPENED = "WRITER_REOPENED"
    WRITER_RESUMED = "WRITER_RESUMED"
    VERIFIED = "VERIFIED"
    ABORTED = "ABORTED"


ORDER = [
    HandshakeState.WRITER_RUNNING,
    HandshakeState.ROTATION_REQUESTED,
    HandshakeState.PAUSE_REQUESTED,
    HandshakeState.WRITER_FLUSHED,
    HandshakeState.WRITER_CLOSED,
    HandshakeState.ROTATION_EXECUTED,
    HandshakeState.ACTIVE_RECREATED,
    HandshakeState.WRITER_REOPENED,
    HandshakeState.WRITER_RESUMED,
    HandshakeState.VERIFIED,
]


class HandshakeError(RuntimeError):
    """Raised when the explicit state sequence is violated."""


@dataclass
class RotationHandshake:
    state: HandshakeState = HandshakeState.WRITER_RUNNING
    history: List[str] = field(default_factory=lambda: [HandshakeState.WRITER_RUNNING.value])

    def transition(self, expected: HandshakeState, new_state: HandshakeState) -> None:
        if self.state != expected:
            self.state = HandshakeState.ABORTED
            self.history.append(HandshakeState.ABORTED.value)
            raise HandshakeError(f"expected {expected.value}, found {self.history[-2]}")
        current_index = ORDER.index(expected)
        if current_index + 1 >= len(ORDER) or ORDER[current_index + 1] != new_state:
            self.state = HandshakeState.ABORTED
            self.history.append(HandshakeState.ABORTED.value)
            raise HandshakeError(f"invalid transition {expected.value}->{new_state.value}")
        self.state = new_state
        self.history.append(new_state.value)

    def require(self, state: HandshakeState) -> None:
        if self.state != state:
            raise HandshakeError(f"required {state.value}, found {self.state.value}")


def classify_interruption(state: HandshakeState) -> str:
    """Classify R7 interruption boundaries without implementing recovery."""
    mapping = {
        HandshakeState.ROTATION_REQUESTED: "CRASH_BEFORE_PAUSE_ACK_NO_MUTATION_RETRY_SAFE",
        HandshakeState.WRITER_CLOSED: "CRASH_AFTER_CLOSE_BEFORE_RENAME_ACTIVE_INTACT_WRITER_STOPPED",
        HandshakeState.ROTATION_EXECUTED: "CRASH_AFTER_RENAME_BEFORE_ACTIVE_RECREATE_ACTIVE_MISSING",
        HandshakeState.ACTIVE_RECREATED: "CRASH_AFTER_ACTIVE_RECREATE_BEFORE_REOPEN_WRITER_STOPPED",
    }
    return mapping.get(state, "UNCLASSIFIED_INTERRUPTION_STATE")
