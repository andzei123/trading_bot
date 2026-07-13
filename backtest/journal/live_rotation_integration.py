from __future__ import annotations

"""Disabled production integration boundary for live journal rotation.

R8 establishes exactly one callable boundary between the production shell and a
future executor. It does not import or execute the R6 filesystem executor.
Production mutation remains mechanically disabled in both configuration modes.
"""

from dataclasses import dataclass
import os
from typing import Callable, Optional


ENV_ENABLE = "ATS_LIVE_ROTATION_INTEGRATION_ENABLED"

STATUS_DISABLED = "INTEGRATION_DISABLED"
STATUS_BOUNDARY_REACHED = "BOUNDARY_REACHED_EXECUTION_DISABLED"


@dataclass(frozen=True)
class LiveRotationIntegrationConfig:
    enabled: bool = False
    execution_enabled: bool = False

    @classmethod
    def from_environment(cls) -> "LiveRotationIntegrationConfig":
        value = os.getenv(ENV_ENABLE, "").strip().lower()
        enabled = value in {"1", "true", "yes", "on"}
        return cls(enabled=enabled, execution_enabled=False)


class LiveRotationIntegrationBoundary:
    """Non-mutating R8 boundary.

    Even when integration is explicitly enabled, execution_enabled is fixed to
    False and the executor callback is never invoked.
    """

    def __init__(self, config: LiveRotationIntegrationConfig) -> None:
        self.config = config

    def reach(self, executor: Optional[Callable[[], object]] = None) -> str:
        if not self.config.enabled:
            return STATUS_DISABLED
        if not self.config.execution_enabled:
            return STATUS_BOUNDARY_REACHED
        # R8 cannot reach this branch because execution_enabled is fixed false.
        # Keep the guard fail-closed if a future caller constructs bad config.
        raise RuntimeError("R8 production rotation execution is disabled")
