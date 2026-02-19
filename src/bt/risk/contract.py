from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class StopSpec:
    """
    Normalized stop intent carried from Strategy -> Risk boundary.

    This is a data container only. No behavior in Task X2.
    """

    kind: Literal["explicit", "structural", "atr", "hybrid"]
    contract_version: int = 1

    # For explicit/structural
    stop_price: float | None = None

    # For atr / hybrid
    atr_multiple: float | None = None

    # Optional: per-signal override; otherwise config decides later
    hybrid_policy: Literal["wider", "tighter"] | None = None

    # Diagnostics (optional, do not rely on it)
    raw_source: str | None = None
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class StopResolutionResult:
    """
    Normalized output of stop resolution.
    Pure container only in Task X2.
    """

    stop_price: float | None
    stop_distance: float | None
    stop_source: str
    is_valid: bool
    used_fallback: bool
    reason_code: str
    details: dict[str, Any] | None = None


class StrategyContractError(ValueError):
    """Raised when a strategy violates a required contract (used for config/schema-level issues)."""
