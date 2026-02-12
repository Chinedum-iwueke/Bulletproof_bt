"""Resolve and validate runtime configuration into a single canonical shape."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import copy


@dataclass(frozen=True)
class ResolvedConfig:
    raw: dict[str, Any]
    resolved: dict[str, Any]


class ConfigError(ValueError):
    """Raised when configuration keys conflict or are invalid."""


def _ensure_mapping(value: Any, *, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a mapping when provided")
    return value


def _resolve_risk_value(
    *,
    resolved: dict[str, Any],
    top_key: str,
    nested_key: str,
    default: Any,
) -> None:
    risk_cfg = _ensure_mapping(resolved.get("risk"), name="risk")
    top_present = top_key in resolved
    nested_present = nested_key in risk_cfg

    if top_present and nested_present and resolved[top_key] != risk_cfg[nested_key]:
        raise ConfigError(
            f"Conflicting config values for '{top_key}' ({resolved[top_key]!r}) "
            f"and 'risk.{nested_key}' ({risk_cfg[nested_key]!r}). "
            "Define only one or make them equal."
        )

    if nested_present:
        canonical_value = risk_cfg[nested_key]
    elif top_present:
        canonical_value = resolved[top_key]
    else:
        canonical_value = default

    risk_cfg[nested_key] = canonical_value
    resolved["risk"] = risk_cfg


def resolve_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize config into one authoritative shape.
    - Enforce precedence rules.
    - Reject contradictions (don't silently pick).
    - Return a deep-copied resolved dict to be used by runners/engine wiring.
    """
    resolved = copy.deepcopy(cfg)
    if not isinstance(resolved, dict):
        raise ConfigError("Config root must be a mapping")

    _resolve_risk_value(
        resolved=resolved,
        top_key="max_positions",
        nested_key="max_positions",
        default=1,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="risk_per_trade_pct",
        nested_key="risk_per_trade_pct",
        default=0.01,
    )

    return resolved
