"""Structured risk sizing configuration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class RiskSpec:
    """Validated risk sizing settings.

    Defaults:
    - ``min_stop_distance`` defaults to ``None`` when omitted.
    - ``max_leverage`` defaults to ``None`` when omitted.
    """

    mode: Literal["r_fixed", "equity_pct"]
    r_per_trade: float
    min_stop_distance: float | None
    max_leverage: float | None
    maintenance_free_margin_pct: float


def _as_positive_float(value: object, key: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Invalid risk.{key}: expected positive float got {value!r}")

    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"Invalid risk.{key}: expected positive float got {value!r}")
    return parsed


def _as_optional_positive_float(value: object, key: str) -> float | None:
    if value is None:
        return None
    return _as_positive_float(value, key)


def parse_risk_spec(config: dict[str, object]) -> RiskSpec:
    """Parse and validate a :class:`RiskSpec` from a config mapping."""

    risk_cfg = config.get("risk", {})
    if not isinstance(risk_cfg, dict):
        raise ValueError("risk.mode and risk.r_per_trade are required")

    raw_mode = risk_cfg.get("mode")
    raw_r_per_trade = risk_cfg.get("r_per_trade")
    if raw_mode is None or raw_r_per_trade is None:
        raise ValueError("risk.mode and risk.r_per_trade are required")

    if raw_mode not in ("r_fixed", "equity_pct"):
        raise ValueError(f"Invalid risk.mode: expected 'r_fixed' or 'equity_pct' got {raw_mode!r}")

    r_per_trade = _as_positive_float(raw_r_per_trade, "r_per_trade")
    min_stop_distance = _as_optional_positive_float(risk_cfg.get("min_stop_distance"), "min_stop_distance")
    max_leverage = _as_optional_positive_float(risk_cfg.get("max_leverage"), "max_leverage")

    raw_maintenance = risk_cfg.get("maintenance_free_margin_pct", 0.01)
    if isinstance(raw_maintenance, bool) or not isinstance(raw_maintenance, (int, float)):
        raise ValueError(
            "Invalid risk.maintenance_free_margin_pct: expected float in [0.0, 0.20] "
            f"got {raw_maintenance!r}"
        )
    maintenance_free_margin_pct = float(raw_maintenance)
    if not (0.0 <= maintenance_free_margin_pct <= 0.20):
        raise ValueError(
            "Invalid risk.maintenance_free_margin_pct: expected float in [0.0, 0.20] "
            f"got {raw_maintenance!r}"
        )

    return RiskSpec(
        mode=raw_mode,
        r_per_trade=r_per_trade,
        min_stop_distance=min_stop_distance,
        max_leverage=max_leverage,
        maintenance_free_margin_pct=maintenance_free_margin_pct,
    )
