"""Resolve and validate runtime configuration into a single canonical shape."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import copy

from bt.execution.intrabar import parse_intrabar_spec


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


def _resolve_r_per_trade_alias(resolved: dict[str, Any]) -> None:
    """Normalize legacy risk aliases to canonical ``risk.r_per_trade``.

    ``risk.risk_per_trade_pct`` and top-level ``risk_per_trade_pct`` are treated
    as input aliases only. They are never injected by default.
    """
    risk_cfg = _ensure_mapping(resolved.get("risk"), name="risk")
    canonical_present = "r_per_trade" in risk_cfg
    nested_legacy_present = "risk_per_trade_pct" in risk_cfg
    top_legacy_present = "risk_per_trade_pct" in resolved

    canonical_value = risk_cfg.get("r_per_trade")
    nested_legacy_value = risk_cfg.get("risk_per_trade_pct")
    top_legacy_value = resolved.get("risk_per_trade_pct")

    if canonical_present and nested_legacy_present and canonical_value != nested_legacy_value:
        raise ConfigError(
            "Conflicting config values for 'risk.r_per_trade' "
            f"({canonical_value!r}) and 'risk.risk_per_trade_pct' ({nested_legacy_value!r})."
        )

    if canonical_present and top_legacy_present and canonical_value != top_legacy_value:
        raise ConfigError(
            "Conflicting config values for 'risk.r_per_trade' "
            f"({canonical_value!r}) and 'risk_per_trade_pct' ({top_legacy_value!r})."
        )

    if nested_legacy_present and top_legacy_present and nested_legacy_value != top_legacy_value:
        raise ConfigError(
            "Conflicting config values for 'risk.risk_per_trade_pct' "
            f"({nested_legacy_value!r}) and 'risk_per_trade_pct' ({top_legacy_value!r})."
        )

    if not canonical_present:
        if nested_legacy_present:
            risk_cfg["r_per_trade"] = nested_legacy_value
        elif top_legacy_present:
            risk_cfg["r_per_trade"] = top_legacy_value

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

    resolved.setdefault("signal_delay_bars", 1)
    resolved.setdefault("initial_cash", 100000.0)
    resolved.setdefault("model", "fixed_bps")
    resolved.setdefault("fixed_bps", 5.0)

    outputs_cfg = _ensure_mapping(resolved.get("outputs"), name="outputs")
    outputs_cfg.setdefault("root_dir", "outputs/runs")
    outputs_cfg.setdefault("jsonl", True)
    resolved["outputs"] = outputs_cfg

    data_cfg = _ensure_mapping(resolved.get("data"), name="data")
    data_cfg.setdefault("mode", "streaming")
    data_cfg.setdefault("symbols_subset", None)
    data_cfg.setdefault("chunksize", 50000)
    resolved["data"] = data_cfg

    strategy_cfg = _ensure_mapping(resolved.get("strategy"), name="strategy")
    strategy_cfg.setdefault("name", "coinflip")
    resolved["strategy"] = strategy_cfg

    execution_cfg = _ensure_mapping(resolved.get("execution"), name="execution")
    execution_cfg.setdefault("spread_mode", "none")
    execution_cfg.setdefault("spread_bps", 0.0)

    resolved["execution"] = execution_cfg
    intrabar_spec = parse_intrabar_spec(resolved)
    execution_cfg["intrabar_mode"] = intrabar_spec.mode

    spread_mode = execution_cfg.get("spread_mode")
    if spread_mode not in {"none", "fixed_bps", "bar_range_proxy"}:
        raise ConfigError(
            "Invalid execution.spread_mode: expected one of "
            "{'none', 'fixed_bps', 'bar_range_proxy'} "
            f"got {spread_mode!r}"
        )

    if spread_mode == "fixed_bps":
        if "spread_bps" not in execution_cfg:
            raise ConfigError("execution.spread_bps is required when execution.spread_mode='fixed_bps'")
        try:
            spread_bps = float(execution_cfg["spread_bps"])
        except (TypeError, ValueError) as exc:
            raise ConfigError("Invalid execution.spread_bps: expected float >= 0") from exc
        if spread_bps < 0:
            raise ConfigError("Invalid execution.spread_bps: expected float >= 0")
        execution_cfg["spread_bps"] = spread_bps

    if spread_mode == "none":
        execution_cfg["spread_bps"] = float(execution_cfg.get("spread_bps", 0.0))

    if spread_mode == "bar_range_proxy":
        execution_cfg["spread_bps"] = float(execution_cfg.get("spread_bps", 0.0))

    resolved["execution"] = execution_cfg

    if "htf_timeframes" in resolved or "htf_strict" in resolved:
        htf_resampler_cfg = _ensure_mapping(resolved.get("htf_resampler"), name="htf_resampler")
        if "htf_timeframes" in resolved:
            htf_resampler_cfg.setdefault("timeframes", resolved.get("htf_timeframes"))
        if "htf_strict" in resolved:
            htf_resampler_cfg.setdefault("strict", resolved.get("htf_strict"))
        htf_resampler_cfg.setdefault("strict", True)
        resolved["htf_resampler"] = htf_resampler_cfg

    _resolve_risk_value(
        resolved=resolved,
        top_key="max_positions",
        nested_key="max_positions",
        default=1,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="max_leverage",
        nested_key="max_leverage",
        default=2.0,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="stop_resolution",
        nested_key="stop_resolution",
        default="strict",
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="slippage_k",
        nested_key="slippage_k_proxy",
        default=0.0,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="margin_buffer_tier",
        nested_key="margin_buffer_tier",
        default=1,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="min_stop_distance_pct",
        nested_key="min_stop_distance_pct",
        default=0.001,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="max_notional_pct_equity",
        nested_key="max_notional_pct_equity",
        default=1.0,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="maintenance_free_margin_pct",
        nested_key="maintenance_free_margin_pct",
        default=0.01,
    )
    _resolve_r_per_trade_alias(resolved)

    risk_cfg = resolved.get("risk", {})
    risk_cfg.setdefault("mode", "equity_pct")
    stop_resolution = risk_cfg.get("stop_resolution")
    if stop_resolution not in {"strict", "allow_legacy_proxy"}:
        raise ConfigError(
            "Invalid risk.stop_resolution: expected 'strict' or 'allow_legacy_proxy' "
            f"got {stop_resolution!r}"
        )

    try:
        margin_buffer_tier = int(risk_cfg.get("margin_buffer_tier"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.margin_buffer_tier: expected one of {1, 2, 3}; "
            f"got {risk_cfg.get('margin_buffer_tier')!r}."
        ) from exc
    if margin_buffer_tier not in {1, 2, 3}:
        raise ConfigError(
            "Invalid risk.margin_buffer_tier: expected one of {1, 2, 3} "
            f"got {margin_buffer_tier!r}. "
            "Set risk.margin_buffer_tier explicitly to 1 (no proxy buffer), 2, or 3."
        )
    risk_cfg["margin_buffer_tier"] = margin_buffer_tier

    try:
        slippage_k_proxy = float(risk_cfg.get("slippage_k_proxy"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.slippage_k_proxy: expected a value in [0.0, 0.05]; "
            f"got {risk_cfg.get('slippage_k_proxy')!r}."
        ) from exc
    if not (0.0 <= slippage_k_proxy <= 0.05):
        raise ConfigError(
            "Invalid risk.slippage_k_proxy: expected a value in [0.0, 0.05] "
            f"got {slippage_k_proxy!r}. "
            "Use 0.0 to disable the proxy buffer or a small fraction like 0.001."
        )
    risk_cfg["slippage_k_proxy"] = slippage_k_proxy

    try:
        min_stop_distance_pct = float(risk_cfg.get("min_stop_distance_pct"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.min_stop_distance_pct: expected a float in [0.0, 0.05]; "
            f"got {risk_cfg.get('min_stop_distance_pct')!r}."
        ) from exc
    if not (0.0 <= min_stop_distance_pct <= 0.05):
        raise ConfigError(
            "Invalid risk.min_stop_distance_pct: expected a float in [0.0, 0.05] "
            f"got {min_stop_distance_pct!r}. "
            "Use 0.0 to disable this guardrail or a small fraction like 0.001 (0.1%)."
        )
    risk_cfg["min_stop_distance_pct"] = min_stop_distance_pct

    try:
        max_notional_pct_equity = float(risk_cfg.get("max_notional_pct_equity"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.max_notional_pct_equity: expected a float in (0.0, 5.0]; "
            f"got {risk_cfg.get('max_notional_pct_equity')!r}."
        ) from exc
    if not (0.0 < max_notional_pct_equity <= 5.0):
        raise ConfigError(
            "Invalid risk.max_notional_pct_equity: expected a float in (0.0, 5.0] "
            f"got {max_notional_pct_equity!r}. "
            "Set it to 1.0 for a 100% of equity cap or increase up to 5.0 when needed."
        )
    risk_cfg["max_notional_pct_equity"] = max_notional_pct_equity

    try:
        maintenance_free_margin_pct = float(risk_cfg.get("maintenance_free_margin_pct"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.maintenance_free_margin_pct: expected a float in [0.0, 0.20]; "
            f"got {risk_cfg.get('maintenance_free_margin_pct')!r}."
        ) from exc
    if not (0.0 <= maintenance_free_margin_pct <= 0.20):
        raise ConfigError(
            "Invalid risk.maintenance_free_margin_pct: expected a float in [0.0, 0.20] "
            f"got {maintenance_free_margin_pct!r}. "
            "Set it to 0.01 for a 1% maintenance free-margin floor."
        )
    risk_cfg["maintenance_free_margin_pct"] = maintenance_free_margin_pct

    resolved["risk"] = risk_cfg

    return resolved
