"""Intrabar assumptions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from bt.core.enums import Side


class BarLike(Protocol):
    high: float
    low: float

IntrabarMode = Literal["worst_case", "best_case", "midpoint"]
_VALID_INTRABAR_MODES: tuple[IntrabarMode, ...] = ("worst_case", "best_case", "midpoint")


@dataclass(frozen=True)
class IntrabarSpec:
    mode: IntrabarMode


def parse_intrabar_spec(config: dict[str, Any]) -> IntrabarSpec:
    execution_cfg = config.get("execution") if isinstance(config, dict) else None
    mode_raw = execution_cfg.get("intrabar_mode") if isinstance(execution_cfg, dict) else None
    if mode_raw is None:
        return IntrabarSpec(mode="worst_case")
    if mode_raw not in _VALID_INTRABAR_MODES:
        raise ValueError(
            "execution.intrabar_mode must be one of: worst_case, best_case, midpoint "
            f"(got: {mode_raw})"
        )
    return IntrabarSpec(mode=mode_raw)


def midpoint_fill_price(bar: BarLike) -> float:
    return (bar.high + bar.low) / 2.0


def market_fill_price(*, side: Side, bar: BarLike, intrabar_spec: IntrabarSpec) -> float:
    if intrabar_spec.mode == "worst_case":
        return worst_case_market_fill_price(side, bar)
    if intrabar_spec.mode == "best_case":
        return best_case_market_fill_price(side, bar)
    return midpoint_fill_price(bar)


def resolve_ambiguous_tp_sl(
    *,
    position_side: Side,
    take_profit: float,
    stop_loss: float,
    bar: BarLike,
    intrabar_spec: IntrabarSpec,
) -> tuple[str, float]:
    del position_side
    if intrabar_spec.mode == "worst_case":
        return "sl", stop_loss
    if intrabar_spec.mode == "best_case":
        return "tp", take_profit
    return "midpoint", midpoint_fill_price(bar)


def worst_case_market_fill_price(side: Side, bar: BarLike) -> float:
    if side == Side.BUY:
        return bar.high
    if side == Side.SELL:
        return bar.low
    raise ValueError(f"Unsupported side: {side}")


def best_case_market_fill_price(side: Side, bar: BarLike) -> float:
    if side == Side.BUY:
        return bar.low
    if side == Side.SELL:
        return bar.high
    raise ValueError(f"Unsupported side: {side}")
