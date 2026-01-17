"""Intrabar assumptions."""
from __future__ import annotations

from bt.core.enums import Side
from bt.core.types import Bar


def worst_case_market_fill_price(side: Side, bar: Bar) -> float:
    if side == Side.BUY:
        return bar.high
    if side == Side.SELL:
        return bar.low
    raise ValueError(f"Unsupported side: {side}")


def best_case_market_fill_price(side: Side, bar: Bar) -> float:
    if side == Side.BUY:
        return bar.low
    if side == Side.SELL:
        return bar.high
    raise ValueError(f"Unsupported side: {side}")


def randomized_market_fill_price(side: Side, bar: Bar) -> float:
    """Placeholder for randomized intrabar fill modeling."""
    raise NotImplementedError("TODO: implement randomized intrabar fill pricing.")
