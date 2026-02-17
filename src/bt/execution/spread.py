"""Spread modeling helpers for deterministic execution pricing."""
from __future__ import annotations

from typing import Literal


SpreadMode = Literal["none", "fixed_bps", "bar_range_proxy"]

_BAR_RANGE_PROXY_FACTOR = 0.5


def apply_spread(
    *,
    mode: SpreadMode,
    spread_bps: float,
    price: float,
    bar_high: float,
    bar_low: float,
    side: Literal["buy", "sell"],
) -> float:
    """Apply spread adjustment to a raw fill price.

    Contracts:
    - If mode=="none": return price unchanged.
    - If mode=="fixed_bps":
      - buy: price * (1 + spread_bps / 10_000)
      - sell: price * (1 - spread_bps / 10_000)
    - If mode=="bar_range_proxy":
      - proxy spread := (bar_high - bar_low) * 1/2
      - buy: price + proxy_spread/2
      - sell: price - proxy_spread/2
    """
    if price <= 0:
        raise ValueError("price must be > 0")
    if bar_high < bar_low:
        raise ValueError("bar_high must be >= bar_low")
    if side not in {"buy", "sell"}:
        raise ValueError(f"Unsupported side: {side}")

    if mode == "none":
        return price

    if mode == "fixed_bps":
        if spread_bps < 0:
            raise ValueError("spread_bps must be >= 0 for fixed_bps mode")
        spread_frac = spread_bps / 10_000.0
        adjusted = price * (1.0 + spread_frac) if side == "buy" else price * (1.0 - spread_frac)
    elif mode == "bar_range_proxy":
        if spread_bps < 0:
            raise ValueError("spread_bps must be >= 0")
        proxy_spread = (bar_high - bar_low) * _BAR_RANGE_PROXY_FACTOR
        half_spread = proxy_spread / 2.0
        adjusted = price + half_spread if side == "buy" else price - half_spread
    else:
        raise ValueError(f"Unsupported spread mode: {mode}")

    if adjusted <= 0:
        raise ValueError("spread-adjusted price must be > 0")
    return adjusted
