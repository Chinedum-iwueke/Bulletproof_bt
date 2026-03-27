"""L1-H4B hypothesis primitives (liquidity gate + capped size adjustment over L1-H2)."""
from __future__ import annotations

from collections import deque
from statistics import median
from typing import Deque

from bt.core.types import Bar
from bt.hypotheses.l1_h4a import RollingQuantileGate, bars_for_30_calendar_days


def spread_proxy_from_bar(bar: Bar) -> float | None:
    """Deterministic spread proxy: 0.5 * (high - low) / close."""
    if bar.close <= 0:
        return None
    return float(0.5 * (bar.high - bar.low) / bar.close)


class RollingMedianReference:
    """Past-only rolling median reference over a fixed-length window."""

    def __init__(self, lookback_bars: int) -> None:
        if lookback_bars <= 0:
            raise ValueError("lookback_bars must be > 0")
        self._history: Deque[float] = deque(maxlen=lookback_bars)

    def update(self, value: float | None) -> float | None:
        if value is None:
            return None
        reference = tuple(self._history)
        if len(reference) < self._history.maxlen:
            ref = None
        else:
            ref = float(median(reference))
        self._history.append(float(value))
        return ref


def capped_inverse_spread_ratio(
    *,
    spread_proxy_t: float | None,
    spread_proxy_ref: float | None,
    cap_multiplier: float,
) -> float | None:
    """size_factor_t = min(1.0, max(cap_multiplier, spread_proxy_ref / spread_proxy_t))."""
    if spread_proxy_t is None or spread_proxy_ref is None:
        return None
    if spread_proxy_t <= 0:
        return float(min(1.0, max(cap_multiplier, 1.0)))
    ratio = float(spread_proxy_ref) / float(spread_proxy_t)
    return float(min(1.0, max(float(cap_multiplier), ratio)))


__all__ = [
    "RollingQuantileGate",
    "RollingMedianReference",
    "bars_for_30_calendar_days",
    "spread_proxy_from_bar",
    "capped_inverse_spread_ratio",
]
