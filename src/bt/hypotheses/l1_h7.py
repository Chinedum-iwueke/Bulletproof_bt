"""L1-H7 reusable flow and runner primitives."""
from __future__ import annotations

from collections import deque
from statistics import mean, pstdev
from typing import Deque

from bt.core.types import Bar


class RollingZScore:
    """Past-only rolling z-score estimator.

    Returns None until warmup is complete.
    """

    def __init__(self, lookback_bars: int) -> None:
        if lookback_bars <= 1:
            raise ValueError("lookback_bars must be > 1")
        self._history: Deque[float] = deque(maxlen=lookback_bars)

    def update(self, value: float | None) -> float | None:
        if value is None:
            return None
        window = tuple(self._history)
        if len(window) < self._history.maxlen:
            out = None
        else:
            mu = mean(window)
            sigma = pstdev(window)
            out = 0.0 if sigma <= 0 else float((float(value) - float(mu)) / float(sigma))
        self._history.append(float(value))
        return out


def imbalance_proxy_from_bar(bar: Bar) -> float | None:
    """Directional pressure proxy from OHLC bar shape in [-1, 1]."""
    rng = float(bar.high) - float(bar.low)
    if rng <= 0:
        return None
    raw = (float(bar.close) - float(bar.open)) / rng
    return float(max(-1.0, min(1.0, raw)))


def resolve_imbalance_threshold(name: str) -> float:
    key = str(name).lower()
    if key == "strict":
        return 0.35
    return 0.2


def flow_gate_passes(
    *,
    gate_mode: str,
    sigma_z: float | None,
    sigma_z_threshold: float,
    spread_proxy: float | None,
    spread_ref: float | None,
    imbalance: float | None,
    imbalance_min: float,
) -> bool:
    mode = str(gate_mode).lower()
    if mode == "off":
        return True
    if sigma_z is None or spread_proxy is None or spread_ref is None or imbalance is None:
        return False
    spread_mult = 1.25 if mode == "moderate" else 1.10
    sigma_cap = float(sigma_z_threshold) if mode == "moderate" else min(float(sigma_z_threshold), 0.5)
    return bool(abs(float(sigma_z)) <= sigma_cap and float(spread_proxy) <= float(spread_ref) * spread_mult and abs(float(imbalance)) >= float(imbalance_min))


__all__ = [
    "RollingZScore",
    "imbalance_proxy_from_bar",
    "resolve_imbalance_threshold",
    "flow_gate_passes",
]
