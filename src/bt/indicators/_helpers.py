"""Internal reusable helper classes for streaming indicators."""
from __future__ import annotations

from collections import deque

from bt.indicators.base import safe_div


class WilderRMA:
    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.period = period
        self._values: deque[float] = deque(maxlen=period)
        self.value: float | None = None

    @property
    def is_ready(self) -> bool:
        return self.value is not None

    def reset(self) -> None:
        self._values.clear()
        self.value = None

    def update(self, x: float) -> float | None:
        if self.value is None:
            self._values.append(float(x))
            if len(self._values) == self.period:
                self.value = sum(self._values) / self.period
            return self.value
        self.value = ((self.value * (self.period - 1)) + x) / self.period
        return self.value


class StreamingEMA:
    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.period = period
        self.alpha = 2.0 / (period + 1)
        self._count = 0
        self._sum = 0.0
        self.value: float | None = None

    @property
    def is_ready(self) -> bool:
        return self._count >= self.period and self.value is not None

    def reset(self) -> None:
        self._count = 0
        self._sum = 0.0
        self.value = None

    def update(self, x: float) -> float | None:
        self._count += 1
        if self._count <= self.period:
            self._sum += x
            if self._count == self.period:
                self.value = self._sum / self.period
            return self.value
        if self.value is None:
            self.value = x
        else:
            self.value = (self.alpha * x) + ((1 - self.alpha) * self.value)
        return self.value


class RollingMinMax:
    def __init__(self, period: int) -> None:
        self.period = period
        self._vals: deque[float] = deque(maxlen=period)

    def reset(self) -> None:
        self._vals.clear()

    def update(self, value: float) -> None:
        self._vals.append(float(value))

    @property
    def is_ready(self) -> bool:
        return len(self._vals) == self.period

    @property
    def min(self) -> float | None:
        return min(self._vals) if self._vals else None

    @property
    def max(self) -> float | None:
        return max(self._vals) if self._vals else None


class DeltaTracker:
    def __init__(self) -> None:
        self.prev: float | None = None

    def reset(self) -> None:
        self.prev = None

    def update(self, x: float) -> float | None:
        if self.prev is None:
            self.prev = x
            return None
        delta = x - self.prev
        self.prev = x
        return delta


def typical_price(high: float, low: float, close: float) -> float:
    return (high + low + close) / 3.0


def money_flow_multiplier(high: float, low: float, close: float) -> float:
    return safe_div(((close - low) - (high - close)), high - low)
