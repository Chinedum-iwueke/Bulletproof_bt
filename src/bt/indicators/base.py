"""Streaming indicator interfaces and shared rolling utilities."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from math import isfinite, sqrt
from typing import Protocol

from bt.core.types import Bar

EPSILON = 1e-12


class Indicator(Protocol):
    """Protocol for streaming indicators."""

    name: str
    warmup_bars: int

    def update(self, bar: Bar) -> None:
        ...

    def reset(self) -> None:
        ...

    @property
    def is_ready(self) -> bool:
        ...

    @property
    def value(self) -> float | None:
        ...


@dataclass
class IndicatorState:
    value: float | None
    is_ready: bool


class BaseIndicator(ABC):
    """Base class for stateful bar-by-bar indicators."""

    name: str
    warmup_bars: int

    def __init__(self, name: str, warmup_bars: int) -> None:
        self.name = name
        self.warmup_bars = max(0, warmup_bars)
        self._bars_seen = 0

    @abstractmethod
    def update(self, bar: Bar) -> None:
        ...

    @abstractmethod
    def reset(self) -> None:
        ...

    @property
    def is_ready(self) -> bool:
        return self._bars_seen >= self.warmup_bars and self.value is not None

    @property
    @abstractmethod
    def value(self) -> float | None:
        ...


class MultiValueIndicator(BaseIndicator):
    """Base class for multi-output indicators."""

    def __init__(self, name: str, warmup_bars: int, primary_key: str) -> None:
        super().__init__(name=name, warmup_bars=warmup_bars)
        self._primary_key = primary_key
        self._values: dict[str, float | None] = {}

    @property
    def values(self) -> dict[str, float | None]:
        return dict(self._values)

    def get(self, key: str) -> float | None:
        return self._values.get(key)

    @property
    def value(self) -> float | None:
        return self._values.get(self._primary_key)


class RollingWindow:
    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.period = period
        self._values: deque[float] = deque(maxlen=period)

    def append(self, value: float) -> None:
        self._values.append(float(value))

    def clear(self) -> None:
        self._values.clear()

    @property
    def full(self) -> bool:
        return len(self._values) == self.period

    @property
    def values(self) -> tuple[float, ...]:
        return tuple(self._values)

    def __len__(self) -> int:
        return len(self._values)


class RollingSum:
    def __init__(self, period: int) -> None:
        self.period = period
        self._values: deque[float] = deque(maxlen=period)
        self._sum = 0.0

    def update(self, value: float) -> float:
        if len(self._values) == self.period:
            self._sum -= self._values[0]
        self._values.append(float(value))
        self._sum += float(value)
        return self._sum

    def reset(self) -> None:
        self._values.clear()
        self._sum = 0.0

    @property
    def is_full(self) -> bool:
        return len(self._values) == self.period

    @property
    def value(self) -> float:
        return self._sum


class RollingMean(RollingSum):
    @property
    def mean(self) -> float | None:
        if not self.is_full:
            return None
        return self._sum / self.period


class RollingStd:
    def __init__(self, period: int) -> None:
        if period <= 1:
            raise ValueError("period must be > 1")
        self.period = period
        self._values: deque[float] = deque(maxlen=period)
        self._sum = 0.0
        self._sum_sq = 0.0

    def update(self, value: float) -> float | None:
        v = float(value)
        if len(self._values) == self.period:
            old = self._values[0]
            self._sum -= old
            self._sum_sq -= old * old
        self._values.append(v)
        self._sum += v
        self._sum_sq += v * v
        return self.std

    def reset(self) -> None:
        self._values.clear()
        self._sum = 0.0
        self._sum_sq = 0.0

    @property
    def is_full(self) -> bool:
        return len(self._values) == self.period

    @property
    def std(self) -> float | None:
        if not self.is_full:
            return None
        mean = self._sum / self.period
        var = max(0.0, (self._sum_sq / self.period) - (mean * mean))
        return sqrt(var)


def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(denominator) <= EPSILON:
        return default
    return numerator / denominator


def clamp(value: float, minimum: float, maximum: float) -> float:
    if not isfinite(value):
        return minimum
    return max(minimum, min(value, maximum))
