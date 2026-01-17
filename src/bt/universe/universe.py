"""Universe selection engine for streaming bars."""
from __future__ import annotations

from bisect import bisect_right
from collections import deque
from typing import Deque

import pandas as pd

from bt.core.types import Bar


class UniverseEngine:
    """Maintain a tradeable universe based on lagged rolling volume."""

    def __init__(
        self,
        *,
        min_history_bars: int = 60,
        lookback_bars: int = 60,
        min_avg_volume: float = 0.0,
        lag_bars: int = 1,
    ) -> None:
        if min_history_bars <= 0:
            raise ValueError("min_history_bars must be positive")
        if lookback_bars <= 0:
            raise ValueError("lookback_bars must be positive")
        if min_avg_volume < 0:
            raise ValueError("min_avg_volume must be >= 0")
        if lag_bars < 0:
            raise ValueError("lag_bars must be >= 0")

        self._min_history_bars = min_history_bars
        self._lookback_bars = lookback_bars
        self._min_avg_volume = min_avg_volume
        self._lag_bars = lag_bars

        self._volumes: dict[str, Deque[float]] = {}
        self._volume_sums: dict[str, float] = {}
        self._bar_counts: dict[str, int] = {}
        self._pending: dict[str, Deque[bool]] = {}
        self._decision_timestamps: dict[str, list[pd.Timestamp]] = {}
        self._decision_values: dict[str, list[bool]] = {}
        self._latest_effective_ts: pd.Timestamp | None = None

    def update(self, bar: Bar) -> None:
        """Consume one bar, update internal rolling stats."""
        if bar.ts.tz is None:
            raise ValueError("bar timestamp must be timezone-aware UTC")
        if str(bar.ts.tz) != "UTC":
            raise ValueError("bar timestamp must be in UTC")

        symbol = bar.symbol
        self._bar_counts[symbol] = self._bar_counts.get(symbol, 0) + 1

        window = self._volumes.setdefault(symbol, deque())
        window.append(bar.volume)
        self._volume_sums[symbol] = self._volume_sums.get(symbol, 0.0) + bar.volume

        if len(window) > self._lookback_bars:
            removed = window.popleft()
            self._volume_sums[symbol] -= removed

        if (
            self._bar_counts[symbol] < self._min_history_bars
            or len(window) < self._lookback_bars
        ):
            return

        avg_volume = self._volume_sums[symbol] / len(window)
        is_tradeable = avg_volume >= self._min_avg_volume

        pending = self._pending.setdefault(symbol, deque())
        pending.append(is_tradeable)

        if self._lag_bars == 0:
            self._publish_decision(symbol, bar.ts, pending.popleft())
            return

        if len(pending) > self._lag_bars:
            decision = pending.popleft()
            self._publish_decision(symbol, bar.ts, decision)

    def tradeable_at(self, ts: pd.Timestamp) -> set[str]:
        """Return tradeable symbols at timestamp ts based on lagged info."""
        if ts.tz is None:
            raise ValueError("ts must be timezone-aware UTC")
        if str(ts.tz) != "UTC":
            raise ValueError("ts must be in UTC")

        tradeable: set[str] = set()
        for symbol, timestamps in self._decision_timestamps.items():
            idx = bisect_right(timestamps, ts) - 1
            if idx >= 0 and self._decision_values[symbol][idx]:
                tradeable.add(symbol)
        return tradeable

    def symbols_seen(self) -> set[str]:
        return set(self._bar_counts)

    def latest_tradeable(self) -> set[str]:
        if self._latest_effective_ts is None:
            return set()
        return self.tradeable_at(self._latest_effective_ts)

    def _publish_decision(self, symbol: str, ts: pd.Timestamp, is_tradeable: bool) -> None:
        timestamps = self._decision_timestamps.setdefault(symbol, [])
        values = self._decision_values.setdefault(symbol, [])
        if timestamps and ts < timestamps[-1]:
            raise ValueError("decision timestamps must be non-decreasing")
        timestamps.append(ts)
        values.append(is_tradeable)
        if self._latest_effective_ts is None or ts > self._latest_effective_ts:
            self._latest_effective_ts = ts
