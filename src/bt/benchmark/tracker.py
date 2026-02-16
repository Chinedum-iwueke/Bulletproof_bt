from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from bt.benchmark.buy_hold import EquityPoint, compute_buy_hold_equity
from bt.benchmark.spec import BenchmarkSpec


@dataclass
class BenchmarkTracker:
    spec: BenchmarkSpec
    _started: bool = False
    _found_any: bool = False
    _bars: list[object] | None = None

    def __post_init__(self) -> None:
        self._bars = []

    def on_tick(self, ts: datetime, bars_by_symbol: dict[str, object]) -> None:
        """
        Called once per engine tick (outside engine loop via existing orchestration point).
        If benchmark symbol is present in bars_by_symbol at this ts, append that bar to internal list.
        Must not synthesize or resample; only store what appears.
        """
        if not self.spec.enabled:
            return

        self._started = True
        benchmark_symbol = self.spec.symbol
        if benchmark_symbol is None:
            return

        bar = bars_by_symbol.get(benchmark_symbol)
        if bar is None:
            return

        self._found_any = True
        self._bars.append(bar)

    def finalize(self, *, initial_equity: float) -> list[EquityPoint]:
        """
        Compute buy&hold curve using stored bars.
        Raise ValueError if enabled but no bars were observed.
        """
        if not self.spec.enabled:
            return []
        if not self._found_any:
            raise ValueError(f"benchmark.enabled=true but no bars found for symbol={self.spec.symbol}")

        return compute_buy_hold_equity(
            bars=self._bars,
            initial_equity=initial_equity,
            price_field=self.spec.price_field,
        )


def write_benchmark_equity_csv(points: list[EquityPoint], path: Path) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ts", "equity"])
        for point in points:
            writer.writerow([point.ts.isoformat(), point.equity])



class BenchmarkTrackingFeed:
    def __init__(self, *, inner_feed: Any, tracker: BenchmarkTracker) -> None:
        self._inner_feed = inner_feed
        self._tracker = tracker

    def symbols(self) -> list[str]:
        symbols = getattr(self._inner_feed, "symbols", None)
        if callable(symbols):
            return list(symbols())
        return []

    def reset(self) -> None:
        reset = getattr(self._inner_feed, "reset", None)
        if callable(reset):
            reset()

    def next(self):
        bars = self._inner_feed.next()
        if bars is None:
            return None

        if isinstance(bars, dict):
            bars_by_symbol = bars
            bars_list = list(bars.values())
        else:
            bars_list = list(bars)
            bars_by_symbol = {bar.symbol: bar for bar in bars_list}

        if bars_list:
            self._tracker.on_tick(bars_list[0].ts, bars_by_symbol)

        return bars
