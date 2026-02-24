from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from bt.benchmark.buy_hold import EquityPoint, compute_buy_hold_equity
from bt.benchmark.spec import BenchmarkSpec
from bt.logging.formatting import FLOAT_DECIMALS_CSV


@dataclass
class BenchmarkTracker:
    spec: BenchmarkSpec
    _started: bool = False
    _found_any: bool = False
    _bars: list[object] | None = None
    _all_timestamps: list[datetime] | None = None
    _baseline_symbol: str | None = None

    def __post_init__(self) -> None:
        self._bars = []
        self._all_timestamps = []

    def on_tick(self, ts: datetime, bars_by_symbol: dict[str, object]) -> None:
        if not self.spec.enabled:
            return

        self._started = True
        self._all_timestamps.append(ts)

        if self.spec.mode == "flat":
            return

        benchmark_symbol = self.spec.symbol
        if self.spec.mode == "baseline_strategy" and benchmark_symbol is None:
            keys = sorted(str(k) for k in bars_by_symbol.keys())
            if not keys:
                return
            if self._baseline_symbol is None:
                self._baseline_symbol = keys[0]
            benchmark_symbol = self._baseline_symbol

        if benchmark_symbol is None:
            return

        bar = bars_by_symbol.get(benchmark_symbol)
        if bar is None:
            return

        self._found_any = True
        self._bars.append(bar)

    def _finalize_flat(self, *, initial_equity: float) -> list[EquityPoint]:
        points: list[EquityPoint] = []
        for ts in self._all_timestamps or []:
            points.append(EquityPoint(ts=ts, equity=float(initial_equity)))
        if not points:
            raise ValueError("benchmark.type=flat produced no timestamps from strategy run")
        return points

    def _finalize_baseline_strategy(self, *, initial_equity: float) -> list[EquityPoint]:
        if not self._found_any:
            raise ValueError("benchmark.type=baseline_strategy but no bars were observed in scoped data")
        if self.spec.baseline_strategy_name != "ma_cross":
            raise ValueError(
                "Unsupported benchmark.baseline_strategy.name: "
                f"{self.spec.baseline_strategy_name!r}. Supported names: ['ma_cross']"
            )

        params = self.spec.baseline_strategy_params or {}
        fast_raw = params.get("fast", 20)
        slow_raw = params.get("slow", 50)
        try:
            fast = int(fast_raw)
            slow = int(slow_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"benchmark.baseline_strategy.params.fast/slow must be positive ints (got: fast={fast_raw!r}, slow={slow_raw!r})"
            ) from exc
        if fast <= 0 or slow <= 0 or fast >= slow:
            raise ValueError(
                "benchmark.baseline_strategy.params require 0 < fast < slow. "
                f"Got fast={fast!r}, slow={slow!r}."
            )

        closes: list[float] = [float(getattr(bar, self.spec.price_field)) for bar in self._bars or []]
        timestamps: list[datetime] = [getattr(bar, "ts") for bar in self._bars or []]

        cash = float(initial_equity)
        units = 0.0
        points: list[EquityPoint] = []

        for idx, (ts, close) in enumerate(zip(timestamps, closes)):
            if idx + 1 >= slow:
                fast_ma = sum(closes[idx - fast + 1 : idx + 1]) / fast
                slow_ma = sum(closes[idx - slow + 1 : idx + 1]) / slow
                if units == 0.0 and fast_ma > slow_ma:
                    units = cash / close
                    cash = 0.0
                elif units > 0.0 and fast_ma < slow_ma:
                    cash = units * close
                    units = 0.0

            equity = cash + (units * close)
            points.append(EquityPoint(ts=ts, equity=equity))

        if not points:
            raise ValueError("benchmark.type=baseline_strategy produced no equity points")
        return points

    def finalize(self, *, initial_equity: float) -> list[EquityPoint]:
        if not self.spec.enabled:
            return []

        if self.spec.mode == "flat":
            return self._finalize_flat(initial_equity=initial_equity)

        if self.spec.mode == "baseline_strategy":
            return self._finalize_baseline_strategy(initial_equity=initial_equity)

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
            writer.writerow([point.ts.isoformat(), f"{point.equity:.{FLOAT_DECIMALS_CSV}f}"])


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
