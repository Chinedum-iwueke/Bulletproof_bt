from __future__ import annotations

import pandas as pd

from bt.benchmarks.types import BenchmarkComparisonFigure, BenchmarkComparisonFigureSeries, NormalizedBenchmarkComparison


def _format_ts(ts: pd.Timestamp) -> str:
    utc_ts = ts.tz_convert("UTC") if ts.tzinfo is not None else ts.tz_localize("UTC")
    return utc_ts.isoformat().replace("+00:00", "Z")


def _series_points(frame: pd.DataFrame, *, value_column: str) -> list[tuple[str, float]]:
    return [
        (_format_ts(ts), float(value))
        for ts, value in zip(frame["ts"], frame[value_column], strict=True)
    ]


def build_benchmark_comparison_figure(
    normalized_result: NormalizedBenchmarkComparison,
) -> BenchmarkComparisonFigure:
    strategy = BenchmarkComparisonFigureSeries(
        id="strategy",
        label="Strategy",
        points=_series_points(normalized_result.strategy_points, value_column="normalized"),
    )
    benchmark = BenchmarkComparisonFigureSeries(
        id=f"benchmark_{normalized_result.benchmark_id}",
        label=normalized_result.benchmark_id,
        points=_series_points(normalized_result.benchmark_points, value_column="normalized"),
    )
    return BenchmarkComparisonFigure(
        type="timeseries_overlay",
        title="Strategy vs Benchmark",
        series=[strategy, benchmark],
    )
