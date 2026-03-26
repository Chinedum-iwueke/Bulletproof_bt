from __future__ import annotations

from bt.benchmarks.align import align_strategy_and_benchmark, prepare_strategy_daily_comparison_series
from bt.benchmarks.normalize import normalize_aligned_series
from bt.benchmarks.types import (
    BenchmarkComparisonMetrics,
    BenchmarkComparisonUnavailable,
    BenchmarkMetricResult,
    LoadedBenchmarkResult,
    NormalizedBenchmarkComparison,
)


def compute_benchmark_comparison_metrics(
    normalized_result: NormalizedBenchmarkComparison | BenchmarkComparisonUnavailable,
) -> BenchmarkMetricResult:
    if isinstance(normalized_result, BenchmarkComparisonUnavailable):
        return normalized_result

    if normalized_result.point_count <= 0:
        return BenchmarkComparisonUnavailable(
            available=False,
            reason="insufficient_aligned_points",
            benchmark_id=normalized_result.benchmark_id,
            detail="normalized comparison has zero points",
        )

    strategy_initial = float(normalized_result.strategy_points["normalized"].iloc[0])
    strategy_final = float(normalized_result.strategy_points["normalized"].iloc[-1])
    benchmark_initial = float(normalized_result.benchmark_points["normalized"].iloc[0])
    benchmark_final = float(normalized_result.benchmark_points["normalized"].iloc[-1])

    strategy_return = (strategy_final / strategy_initial) - 1.0
    benchmark_return = (benchmark_final / benchmark_initial) - 1.0
    excess_return_vs_benchmark = strategy_return - benchmark_return

    return BenchmarkComparisonMetrics(
        available=True,
        benchmark_id=normalized_result.benchmark_id,
        comparison_frequency=normalized_result.comparison_frequency,
        source_frequency=normalized_result.source_frequency,
        point_count=normalized_result.point_count,
        first_common_ts=normalized_result.first_common_ts,
        last_common_ts=normalized_result.last_common_ts,
        strategy_return=strategy_return,
        benchmark_return=benchmark_return,
        excess_return_vs_benchmark=excess_return_vs_benchmark,
    )


def build_benchmark_comparison(
    *,
    strategy_series,
    loaded_benchmark: LoadedBenchmarkResult,
    ts_column: str = "ts",
    value_column: str = "equity",
    min_points: int = 2,
) -> NormalizedBenchmarkComparison | BenchmarkComparisonUnavailable:
    strategy_daily = prepare_strategy_daily_comparison_series(
        strategy_series,
        ts_column=ts_column,
        value_column=value_column,
    )
    aligned = align_strategy_and_benchmark(
        strategy_daily=strategy_daily,
        loaded_benchmark=loaded_benchmark,
        min_points=min_points,
    )
    return normalize_aligned_series(aligned)
