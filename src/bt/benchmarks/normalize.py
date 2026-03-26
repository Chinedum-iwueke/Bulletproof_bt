from __future__ import annotations

import math

import pandas as pd

from bt.benchmarks.types import (
    BenchmarkComparisonResult,
    BenchmarkComparisonUnavailable,
    NormalizedBenchmarkComparison,
)


def _normalize_points(values: pd.Series, *, anchor: float) -> pd.Series:
    return (values.astype("float64") / anchor) * 100.0


def normalize_aligned_series(
    aligned_result: BenchmarkComparisonResult,
) -> NormalizedBenchmarkComparison | BenchmarkComparisonUnavailable:
    if isinstance(aligned_result, BenchmarkComparisonUnavailable):
        return aligned_result

    strategy_anchor = float(aligned_result.strategy_points["strategy_value"].iloc[0])
    benchmark_anchor = float(aligned_result.benchmark_points["benchmark_value"].iloc[0])

    if not math.isfinite(strategy_anchor) or strategy_anchor <= 0:
        return BenchmarkComparisonUnavailable(
            available=False,
            reason="invalid_normalization_anchor",
            benchmark_id=aligned_result.benchmark_id,
            detail=f"invalid strategy normalization anchor: {strategy_anchor!r}",
        )
    if not math.isfinite(benchmark_anchor) or benchmark_anchor <= 0:
        return BenchmarkComparisonUnavailable(
            available=False,
            reason="invalid_normalization_anchor",
            benchmark_id=aligned_result.benchmark_id,
            detail=f"invalid benchmark normalization anchor: {benchmark_anchor!r}",
        )

    strategy_normalized = aligned_result.strategy_points.copy()
    benchmark_normalized = aligned_result.benchmark_points.copy()

    strategy_normalized["normalized"] = _normalize_points(
        strategy_normalized["strategy_value"],
        anchor=strategy_anchor,
    )
    benchmark_normalized["normalized"] = _normalize_points(
        benchmark_normalized["benchmark_value"],
        anchor=benchmark_anchor,
    )

    return NormalizedBenchmarkComparison(
        available=True,
        benchmark_id=aligned_result.benchmark_id,
        comparison_frequency=aligned_result.comparison_frequency,
        source_frequency=aligned_result.source_frequency,
        strategy_window_start=aligned_result.strategy_window_start,
        strategy_window_end=aligned_result.strategy_window_end,
        first_common_ts=aligned_result.first_common_ts,
        last_common_ts=aligned_result.last_common_ts,
        point_count=aligned_result.point_count,
        strategy_points=strategy_normalized,
        benchmark_points=benchmark_normalized,
    )
