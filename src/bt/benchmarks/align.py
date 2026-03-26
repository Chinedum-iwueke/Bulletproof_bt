from __future__ import annotations

import pandas as pd

from bt.benchmarks.types import (
    AlignedBenchmarkComparison,
    BenchmarkComparisonResult,
    BenchmarkComparisonUnavailable,
    BenchmarkComparisonUnavailableReason,
    ComparisonFrequency,
    LoadedBenchmarkResult,
    StrategyComparisonSeries,
)


def _build_unavailable(
    *,
    reason: BenchmarkComparisonUnavailableReason,
    benchmark_id: str | None = None,
    detail: str | None = None,
) -> BenchmarkComparisonUnavailable:
    return BenchmarkComparisonUnavailable(
        available=False,
        reason=reason,
        benchmark_id=benchmark_id,
        detail=detail,
    )


def _coerce_numeric(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.replace([float("inf"), float("-inf")], pd.NA)


def prepare_strategy_daily_comparison_series(
    strategy_series: pd.DataFrame,
    *,
    ts_column: str = "ts",
    value_column: str = "equity",
    comparison_frequency: ComparisonFrequency = "1d",
) -> StrategyComparisonSeries | BenchmarkComparisonUnavailable:
    if comparison_frequency != "1d":
        return _build_unavailable(
            reason="unsupported_comparison_frequency",
            detail=f"comparison_frequency={comparison_frequency!r} is not supported in v1",
        )

    if strategy_series.empty:
        return _build_unavailable(reason="no_strategy_series")

    if ts_column not in strategy_series.columns or value_column not in strategy_series.columns:
        return _build_unavailable(
            reason="no_strategy_series",
            detail=f"expected columns ({ts_column!r}, {value_column!r}) not found",
        )

    frame = strategy_series.loc[:, [ts_column, value_column]].copy()
    frame.columns = ["ts", "value"]
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
    frame["value"] = _coerce_numeric(frame["value"])
    frame = frame.dropna(subset=["ts", "value"])

    if frame.empty:
        return _build_unavailable(reason="no_strategy_series", detail="strategy comparison series has no valid points")

    frame = frame.sort_values("ts", kind="mergesort")
    source_frequency: ComparisonFrequency = "1d"
    if (frame["ts"].dt.floor("D") != frame["ts"]).any() or frame["ts"].dt.floor("D").duplicated().any():
        source_frequency = "intraday"

    # v1 comparison contract: one daily strategy point per UTC date using the
    # last available strategy point in that day.
    frame["day"] = frame["ts"].dt.floor("D")
    frame = frame.groupby("day", as_index=False).tail(1)
    frame = frame.loc[:, ["day", "value"]].rename(columns={"day": "ts"}).reset_index(drop=True)

    if frame.empty:
        return _build_unavailable(reason="no_strategy_series", detail="daily strategy conversion yielded no points")

    return StrategyComparisonSeries(
        comparison_frequency=comparison_frequency,
        source_frequency=source_frequency,
        data=frame,
        window_start=frame["ts"].iloc[0],
        window_end=frame["ts"].iloc[-1],
        point_count=int(len(frame)),
    )


def align_strategy_and_benchmark(
    *,
    strategy_daily: StrategyComparisonSeries | BenchmarkComparisonUnavailable,
    loaded_benchmark: LoadedBenchmarkResult,
    min_points: int = 2,
) -> BenchmarkComparisonResult:
    if isinstance(strategy_daily, BenchmarkComparisonUnavailable):
        return strategy_daily

    if not loaded_benchmark.enabled:
        return _build_unavailable(reason="benchmark_disabled")

    benchmark_id = loaded_benchmark.config.id
    benchmark_data = loaded_benchmark.data.loc[:, ["ts", "close"]].copy()
    window_start = strategy_daily.window_start
    window_end = strategy_daily.window_end

    clipped_benchmark = benchmark_data[
        (benchmark_data["ts"] >= window_start) & (benchmark_data["ts"] <= window_end)
    ].copy()
    if clipped_benchmark.empty:
        return _build_unavailable(
            reason="no_benchmark_overlap",
            benchmark_id=benchmark_id,
            detail="benchmark has no points in strategy run window",
        )

    strategy_aligned = strategy_daily.data.merge(
        clipped_benchmark,
        on="ts",
        how="inner",
        validate="one_to_one",
    ).sort_values("ts", kind="mergesort")

    if strategy_aligned.empty:
        return _build_unavailable(
            reason="no_benchmark_overlap",
            benchmark_id=benchmark_id,
            detail="strategy and benchmark have no common daily timestamps",
        )

    if len(strategy_aligned) < min_points:
        return _build_unavailable(
            reason="insufficient_aligned_points",
            benchmark_id=benchmark_id,
            detail=f"aligned points={len(strategy_aligned)} < min_points={min_points}",
        )

    strategy_points = strategy_aligned.loc[:, ["ts", "value"]].rename(columns={"value": "strategy_value"})
    benchmark_points = strategy_aligned.loc[:, ["ts", "close"]].rename(columns={"close": "benchmark_value"})

    return AlignedBenchmarkComparison(
        available=True,
        benchmark_id=benchmark_id,
        comparison_frequency=strategy_daily.comparison_frequency,
        source_frequency=strategy_daily.source_frequency,
        strategy_window_start=window_start,
        strategy_window_end=window_end,
        first_common_ts=strategy_points["ts"].iloc[0],
        last_common_ts=strategy_points["ts"].iloc[-1],
        point_count=int(len(strategy_points)),
        strategy_points=strategy_points.reset_index(drop=True),
        benchmark_points=benchmark_points.reset_index(drop=True),
    )
