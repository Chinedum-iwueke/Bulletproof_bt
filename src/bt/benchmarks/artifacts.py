from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from bt.benchmarks.figures import build_benchmark_comparison_figure
from bt.benchmarks.metrics import build_benchmark_comparison, compute_benchmark_comparison_metrics
from bt.benchmarks.types import (
    BenchmarkComparisonMetadata,
    BenchmarkComparisonOverviewAvailable,
    BenchmarkComparisonOverviewPayload,
    BenchmarkComparisonOverviewUnavailable,
    BenchmarkComparisonSummaryMetrics,
    BenchmarkComparisonUnavailable,
    BenchmarkComparisonUnavailableReason,
    LoadedBenchmarkResult,
)


def _format_ts(ts: pd.Timestamp | None) -> str | None:
    if ts is None:
        return None
    utc_ts = ts.tz_convert("UTC") if ts.tzinfo is not None else ts.tz_localize("UTC")
    return utc_ts.isoformat().replace("+00:00", "Z")


def _assumptions() -> list[str]:
    return [
        "Benchmark comparison uses platform-managed daily close data.",
        "Strategy equity is converted to daily comparison points using the last available point per UTC day.",
        "Benchmark and strategy are aligned on common daily timestamps only.",
        "Strategy and benchmark curves are normalized to 100 at the first common timestamp.",
    ]


def _unavailable_message(reason: BenchmarkComparisonUnavailableReason) -> str:
    return {
        "benchmark_disabled": "Benchmark comparison is disabled by configuration.",
        "benchmark_not_configured": "Benchmark configuration is not available in this analysis context.",
        "benchmark_dataset_load_failed": "Benchmark dataset could not be loaded from managed storage.",
        "invalid_benchmark_config": "Benchmark configuration could not be parsed.",
        "no_strategy_series": "No valid strategy equity/performance series is available for comparison.",
        "no_benchmark_overlap": "No common daily timestamps are shared by strategy and benchmark.",
        "insufficient_aligned_points": "Aligned benchmark comparison does not have enough points.",
        "invalid_normalization_anchor": "Benchmark comparison normalization anchor is invalid.",
        "unsupported_comparison_frequency": "Configured benchmark comparison frequency is not supported in v1.",
    }.get(reason, "Benchmark comparison is unavailable.")


def _metadata_from_loaded_benchmark(
    loaded_benchmark: LoadedBenchmarkResult,
    *,
    comparison_window_start: pd.Timestamp | None,
    comparison_window_end: pd.Timestamp | None,
    point_count: int,
) -> BenchmarkComparisonMetadata:
    if loaded_benchmark.enabled:
        return BenchmarkComparisonMetadata(
            benchmark_id=loaded_benchmark.config.id,
            benchmark_source=loaded_benchmark.config.source,
            library_revision=loaded_benchmark.config.library_revision,
            benchmark_frequency=loaded_benchmark.config.frequency,
            comparison_frequency=loaded_benchmark.config.comparison_frequency,
            alignment_basis="common_daily_timestamps_only",
            normalization_basis="100_at_first_common_timestamp",
            comparison_window_start=_format_ts(comparison_window_start),
            comparison_window_end=_format_ts(comparison_window_end),
            point_count=point_count,
        )

    return BenchmarkComparisonMetadata(
        benchmark_id=None,
        benchmark_source=None,
        library_revision=None,
        benchmark_frequency=None,
        comparison_frequency=None,
        alignment_basis="common_daily_timestamps_only",
        normalization_basis="100_at_first_common_timestamp",
        comparison_window_start=_format_ts(comparison_window_start),
        comparison_window_end=_format_ts(comparison_window_end),
        point_count=point_count,
    )


def _unavailable_payload(
    unavailable: BenchmarkComparisonUnavailable,
    *,
    loaded_benchmark: LoadedBenchmarkResult,
) -> BenchmarkComparisonOverviewUnavailable:
    return BenchmarkComparisonOverviewUnavailable(
        available=False,
        limited=True,
        reason=unavailable.reason,
        message=unavailable.detail or _unavailable_message(unavailable.reason),
        summary_metrics=None,
        metadata=_metadata_from_loaded_benchmark(
            loaded_benchmark,
            comparison_window_start=None,
            comparison_window_end=None,
            point_count=0,
        ),
        figure=None,
        assumptions=_assumptions(),
        limitations=[_unavailable_message(unavailable.reason)],
    )


def emit_benchmark_comparison_artifact(
    *,
    strategy_series: pd.DataFrame,
    loaded_benchmark: LoadedBenchmarkResult,
    ts_column: str = "ts",
    value_column: str = "equity",
    min_points: int = 2,
) -> dict[str, dict]:
    normalized = build_benchmark_comparison(
        strategy_series=strategy_series,
        loaded_benchmark=loaded_benchmark,
        ts_column=ts_column,
        value_column=value_column,
        min_points=min_points,
    )
    metrics = compute_benchmark_comparison_metrics(normalized)

    if isinstance(normalized, BenchmarkComparisonUnavailable):
        payload: BenchmarkComparisonOverviewPayload = _unavailable_payload(
            normalized,
            loaded_benchmark=loaded_benchmark,
        )
        return {"benchmark_comparison": asdict(payload)}

    if isinstance(metrics, BenchmarkComparisonUnavailable):
        payload = _unavailable_payload(metrics, loaded_benchmark=loaded_benchmark)
        return {"benchmark_comparison": asdict(payload)}

    payload = BenchmarkComparisonOverviewAvailable(
        available=True,
        limited=False,
        summary_metrics=BenchmarkComparisonSummaryMetrics(
            benchmark_selected=metrics.benchmark_id,
            strategy_return=metrics.strategy_return,
            benchmark_return=metrics.benchmark_return,
            excess_return_vs_benchmark=metrics.excess_return_vs_benchmark,
        ),
        metadata=_metadata_from_loaded_benchmark(
            loaded_benchmark,
            comparison_window_start=normalized.first_common_ts,
            comparison_window_end=normalized.last_common_ts,
            point_count=normalized.point_count,
        ),
        figure=build_benchmark_comparison_figure(normalized),
        assumptions=_assumptions(),
        limitations=[],
    )
    return {"benchmark_comparison": asdict(payload)}
