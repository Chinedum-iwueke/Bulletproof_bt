from __future__ import annotations

from typing import Any

import pandas as pd

from bt.benchmarks.artifacts import emit_benchmark_comparison_artifact
from bt.benchmarks.config import BenchmarkConfigError, parse_benchmark_config
from bt.benchmarks.loader import BenchmarkDatasetError, load_benchmark_dataset
from bt.benchmarks.types import BenchmarkComparisonUnavailable


def _unavailable(reason: str, detail: str | None = None) -> dict[str, dict[str, Any]]:
    unavailable = BenchmarkComparisonUnavailable(
        available=False,
        reason=reason,
        benchmark_id=None,
        detail=detail,
    )
    return {
        "benchmark_comparison": {
            "available": False,
            "limited": True,
            "reason": unavailable.reason,
            "message": unavailable.detail or "Benchmark comparison is unavailable.",
            "summary_metrics": None,
            "metadata": {
                "benchmark_id": None,
                "benchmark_source": None,
                "library_revision": None,
                "benchmark_frequency": None,
                "comparison_frequency": None,
                "alignment_basis": "common_daily_timestamps_only",
                "normalization_basis": "100_at_first_common_timestamp",
                "comparison_window_start": None,
                "comparison_window_end": None,
                "point_count": 0,
            },
            "figure": None,
            "assumptions": [],
            "limitations": [unavailable.detail or "Benchmark comparison is unavailable."],
        }
    }


def build_benchmark_overview_payload(
    *,
    benchmark_config: dict[str, Any],
    strategy_series: pd.DataFrame,
    ts_column: str = "ts",
    value_column: str = "equity",
    min_points: int = 2,
) -> dict[str, dict[str, Any]]:
    try:
        parsed_config = parse_benchmark_config(benchmark_config)
    except BenchmarkConfigError as exc:
        return _unavailable("invalid_benchmark_config", str(exc))

    try:
        loaded_benchmark = load_benchmark_dataset(parsed_config)
    except BenchmarkDatasetError as exc:
        return _unavailable("benchmark_dataset_load_failed", str(exc))

    return emit_benchmark_comparison_artifact(
        strategy_series=strategy_series,
        loaded_benchmark=loaded_benchmark,
        ts_column=ts_column,
        value_column=value_column,
        min_points=min_points,
    )


def build_benchmark_overview_payload_from_metadata(
    *,
    run_metadata: dict[str, Any],
    strategy_series: pd.DataFrame,
    ts_column: str = "ts",
    value_column: str = "equity",
    min_points: int = 2,
) -> dict[str, dict[str, Any]]:
    benchmark_cfg = run_metadata.get("benchmark_config")
    if not isinstance(benchmark_cfg, dict):
        return _unavailable("benchmark_not_configured", "run metadata does not include benchmark_config")

    return build_benchmark_overview_payload(
        benchmark_config=benchmark_cfg,
        strategy_series=strategy_series,
        ts_column=ts_column,
        value_column=value_column,
        min_points=min_points,
    )
