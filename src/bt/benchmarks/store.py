from __future__ import annotations

from pathlib import Path

from bt.benchmarks.config import BenchmarkConfigError
from bt.benchmarks.types import EnabledBenchmarkConfig


class BenchmarkStoreError(ValueError):
    """Raised when benchmark dataset path resolution or access checks fail."""


def resolve_benchmark_dataset_path(config: EnabledBenchmarkConfig) -> Path:
    if config.source != "platform_managed":
        raise BenchmarkStoreError(
            f"Unsupported benchmark source for store resolution: {config.source!r}"
        )
    if config.frequency != "1d":
        raise BenchmarkStoreError(
            f"Unsupported benchmark frequency for store resolution: {config.frequency!r}"
        )
    return config.library_root / config.id / "daily.parquet"


def ensure_benchmark_dataset_accessible(config: EnabledBenchmarkConfig) -> Path:
    dataset_path = resolve_benchmark_dataset_path(config)
    if not dataset_path.exists():
        raise BenchmarkStoreError(
            f"Benchmark dataset file not found: {dataset_path} "
            f"(benchmark.id={config.id}, library_revision={config.library_revision})"
        )
    if not dataset_path.is_file():
        raise BenchmarkStoreError(
            f"Benchmark dataset path is not a file: {dataset_path}"
        )
    try:
        with dataset_path.open("rb"):
            pass
    except OSError as exc:
        raise BenchmarkStoreError(
            f"Benchmark dataset file is not readable: {dataset_path}"
        ) from exc
    return dataset_path


def parse_and_resolve_dataset_path(raw: dict | None) -> Path:
    """Helper seam for callers that just need parse+path resolution in one step."""
    from bt.benchmarks.config import parse_benchmark_config

    config = parse_benchmark_config(raw)
    if not config.enabled:
        raise BenchmarkConfigError("Cannot resolve benchmark dataset path when benchmark is disabled")
    return resolve_benchmark_dataset_path(config)
