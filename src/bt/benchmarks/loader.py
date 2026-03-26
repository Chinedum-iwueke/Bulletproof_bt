from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bt.benchmarks.store import BenchmarkStoreError, ensure_benchmark_dataset_accessible
from bt.benchmarks.types import (
    BenchmarkDatasetMetadata,
    BenchmarkId,
    BenchmarkConfig,
    DisabledBenchmarkDataset,
    LoadedBenchmarkDataset,
    LoadedBenchmarkResult,
)


class BenchmarkDatasetError(ValueError):
    """Raised when benchmark parquet loading or validation fails."""


@dataclass(frozen=True)
class BenchmarkDatasetValidation:
    row_count: int
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp


def validate_benchmark_dataset(
    df: pd.DataFrame,
    *,
    expected_benchmark_id: BenchmarkId,
) -> tuple[pd.DataFrame, BenchmarkDatasetValidation]:
    required_columns = ("ts", "symbol", "close")
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise BenchmarkDatasetError(
            f"Benchmark dataset missing required column(s): {', '.join(missing_columns)}"
        )

    data = df.loc[:, required_columns].copy()

    data["ts"] = pd.to_datetime(data["ts"], utc=True, errors="coerce")
    if data["ts"].isna().any():
        raise BenchmarkDatasetError("benchmark dataset column 'ts' contains unreadable timestamps")

    if data["symbol"].isna().any():
        raise BenchmarkDatasetError("benchmark dataset column 'symbol' contains null values")

    symbol_values = set(data["symbol"].astype(str).str.strip())
    if symbol_values != {expected_benchmark_id}:
        raise BenchmarkDatasetError(
            "benchmark dataset symbol mismatch: "
            f"expected only {expected_benchmark_id!r}, found {sorted(symbol_values)!r}"
        )

    close_series = pd.to_numeric(data["close"], errors="coerce")
    if close_series.isna().any():
        raise BenchmarkDatasetError("benchmark dataset column 'close' contains null/non-numeric values")
    if (close_series <= 0).any():
        raise BenchmarkDatasetError("benchmark dataset column 'close' must be > 0")
    if not close_series.map(math.isfinite).all():
        raise BenchmarkDatasetError("benchmark dataset column 'close' contains non-finite values")
    data["close"] = close_series.astype("float64")

    data = data.sort_values("ts", kind="mergesort").reset_index(drop=True)
    if data["ts"].duplicated().any():
        raise BenchmarkDatasetError("benchmark dataset contains duplicate timestamps")
    if data.empty:
        raise BenchmarkDatasetError("benchmark dataset is empty")
    if not data["ts"].is_monotonic_increasing:
        raise BenchmarkDatasetError("benchmark dataset timestamps are not monotonic after sorting")

    metadata = BenchmarkDatasetValidation(
        row_count=len(data),
        start_ts=data["ts"].iloc[0],
        end_ts=data["ts"].iloc[-1],
    )
    return data, metadata


def _read_benchmark_parquet(dataset_path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(dataset_path)
    except Exception as exc:  # pragma: no cover - pandas backend exceptions vary
        raise BenchmarkDatasetError(f"Unable to read benchmark parquet file: {dataset_path}") from exc


def load_benchmark_dataset(config: BenchmarkConfig) -> LoadedBenchmarkResult:
    if not config.enabled:
        return DisabledBenchmarkDataset(enabled=False, mode="none")

    try:
        dataset_path = ensure_benchmark_dataset_accessible(config)
    except BenchmarkStoreError as exc:
        raise BenchmarkDatasetError(str(exc)) from exc

    raw_df = _read_benchmark_parquet(dataset_path)
    validated_df, validation = validate_benchmark_dataset(raw_df, expected_benchmark_id=config.id)

    metadata = BenchmarkDatasetMetadata(
        benchmark_id=config.id,
        dataset_path=dataset_path,
        row_count=validation.row_count,
        start_ts=validation.start_ts,
        end_ts=validation.end_ts,
    )
    return LoadedBenchmarkDataset(
        enabled=True,
        config=config,
        data=validated_df,
        metadata=metadata,
    )
