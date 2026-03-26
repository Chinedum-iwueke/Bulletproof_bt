from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias

import pandas as pd

BenchmarkId = Literal["BTC", "SPY", "XAUUSD", "DXY"]
BenchmarkModeEnabled = Literal["auto", "manual"]
BenchmarkMode = Literal["auto", "manual", "none"]
BenchmarkSource = Literal["platform_managed"]
BenchmarkFrequency = Literal["1d"]


@dataclass(frozen=True)
class DisabledBenchmarkConfig:
    enabled: Literal[False]
    mode: Literal["none"]


@dataclass(frozen=True)
class EnabledBenchmarkConfig:
    enabled: Literal[True]
    mode: BenchmarkModeEnabled
    id: BenchmarkId
    source: BenchmarkSource
    library_root: Path
    library_revision: str
    frequency: BenchmarkFrequency
    alignment_policy: str
    comparison_frequency: BenchmarkFrequency
    normalization_basis: str


BenchmarkConfig: TypeAlias = DisabledBenchmarkConfig | EnabledBenchmarkConfig


@dataclass(frozen=True)
class BenchmarkDatasetMetadata:
    benchmark_id: BenchmarkId
    dataset_path: Path
    row_count: int
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp


@dataclass(frozen=True)
class DisabledBenchmarkDataset:
    enabled: Literal[False]
    mode: Literal["none"]


@dataclass(frozen=True)
class LoadedBenchmarkDataset:
    enabled: Literal[True]
    config: EnabledBenchmarkConfig
    data: pd.DataFrame
    metadata: BenchmarkDatasetMetadata


LoadedBenchmarkResult: TypeAlias = DisabledBenchmarkDataset | LoadedBenchmarkDataset
