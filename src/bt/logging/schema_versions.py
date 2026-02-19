"""Backwards-compatible imports for artifact schema versions."""
from __future__ import annotations

from bt.contracts.schema_versions import (
    BENCHMARK_METRICS_SCHEMA_VERSION,
    COMPARISON_SUMMARY_SCHEMA_VERSION,
    PERFORMANCE_SCHEMA_VERSION,
    RUN_STATUS_SCHEMA_VERSION,
)

__all__ = [
    "RUN_STATUS_SCHEMA_VERSION",
    "PERFORMANCE_SCHEMA_VERSION",
    "BENCHMARK_METRICS_SCHEMA_VERSION",
    "COMPARISON_SUMMARY_SCHEMA_VERSION",
]
