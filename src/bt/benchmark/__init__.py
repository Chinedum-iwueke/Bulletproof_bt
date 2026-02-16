"""Benchmark configuration parsing."""

from bt.benchmark.buy_hold import EquityPoint, compute_buy_hold_equity
from bt.benchmark.metrics import BenchmarkMetrics, compute_benchmark_metrics
from bt.benchmark.spec import BenchmarkSpec, parse_benchmark_spec

__all__ = [
    "BenchmarkMetrics",
    "BenchmarkSpec",
    "EquityPoint",
    "compute_benchmark_metrics",
    "compute_buy_hold_equity",
    "parse_benchmark_spec",
]
