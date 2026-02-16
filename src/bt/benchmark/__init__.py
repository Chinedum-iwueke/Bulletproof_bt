"""Benchmark configuration parsing."""

from bt.benchmark.buy_hold import EquityPoint, compute_buy_hold_equity
from bt.benchmark.spec import BenchmarkSpec, parse_benchmark_spec

__all__ = [
    "BenchmarkSpec",
    "EquityPoint",
    "compute_buy_hold_equity",
    "parse_benchmark_spec",
]
