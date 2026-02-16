from __future__ import annotations

from numbers import Real
from typing import Any, Mapping

_COMPARABLE_FIELDS: tuple[str, ...] = (
    "total_return",
    "max_drawdown",
    "sharpe",
    "sortino",
    "cagr",
)

_STRATEGY_ONLY_R_FIELDS: tuple[str, ...] = (
    "ev_r_net",
    "ev_r_gross",
    "avg_r_win",
    "avg_r_loss",
    "profit_factor_r",
    "payoff_ratio_r",
    "win_rate_r",
)


def _is_numeric(value: Any) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool)


def compare_strategy_vs_benchmark(
    *,
    strategy_perf: Mapping[str, Any],
    bench_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Build a V1 comparison summary (JSON-serializable).

    Required fields (always present):
      - strategy: { ...selected metrics... }
      - benchmark: { ...selected metrics... }
      - delta: { ...strategy - benchmark for numeric comparable fields... }

    Selection rules:
      - Prefer these fields if present:
          total_return
          max_drawdown
          sharpe
          sortino
          cagr
      - Also include strategy-only R metrics if present (no benchmark counterpart):
          ev_r_net, ev_r_gross, avg_r_win, avg_r_loss, profit_factor_r, payoff_ratio_r, win_rate_r

    Delta rules:
      - Only compute delta for fields present on BOTH sides AND numeric.
      - delta[field] = strategy[field] - benchmark[field]
      - If a metric is missing or non-numeric for either side, do not include it in delta.
    """
    strategy: dict[str, Any] = {}
    benchmark: dict[str, Any] = {}
    delta: dict[str, Any] = {}

    for field in _COMPARABLE_FIELDS:
        has_strategy = field in strategy_perf
        has_benchmark = field in bench_metrics

        if has_strategy:
            strategy[field] = strategy_perf[field]
        if has_benchmark:
            benchmark[field] = bench_metrics[field]

        if not has_strategy or not has_benchmark:
            continue

        strategy_value = strategy_perf[field]
        benchmark_value = bench_metrics[field]
        if _is_numeric(strategy_value) and _is_numeric(benchmark_value):
            delta[field] = strategy_value - benchmark_value

    for field in _STRATEGY_ONLY_R_FIELDS:
        if field in strategy_perf:
            strategy[field] = strategy_perf[field]

    return {
        "strategy": strategy,
        "benchmark": benchmark,
        "delta": delta,
    }

