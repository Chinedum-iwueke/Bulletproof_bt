import pytest

from bt.benchmark.compare import compare_strategy_vs_benchmark


def test_delta_computed_for_overlapping_fields() -> None:
    strategy_perf = {
        "total_return": 0.20,
        "max_drawdown": 0.10,
        "sharpe": 1.5,
        "ev_r_net": 0.12,
    }
    bench_metrics = {
        "total_return": 0.05,
        "max_drawdown": 0.25,
        "sharpe": 0.7,
    }

    summary = compare_strategy_vs_benchmark(strategy_perf=strategy_perf, bench_metrics=bench_metrics)

    assert summary["delta"]["total_return"] == pytest.approx(0.15)
    assert summary["delta"]["max_drawdown"] == pytest.approx(-0.15)
    assert summary["delta"]["sharpe"] == pytest.approx(0.8)
    assert "ev_r_net" in summary["strategy"]
    assert "ev_r_net" not in summary["benchmark"]
    assert "ev_r_net" not in summary["delta"]


def test_non_numeric_or_missing_fields_do_not_produce_delta() -> None:
    strategy_perf = {"sharpe": None, "total_return": 0.1}
    bench_metrics = {"sharpe": 0.5, "total_return": 0.05}

    summary = compare_strategy_vs_benchmark(strategy_perf=strategy_perf, bench_metrics=bench_metrics)

    assert summary["delta"] == {"total_return": pytest.approx(0.05)}
    assert "sharpe" not in summary["delta"]
