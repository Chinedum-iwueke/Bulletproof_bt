from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from bt.benchmark.metrics import compute_benchmark_metrics


def test_total_return_and_drawdown() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    points = [
        (t0, 1000.0),
        (t0 + timedelta(days=1), 1200.0),
        (t0 + timedelta(days=2), 900.0),
        (t0 + timedelta(days=3), 1100.0),
    ]

    metrics = compute_benchmark_metrics(equity_points=points)

    assert metrics["n_points"] == 4
    assert metrics["total_return"] == pytest.approx(0.1, abs=1e-12)
    assert metrics["max_drawdown"] == pytest.approx(0.25, abs=1e-12)
    assert metrics["max_drawdown_pct"] == pytest.approx(25.0, abs=1e-10)


def test_invalid_equity_raises() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    points = [
        (t0, 1000.0),
        (t0 + timedelta(days=1), 0.0),
    ]

    with pytest.raises(ValueError, match=r"equity must be > 0"):
        compute_benchmark_metrics(equity_points=points)


def test_non_monotonic_timestamps_raise() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=1)
    t2 = t0 + timedelta(days=2)
    points = [(t0, 1000.0), (t2, 1010.0), (t1, 1020.0)]

    with pytest.raises(ValueError, match=r"strictly increasing"):
        compute_benchmark_metrics(equity_points=points)


def test_sharpe_only_when_annualization_inferred() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    minute_points = [
        (t0, 1000.0),
        (t0 + timedelta(minutes=1), 1002.0),
        (t0 + timedelta(minutes=2), 1001.0),
        (t0 + timedelta(minutes=3), 1004.0),
    ]
    minute_metrics = compute_benchmark_metrics(equity_points=minute_points)
    assert isinstance(minute_metrics["sharpe"], float)

    irregular_points = [
        (t0, 1000.0),
        (t0 + timedelta(minutes=7), 1002.0),
        (t0 + timedelta(minutes=19), 1001.0),
        (t0 + timedelta(minutes=43), 1004.0),
    ]
    irregular_metrics = compute_benchmark_metrics(equity_points=irregular_points)
    assert irregular_metrics["sharpe"] is None
    assert irregular_metrics["sortino"] is None
    assert irregular_metrics["volatility"] is None
