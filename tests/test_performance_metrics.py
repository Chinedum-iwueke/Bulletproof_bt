"""Tests for performance metrics."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.metrics.performance import compute_performance, write_performance_artifacts


def _write_equity(path: Path, values: list[float]) -> None:
    pd.DataFrame({"equity": values}).to_csv(path, index=False)


def _write_trades(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_compute_performance_metrics(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_001"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100.0, 110.0, 105.0, 120.0, 90.0, 95.0, 130.0])

    trades = [
        {"pnl_net": 10.0, "fees": -0.5, "slippage": -0.2, "vol_bucket": "low"},
        {"pnl_net": -5.0, "fees": -0.5, "slippage": -0.1, "vol_bucket": "low"},
        {"pnl_net": 7.0, "fees": -0.5, "slippage": -0.3, "vol_bucket": "high"},
        {"pnl_net": -2.0, "fees": -0.5, "slippage": -0.2, "vol_bucket": "high"},
        {"pnl_net": 0.0, "fees": -0.5, "slippage": -0.1, "vol_bucket": "low"},
        {"pnl_net": 4.0, "fees": -0.5, "slippage": -0.2, "vol_bucket": "high"},
    ]
    _write_trades(run_dir / "trades.csv", trades)

    report = compute_performance(run_dir)

    assert report.final_equity == 130.0
    assert report.total_trades == 6
    assert report.ev_net == pytest.approx(2.3333333333, rel=1e-6)
    assert report.ev_gross == pytest.approx(1.65, rel=1e-6)
    assert report.win_rate == pytest.approx(0.5, rel=1e-6)
    assert report.max_drawdown_pct == pytest.approx(-0.25, rel=1e-6)
    assert report.max_drawdown_duration_bars == 2
    assert report.tail_loss_p95 == pytest.approx(4.85, rel=1e-6)
    assert report.tail_loss_p99 == pytest.approx(4.97, rel=1e-6)
    assert report.fee_total == pytest.approx(-3.0, rel=1e-6)
    assert report.slippage_total == pytest.approx(-1.1, rel=1e-6)
    assert report.ev_by_bucket == {
        "high": pytest.approx(3.0, rel=1e-6),
        "low": pytest.approx(1.6666666667, rel=1e-6),
    }
    assert report.trades_by_bucket == {"high": 3, "low": 3}

    write_performance_artifacts(report, run_dir)
    assert (run_dir / "performance.json").exists()
    by_bucket = pd.read_csv(run_dir / "performance_by_bucket.csv")
    assert list(by_bucket["bucket"]) == ["high", "low"]
    assert list(by_bucket["n_trades"]) == [3, 3]


def test_compute_performance_missing_bucket(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_002"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100.0, 101.0])
    trades = [
        {"pnl_net": 1.0, "fees": -0.1, "slippage": -0.05},
        {"pnl_net": -2.0, "fees": -0.1, "slippage": -0.05},
    ]
    _write_trades(run_dir / "trades.csv", trades)

    report = compute_performance(run_dir)

    assert report.ev_by_bucket == {"all": pytest.approx(-0.5, rel=1e-6)}
    assert report.trades_by_bucket == {"all": 2}


def test_compute_performance_no_trades(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_003"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100.0, 100.0])
    pd.DataFrame(columns=["pnl_net", "fees", "slippage"]).to_csv(
        run_dir / "trades.csv", index=False
    )

    report = compute_performance(run_dir)

    assert report.total_trades == 0
    assert report.ev_net == 0.0
    assert report.ev_gross == 0.0
    assert report.tail_loss_p95 == 0.0
    assert report.tail_loss_p99 == 0.0
    assert report.fee_total == 0.0
    assert report.slippage_total == 0.0
    assert report.fee_drag_pct_of_gross is None
    assert report.slippage_drag_pct_of_gross is None
    assert report.ev_by_bucket == {"all": 0.0}
    assert report.trades_by_bucket == {"all": 0}
