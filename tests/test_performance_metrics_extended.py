"""Extended tests for performance metrics upgrades."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bt.metrics.performance import compute_performance, write_performance_artifacts


def _write_equity(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_trades(path: Path, rows: list[dict[str, object]]) -> None:
    if rows:
        pd.DataFrame(rows).to_csv(path, index=False)
    else:
        pd.DataFrame(columns=["pnl_net", "fees", "slippage"]).to_csv(path, index=False)


def test_trade_returns_written_and_ordered(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_trade_returns"
    run_dir.mkdir()

    _write_equity(
        run_dir / "equity.csv",
        [
            {"ts": "2023-01-01T00:00:00Z", "equity": 100.0},
            {"ts": "2023-01-02T00:00:00Z", "equity": 101.0},
            {"ts": "2023-01-03T00:00:00Z", "equity": 102.0},
            {"ts": "2023-01-04T00:00:00Z", "equity": 103.0},
        ],
    )

    trades = [
        {
            "entry_ts": "2023-01-02T00:00:00Z",
            "exit_ts": "2023-01-03T00:00:00Z",
            "symbol": "BBB",
            "side": "LONG",
            "qty": 2.0,
            "entry_price": 10.0,
            "pnl_net": 1.0,
            "fees": -0.1,
            "slippage": -0.05,
        },
        {
            "entry_ts": "2023-01-01T00:00:00Z",
            "exit_ts": "2023-01-02T00:00:00Z",
            "symbol": "AAA",
            "side": "SHORT",
            "qty": 1.0,
            "entry_price": 10.0,
            "pnl_net": -0.5,
            "fees": -0.05,
            "slippage": -0.02,
        },
        {
            "entry_ts": "2023-01-01T00:00:00Z",
            "exit_ts": "2023-01-02T00:00:00Z",
            "symbol": "CCC",
            "side": "LONG",
            "qty": 1.0,
            "entry_price": 20.0,
            "pnl_net": 0.4,
            "fees": -0.02,
            "slippage": -0.01,
        },
    ]
    _write_trades(run_dir / "trades.csv", trades)

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    trade_returns_path = run_dir / "trade_returns.csv"
    trade_returns = pd.read_csv(trade_returns_path)

    assert list(trade_returns.columns) == [
        "entry_ts",
        "exit_ts",
        "symbol",
        "side",
        "trade_return",
        "pnl",
        "fees",
        "slippage",
    ]
    assert list(trade_returns["symbol"]) == ["AAA", "CCC", "BBB"]


def test_trade_return_moments(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_trade_moments"
    run_dir.mkdir()

    _write_equity(
        run_dir / "equity.csv",
        [
            {"ts": "2023-01-01T00:00:00Z", "equity": 100.0},
            {"ts": "2023-01-02T00:00:00Z", "equity": 101.0},
            {"ts": "2023-01-03T00:00:00Z", "equity": 102.0},
            {"ts": "2023-01-04T00:00:00Z", "equity": 103.0},
        ],
    )

    trade_returns = [-1.0, 0.0, 1.0]
    trades = []
    for idx, value in enumerate(trade_returns):
        trades.append(
            {
                "entry_ts": f"2023-01-{idx + 1:02d}T00:00:00Z",
                "exit_ts": f"2023-01-{idx + 1:02d}T01:00:00Z",
                "symbol": f"SYM{idx}",
                "side": "LONG",
                "qty": 1.0,
                "entry_price": 1.0,
                "pnl_net": value,
                "fees": 0.0,
                "slippage": 0.0,
            }
        )
    _write_trades(run_dir / "trades.csv", trades)

    report = compute_performance(run_dir)

    values = np.array(trade_returns, dtype=float)
    mean = values.mean()
    std = values.std()
    centered = values - mean
    expected_skew = np.mean(centered**3) / (std**3)
    expected_kurtosis_excess = np.mean(centered**4) / (std**4) - 3.0

    assert report.trade_return_skew == pytest.approx(expected_skew, rel=1e-6)
    assert report.trade_return_kurtosis_excess == pytest.approx(
        expected_kurtosis_excess, rel=1e-6
    )


def test_trade_return_streaks(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_trade_streaks"
    run_dir.mkdir()

    _write_equity(
        run_dir / "equity.csv",
        [
            {"ts": "2023-01-01T00:00:00Z", "equity": 100.0},
            {"ts": "2023-01-02T00:00:00Z", "equity": 101.0},
            {"ts": "2023-01-03T00:00:00Z", "equity": 102.0},
            {"ts": "2023-01-04T00:00:00Z", "equity": 103.0},
        ],
    )

    trade_returns = [0.1, -0.2, -0.3, 0.0, -0.1, -0.4, -0.2, 0.2]
    trades = []
    for idx, value in enumerate(trade_returns):
        trades.append(
            {
                "entry_ts": f"2023-01-{idx + 1:02d}T00:00:00Z",
                "exit_ts": f"2023-01-{idx + 1:02d}T01:00:00Z",
                "symbol": f"SYM{idx}",
                "side": "LONG",
                "qty": 1.0,
                "entry_price": 1.0,
                "pnl_net": value,
                "fees": 0.0,
                "slippage": 0.0,
            }
        )
    _write_trades(run_dir / "trades.csv", trades)

    report = compute_performance(run_dir)

    assert report.max_consecutive_losses == 3
    assert report.worst_streak_loss == pytest.approx(-0.7, rel=1e-6)


def test_equity_based_metrics(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_equity_metrics"
    run_dir.mkdir()

    equity_rows = [
        {"ts": "2023-01-01T00:00:00Z", "equity": 100.0},
        {"ts": "2023-01-02T00:00:00Z", "equity": 110.0},
        {"ts": "2023-01-03T00:00:00Z", "equity": 90.0},
        {"ts": "2023-01-04T00:00:00Z", "equity": 120.0},
        {"ts": "2023-01-05T00:00:00Z", "equity": 100.0},
    ]
    _write_equity(run_dir / "equity.csv", equity_rows)
    _write_trades(run_dir / "trades.csv", [])

    report = compute_performance(run_dir)

    equity_values = np.array([row["equity"] for row in equity_rows], dtype=float)
    returns = equity_values[1:] / equity_values[:-1] - 1.0
    mean_return = returns.mean()
    std_return = returns.std()
    expected_sharpe = mean_return / std_return * np.sqrt(365)
    downside = returns[returns < 0]
    expected_sortino = mean_return / downside.std() * np.sqrt(365)

    ts = pd.to_datetime(pd.Series([row["ts"] for row in equity_rows]))
    total_seconds = (ts.iloc[-1] - ts.iloc[0]).total_seconds()
    seconds_per_year = 365.25243600 * 24 * 60 * 60
    total_years = total_seconds / seconds_per_year
    expected_cagr = (equity_values[-1] / equity_values[0]) ** (1.0 / total_years) - 1.0
    expected_mar = expected_cagr / abs(report.max_drawdown_pct)

    assert report.sharpe_annualized == pytest.approx(expected_sharpe, rel=1e-6)
    assert report.sortino_annualized == pytest.approx(expected_sortino, rel=1e-6)
    assert report.cagr == pytest.approx(expected_cagr, rel=1e-6)
    assert report.mar_ratio == pytest.approx(expected_mar, rel=1e-6)


def test_param_stability_metrics(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_param_sweep"
    run_dir.mkdir()

    param_sweep_dir = run_dir / "param_sweep"
    param_sweep_dir.mkdir()

    summaries = [
        {"ev_net": 1.0, "max_drawdown_pct": -0.1, "sharpe_annualized": 1.2},
        {"ev_net": 2.0, "max_drawdown_pct": -0.2, "sharpe_annualized": 1.0},
        {"ev_net": 1.5, "max_drawdown_pct": -0.15, "sharpe_annualized": 1.1},
    ]
    for idx, summary in enumerate(summaries):
        sweep_dir = param_sweep_dir / f"run_{idx}"
        sweep_dir.mkdir()
        with (sweep_dir / "performance.json").open("w", encoding="utf-8") as handle:
            json.dump(summary, handle)

    _write_equity(
        run_dir / "equity.csv",
        [
            {"ts": "2023-01-01T00:00:00Z", "equity": 100.0},
            {"ts": "2023-01-02T00:00:00Z", "equity": 101.0},
            {"ts": "2023-01-03T00:00:00Z", "equity": 102.0},
            {"ts": "2023-01-04T00:00:00Z", "equity": 103.0},
        ],
    )
    _write_trades(run_dir / "trades.csv", [])

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    stability_path = run_dir / "param_stability.json"
    assert stability_path.exists()

    stability = json.loads(stability_path.read_text(encoding="utf-8"))
    expected_ev_std = float(np.std([1.0, 2.0, 1.5]))
    expected_dd_std = float(np.std([-0.1, -0.2, -0.15]))
    sharpe_vals = np.array([1.2, 1.0, 1.1])
    expected_flatness = float(np.std(sharpe_vals) / (abs(sharpe_vals.mean()) + 1e-12))

    assert stability["ev_std_across_params"] == pytest.approx(expected_ev_std, rel=1e-6)
    assert stability["dd_std_across_params"] == pytest.approx(expected_dd_std, rel=1e-6)
    assert stability["sharpe_surface_flatness"] == pytest.approx(
        expected_flatness, rel=1e-6
    )
