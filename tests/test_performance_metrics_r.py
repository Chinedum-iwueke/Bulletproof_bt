from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bt.metrics.performance import compute_performance, write_performance_artifacts
from bt.metrics.r_metrics import summarize_r


def _write_equity(path: Path, values: list[float]) -> None:
    pd.DataFrame({"equity": values}).to_csv(path, index=False)


def _write_trades(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_summarize_r_basic_correctness() -> None:
    summary = summarize_r([1.0, -0.5, 2.0, None])

    assert summary.n == 3
    assert summary.ev_r == pytest.approx((1.0 - 0.5 + 2.0) / 3.0)
    assert summary.win_rate == pytest.approx(2.0 / 3.0)
    assert summary.avg_r_win == pytest.approx((1.0 + 2.0) / 2.0)
    assert summary.avg_r_loss == pytest.approx(-0.5)
    assert summary.profit_factor_r == pytest.approx((1.0 + 2.0) / 0.5)
    assert summary.payoff_ratio_r == pytest.approx(summary.avg_r_win / abs(summary.avg_r_loss))


def test_summarize_r_empty_values_returns_none_metrics() -> None:
    summary = summarize_r([None, None])

    assert summary.n == 0
    assert summary.ev_r is None
    assert summary.win_rate is None
    assert summary.avg_r_win is None
    assert summary.avg_r_loss is None
    assert summary.sum_r_pos is None
    assert summary.sum_r_neg_abs is None
    assert summary.profit_factor_r is None
    assert summary.payoff_ratio_r is None


def test_performance_json_includes_r_metrics_when_present(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_r"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100.0, 110.0, 108.0, 115.0])
    trades = [
        {"pnl_net": 10.0, "fees": -1.0, "slippage": -0.5, "r_multiple_net": 1.0, "r_multiple_gross": 1.2},
        {"pnl_net": -5.0, "fees": -0.5, "slippage": -0.2, "r_multiple_net": -0.5, "r_multiple_gross": -0.4},
        {"pnl_net": 7.0, "fees": -0.7, "slippage": -0.3, "r_multiple_net": 2.0, "r_multiple_gross": 2.2},
    ]
    _write_trades(run_dir / "trades.csv", trades)

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    expected_net = summarize_r([1.0, -0.5, 2.0])
    expected_gross = summarize_r([1.2, -0.4, 2.2])

    for key in [
        "ev_r_gross",
        "ev_r_net",
        "avg_r_win",
        "avg_r_loss",
        "profit_factor_r",
        "payoff_ratio_r",
    ]:
        assert key in payload

    assert payload["ev_r_net"] == pytest.approx(expected_net.ev_r)
    assert payload["ev_r_gross"] == pytest.approx(expected_gross.ev_r)
    assert payload["win_rate_r"] == pytest.approx(expected_net.win_rate)
    assert payload["avg_r_win"] == pytest.approx(expected_net.avg_r_win)
    assert payload["avg_r_loss"] == pytest.approx(expected_net.avg_r_loss)
    assert payload["profit_factor_r"] == pytest.approx(expected_net.profit_factor_r)
    assert payload["payoff_ratio_r"] == pytest.approx(expected_net.payoff_ratio_r)


def test_performance_json_legacy_run_without_r_fields_is_resilient(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_legacy"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100.0, 101.0, 99.0])
    _write_trades(
        run_dir / "trades.csv",
        [
            {"pnl_net": 1.0, "fees": -0.1, "slippage": -0.1},
            {"pnl_net": -2.0, "fees": -0.2, "slippage": -0.1},
        ],
    )

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    for key in [
        "ev_r_gross",
        "ev_r_net",
        "win_rate_r",
        "avg_r_win",
        "avg_r_loss",
        "profit_factor_r",
        "payoff_ratio_r",
    ]:
        assert key in payload
        assert payload[key] is None
