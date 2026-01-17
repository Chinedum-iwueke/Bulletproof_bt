"""Tests for robustness metrics from stress-suite outputs."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.metrics.robustness import compute_robustness


def _write_trades(path: Path, pnl_values: list[float]) -> None:
    rows = []
    for pnl in pnl_values:
        rows.append({"pnl_net": pnl})
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_equity(path: Path, equity_values: list[float]) -> None:
    pd.DataFrame({"equity": equity_values}).to_csv(path, index=False)


def test_compute_robustness_deterministic(tmp_path: Path) -> None:
    output_root = tmp_path / "runs"
    baseline_dir = output_root / "baseline_run"
    cost_dir = output_root / "fee_run"
    delay_dir = output_root / "delay_run"

    baseline_dir.mkdir(parents=True)
    cost_dir.mkdir(parents=True)
    delay_dir.mkdir(parents=True)

    _write_trades(baseline_dir / "trades.csv", [1.0, 2.0, 3.0])
    _write_trades(cost_dir / "trades.csv", [-1.0])
    _write_trades(delay_dir / "trades.csv", [0.4])

    _write_equity(baseline_dir / "equity.csv", [100.0, 110.0])
    _write_equity(cost_dir / "equity.csv", [100.0, 90.0])
    _write_equity(delay_dir / "equity.csv", [100.0, 105.0])

    stress_summary = {
        "summary": {
            "baseline": {
                "run_id": "baseline_run",
                "run_dir": str(baseline_dir),
                "final_equity": 110.0,
                "n_trades": 3,
                "n_fills": 10,
            },
            "fee_hit": {
                "run_id": "fee_run",
                "run_dir": str(cost_dir),
                "final_equity": 90.0,
                "n_trades": 1,
                "n_fills": 5,
            },
            "delay_hit": {
                "run_id": "delay_run",
                "run_dir": str(delay_dir),
                "final_equity": 105.0,
                "n_trades": 1,
                "n_fills": 8,
            },
        },
        "scenarios": [
            {
                "name": "baseline",
                "fee_mult": 1.0,
                "slippage_mult": 1.0,
                "add_delay_bars": 0,
                "drop_fill_prob": 0.0,
            },
            {
                "name": "fee_hit",
                "fee_mult": 2.0,
                "slippage_mult": 1.0,
                "add_delay_bars": 0,
                "drop_fill_prob": 0.0,
            },
            {
                "name": "delay_hit",
                "fee_mult": 1.0,
                "slippage_mult": 1.0,
                "add_delay_bars": 2,
                "drop_fill_prob": 0.0,
            },
        ],
    }

    result = compute_robustness(
        stress_summary=stress_summary,
        output_root=str(output_root),
        strategy_name="demo",
        ev_floor=0.5,
    )

    assert result.baseline_run_id == "baseline_run"
    assert result.breaking_fee_mult == 2.0
    assert result.breaking_delay_bars == 2
    assert result.robustness_score == pytest.approx(0.6, rel=1e-6)


def test_compute_robustness_edge_cases(tmp_path: Path) -> None:
    output_root = tmp_path / "runs"
    baseline_dir = output_root / "baseline_run"
    empty_dir = output_root / "empty_run"

    baseline_dir.mkdir(parents=True)
    empty_dir.mkdir(parents=True)

    _write_trades(baseline_dir / "trades.csv", [1.0])
    _write_equity(baseline_dir / "equity.csv", [100.0])
    _write_trades(empty_dir / "trades.csv", [])
    _write_equity(empty_dir / "equity.csv", [100.0])

    missing_baseline = {
        "summary": {
            "not_baseline": {
                "run_id": "empty_run",
                "run_dir": str(empty_dir),
                "final_equity": 100.0,
                "n_trades": 0,
                "n_fills": 0,
            }
        },
        "scenarios": [
            {
                "name": "not_baseline",
                "fee_mult": 2.0,
                "slippage_mult": 1.0,
                "add_delay_bars": 0,
                "drop_fill_prob": 0.0,
            }
        ],
    }

    with pytest.raises(ValueError):
        compute_robustness(
            stress_summary=missing_baseline,
            output_root=str(output_root),
        )

    stress_summary = {
        "summary": {
            "baseline": {
                "run_id": "baseline_run",
                "run_dir": str(baseline_dir),
                "final_equity": 100.0,
                "n_trades": 1,
                "n_fills": 4,
            },
            "empty_trades": {
                "run_id": "empty_run",
                "run_dir": str(empty_dir),
                "final_equity": 100.0,
                "n_trades": 0,
                "n_fills": 0,
            },
        },
        "scenarios": [
            {
                "name": "baseline",
                "fee_mult": 1.0,
                "slippage_mult": 1.0,
                "add_delay_bars": 0,
                "drop_fill_prob": 0.0,
            },
            {
                "name": "empty_trades",
                "fee_mult": 1.0,
                "slippage_mult": 1.0,
                "add_delay_bars": 0,
                "drop_fill_prob": 0.0,
            },
        ],
    }

    result = compute_robustness(
        stress_summary=stress_summary,
        output_root=str(output_root),
    )

    empty_row = next(
        row for row in result.per_scenario if row["scenario"] == "empty_trades"
    )
    assert empty_row["ev"] == 0.0
