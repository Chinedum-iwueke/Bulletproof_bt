from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.metrics.performance import compute_cost_attribution, compute_performance


def _write_equity(path: Path) -> None:
    pd.DataFrame({"equity": [100.0, 100.0]}).to_csv(path, index=False)


def _write_trades(path: Path, rows: list[dict[str, float]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_fills(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def test_totals_and_net_math_consistent(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_a"
    run_dir.mkdir()
    _write_equity(run_dir / "equity.csv")
    _write_trades(run_dir / "trades.csv", [{"pnl_net": 94.0}])
    _write_fills(
        run_dir / "fills.jsonl",
        [
            {"fee": 1.0, "slippage": 2.0, "metadata": {"spread_cost": 0.25}},
            {"fee": 1.0, "slippage": 1.0, "metadata": {"spread_cost": 0.75}},
        ],
    )

    got = compute_cost_attribution(run_dir)

    assert got["fee_total"] == 2.0
    assert got["slippage_total"] == 3.0
    assert got["spread_total"] == 1.0
    assert got["gross_pnl"] == 96.0
    assert got["net_pnl"] == 94.0


def test_drag_pct_robust_with_negative_gross(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_b"
    run_dir.mkdir()
    _write_equity(run_dir / "equity.csv")
    _write_trades(run_dir / "trades.csv", [{"pnl_net": -55.0}])
    _write_fills(
        run_dir / "fills.jsonl",
        [{"fee": 5.0, "slippage": 0.0, "metadata": {"spread_cost": 0.0}}],
    )

    report = compute_performance(run_dir)

    assert report.gross_pnl == -50.0
    assert report.fee_total == 5.0
    assert report.fee_drag_pct == 10.0


def test_drag_pct_zero_gross_safe(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_c"
    run_dir.mkdir()
    _write_equity(run_dir / "equity.csv")
    _write_trades(run_dir / "trades.csv", [{"pnl_net": -6.0}])
    _write_fills(
        run_dir / "fills.jsonl",
        [{"fee": 2.0, "slippage": 3.0, "metadata": {"spread_cost": 1.0}}],
    )

    report = compute_performance(run_dir)

    assert report.gross_pnl == -4.0
    assert report.fee_drag_pct == 50.0
    assert report.slippage_drag_pct == 75.0
    assert report.spread_drag_pct == 25.0


def test_spread_absent_treated_as_zero(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_d"
    run_dir.mkdir()
    _write_equity(run_dir / "equity.csv")
    _write_trades(run_dir / "trades.csv", [{"pnl_net": 95.0}])
    _write_fills(
        run_dir / "fills.jsonl",
        [
            {"fee": 1.0, "slippage": 2.0},
            {"fee": 1.0, "slippage": 1.0},
        ],
    )

    got = compute_cost_attribution(run_dir)

    assert got["spread_total"] == 0.0
    assert got["gross_pnl"] == 97.0
    assert got["net_pnl"] == 95.0
