from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bt.logging.summary import write_summary_txt
from bt.metrics.performance import compute_performance, write_performance_artifacts


def _write_equity(path: Path, values: list[float]) -> None:
    pd.DataFrame(
        {
            "ts": [f"2024-01-0{i+1}T00:00:00Z" for i in range(len(values))],
            "equity": values,
        }
    ).to_csv(path, index=False)


def test_performance_includes_initial_equity_and_total_return(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [100000.0, 101000.0, 99000.0])
    pd.DataFrame(columns=["pnl_net", "fees", "slippage"]).to_csv(run_dir / "trades.csv", index=False)

    report = compute_performance(run_dir)
    assert report.initial_equity == 100000.0
    assert report.final_equity == 99000.0
    assert report.total_return == pytest.approx(-0.01)

    write_performance_artifacts(report, run_dir)
    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    assert payload["initial_equity"] == 100000.0
    assert payload["total_return"] == pytest.approx(-0.01)


def test_summary_uses_total_return_from_performance(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_summary"
    run_dir.mkdir()

    _write_equity(run_dir / "equity.csv", [1000.0, 1200.0])
    pd.DataFrame(columns=["pnl_net", "fees", "slippage"]).to_csv(run_dir / "trades.csv", index=False)

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)
    (run_dir / "run_status.json").write_text('{"status":"PASS"}', encoding="utf-8")

    summary_path = write_summary_txt(run_dir)
    summary = summary_path.read_text(encoding="utf-8")
    assert "- Total Return: 0.200000" in summary
