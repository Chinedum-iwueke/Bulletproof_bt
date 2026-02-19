from __future__ import annotations

import json
from pathlib import Path

import pytest

from bt.logging.summary import write_summary_txt


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_required_run_artifacts(run_dir: Path) -> None:
    perf = {
        "net_pnl": 12.3456789,
        "gross_pnl": 15.0,
        "total_return": 0.12,
        "max_drawdown_pct": 10.0,
        "sharpe_annualized": 1.3,
        "sortino_annualized": 1.8,
        "mar_ratio": 1.1,
        "win_rate": 0.55,
        "profit_factor": 1.4,
        "ev_r_net": 0.03,
        "ev_r_gross": 0.05,
        "fee_total": 1.0,
        "slippage_total": 0.5,
        "spread_total": 0.25,
        "fee_drag_pct": 6.0,
        "slippage_drag_pct": 3.0,
        "spread_drag_pct": 1.0,
        "longest_loss_streak": 2,
        "max_drawdown_duration": 4,
    }
    _write_json(run_dir / "performance.json", perf)
    (run_dir / "trades.csv").write_text(
        "entry_ts,exit_ts,symbol,side,pnl,r_multiple_net\n"
        "2024-01-01,2024-01-02,BTCUSDT,LONG,1.0,0.5\n"
        "2024-01-03,2024-01-04,BTCUSDT,LONG,-2.0,-1.2\n",
        encoding="utf-8",
    )


def test_summary_txt_written_and_contains_sections(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_001"
    run_dir.mkdir()
    _write_required_run_artifacts(run_dir)

    summary_path = write_summary_txt(run_dir)

    assert summary_path.exists()
    content = summary_path.read_text(encoding="utf-8")
    assert "TOP METRICS" in content
    assert "WORST STREAK" in content
    assert "COST ATTRIBUTION" in content
    assert "MOST IMPORTANT CONCLUSION" in content


def test_summary_deterministic_ignoring_generated_line(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_002"
    run_dir.mkdir()
    _write_required_run_artifacts(run_dir)

    write_summary_txt(run_dir)
    first = (run_dir / "summary.txt").read_text(encoding="utf-8")

    write_summary_txt(run_dir)
    second = (run_dir / "summary.txt").read_text(encoding="utf-8")

    normalized_first = "\n".join(
        line for line in first.splitlines() if not line.startswith("Generated: ")
    )
    normalized_second = "\n".join(
        line for line in second.splitlines() if not line.startswith("Generated: ")
    )
    assert normalized_first == normalized_second


def test_benchmark_disabled_prints_disabled(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_003"
    run_dir.mkdir()
    _write_required_run_artifacts(run_dir)

    write_summary_txt(run_dir)

    content = (run_dir / "summary.txt").read_text(encoding="utf-8")
    assert "Benchmark: disabled" in content


def test_benchmark_enabled_missing_artifact_raises(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_004"
    run_dir.mkdir()
    _write_required_run_artifacts(run_dir)
    _write_json(run_dir / "run_manifest.json", {"benchmark_enabled": True})

    with pytest.raises(ValueError, match=r"run_dir=.*comparison_summary\.json"):
        write_summary_txt(run_dir)
