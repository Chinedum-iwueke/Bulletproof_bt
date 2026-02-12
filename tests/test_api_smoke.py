from __future__ import annotations

import json
from pathlib import Path

from bt.api import run_backtest, run_grid


def _reduced_performance(payload: dict[str, object]) -> dict[str, object]:
    keys = ["final_equity", "ev_net", "fee_total", "slippage_total"]
    return {key: payload.get(key) for key in keys}


def test_api_run_backtest_creates_run_dir_and_artifacts(tmp_path: Path) -> None:
    data_path = "data/curated/sample.csv"

    run_dir_1 = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path=data_path,
            out_dir=str(tmp_path / "out_1"),
        )
    )
    run_dir_2 = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path=data_path,
            out_dir=str(tmp_path / "out_2"),
        )
    )

    assert run_dir_1.exists()
    assert (run_dir_1 / "config_used.yaml").exists()

    expected_artifacts = [
        "performance.json",
        "trades.csv",
        "equity.csv",
        "decisions.jsonl",
        "fills.jsonl",
    ]
    for filename in expected_artifacts:
        assert (run_dir_1 / filename).exists()

    perf_1 = json.loads((run_dir_1 / "performance.json").read_text(encoding="utf-8"))
    perf_2 = json.loads((run_dir_2 / "performance.json").read_text(encoding="utf-8"))
    assert _reduced_performance(perf_1) == _reduced_performance(perf_2)


def test_api_run_grid_creates_experiment_outputs(tmp_path: Path) -> None:
    experiment_dir = Path(
        run_grid(
            config_path="configs/engine.yaml",
            experiment_path="configs/experiments/h1_volfloor_donchian.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "grid_out"),
        )
    )

    assert experiment_dir.exists()
    assert (experiment_dir / "summary.csv").exists()
    assert (experiment_dir / "summary.json").exists()
    assert (experiment_dir / "grid_used.yaml").exists()

    run_dirs = sorted((experiment_dir / "runs").iterdir())
    assert len(run_dirs) >= 1
    assert (run_dirs[0] / "config_used.yaml").exists()
