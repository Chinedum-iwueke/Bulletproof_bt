from __future__ import annotations

from pathlib import Path

import yaml

from bt.api import run_backtest, run_grid


def test_run_backtest_override_written_to_config_used(tmp_path: Path) -> None:
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump({"risk": {"max_positions": 2}}, sort_keys=False), encoding="utf-8")

    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "runs"),
            override_paths=[str(override_path)],
            run_name="override_smoke",
        )
    )

    cfg = yaml.safe_load((run_dir / "config_used.yaml").read_text(encoding="utf-8"))
    assert cfg["risk"]["max_positions"] == 2


def test_run_grid_override_and_outputs(tmp_path: Path) -> None:
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump({"risk": {"max_positions": 2}}, sort_keys=False), encoding="utf-8")

    experiment_path = tmp_path / "experiment.yaml"
    experiment_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "name": "smoke",
                "fixed": {"strategy": {"name": "coinflip", "p_trade": 0.0}},
                "grid": {"strategy.seed": [7]},
                "run_naming": {"template": "seed{strategy.seed}"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    exp_dir = Path(
        run_grid(
            config_path="configs/engine.yaml",
            experiment_path=str(experiment_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "grid"),
            override_paths=[str(override_path)],
        )
    )

    assert (exp_dir / "summary.csv").exists()
    assert (exp_dir / "summary.json").exists()
    assert any((exp_dir / "runs").iterdir())
