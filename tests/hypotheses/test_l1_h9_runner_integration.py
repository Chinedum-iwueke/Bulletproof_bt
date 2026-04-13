from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract


def test_l1_h9b_runtime_override_and_parallel_manifest(tmp_path: Path) -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h9b_breakout_partial_runner.yaml")
    spec = next(row for row in contract.to_run_specs() if row["params"]["signal_timeframe"] == "1h")
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l1_h9_momentum_breakout"
    assert override["strategy"]["timeframe"] == "1h"

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l1_h9b_breakout_partial_runner.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()
    assert manifest.name == "l1_h9b_breakout_partial_runner_tier2_grid.csv"
