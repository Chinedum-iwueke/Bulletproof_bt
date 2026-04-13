from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract


def test_runtime_override_uses_grid_signal_timeframe_for_h7f() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h7f_flow_gated_entry.yaml")
    spec = next(row for row in contract.to_run_specs() if row["params"]["signal_timeframe"] == "1h")
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["timeframe"] == "1h"


def test_parallel_manifest_build_for_h7d(tmp_path: Path) -> None:
    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l1_h7d_adaptive_runner.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()
