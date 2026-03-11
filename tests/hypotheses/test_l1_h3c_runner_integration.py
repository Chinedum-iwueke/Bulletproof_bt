from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest_rows
from bt.hypotheses.contract import HypothesisContract


def test_l1_h3c_manifest_generation_smoke() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3c_har_regime_switch.yaml")
    rows = build_hypothesis_manifest_rows(
        contract=contract,
        hypothesis_path=Path("research/hypotheses/l1_h3c_har_regime_switch.yaml"),
        phase="tier2",
    )
    assert rows
    assert rows[0]["hypothesis_id"] == "L1-H3C"


def test_l1_h3c_runtime_override_uses_multitimeframe_resampler() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3c_har_regime_switch.yaml")
    spec = contract.to_run_specs()[0]
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l1_h3c_har_regime_switch"
    assert override["htf_resampler"]["timeframes"] == ["15m", "5m"]
