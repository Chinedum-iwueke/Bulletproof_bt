from pathlib import Path

from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest_rows
from bt.hypotheses.contract import HypothesisContract


def test_l1_h3_manifest_generation_smoke() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3_har_rv_gate_trend.yaml")
    rows = build_hypothesis_manifest_rows(
        contract=contract,
        hypothesis_path=Path("research/hypotheses/l1_h3_har_rv_gate_trend.yaml"),
        phase="tier2",
    )
    assert rows
    assert rows[0]["hypothesis_id"] == "L1-H3"
    assert rows[0]["tier"] == "Tier2"


def test_l1_h3_runtime_override_uses_strategy_and_15m_two_clock() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3_har_rv_gate_trend.yaml")
    spec = contract.to_run_specs()[0]
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["data"]["exit_timeframe"] == "1m"
    assert override["htf_resampler"]["timeframes"] == ["15m"]
    assert override["strategy"]["name"] == "l1_h3_har_rv_gate_trend"
