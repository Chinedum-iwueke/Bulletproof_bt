from bt.hypotheses.contract import HypothesisContract


def test_l1_h3c_contract_loads_and_locked_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3c_har_regime_switch.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H3C"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["strategy_family"] == "regime_switch"
    assert sem["gate_model"] == "har_rv_percentile_switch"
    assert sem["stop_update_policy"] == "frozen_at_entry"
    assert sem["branch_high_vol"]["signal_timeframe"] == "15m"
    assert sem["branch_low_vol"]["signal_timeframe"] == "5m"


def test_l1_h3c_grid_deterministic() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3c_har_regime_switch.yaml")
    one = contract.materialize_grid()
    two = contract.materialize_grid()
    assert one == two
    assert {row["params"]["fit_window_days"] for row in one} == {180, 365}
    assert {row["params"]["q_low"] for row in one} == {0.2, 0.3}
    assert {row["params"]["q_high"] for row in one} == {0.7, 0.8}
    assert {row["params"]["k"] for row in one} == {1.5, 2.0}
