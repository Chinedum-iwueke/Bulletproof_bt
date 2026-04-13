from bt.hypotheses.contract import HypothesisContract


def test_l1_h7c_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h7c_high_selectivity_regime.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H7C"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["exit_monitoring_timeframe"] == "1m"
    assert sem["risk_accounting"] == "engine_canonical_R"


def test_l1_h7c_grid_is_exactly_12() -> None:
    rows = HypothesisContract.from_yaml("research/hypotheses/l1_h7c_high_selectivity_regime.yaml").materialize_grid()
    assert len(rows) == 12
