from bt.hypotheses.contract import HypothesisContract


def test_l1_h7d_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h7d_adaptive_runner.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H7D"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["exit_monitoring_timeframe"] == "1m"


def test_l1_h7d_grid_is_exactly_24() -> None:
    rows = HypothesisContract.from_yaml("research/hypotheses/l1_h7d_adaptive_runner.yaml").materialize_grid()
    assert len(rows) == 24
