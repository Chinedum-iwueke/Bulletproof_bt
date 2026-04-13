from bt.hypotheses.contract import HypothesisContract


def test_l1_h9a_contract_grid_and_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h9a_momentum_breakout.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 24
    assert contract.schema.entry["strategy"] == "l1_h9_momentum_breakout"
    assert contract.schema.execution_semantics["base_data_frequency_expected"] == "1m"
    assert contract.schema.execution_semantics["exit_monitoring_timeframe"] == "1m"
    assert contract.schema.execution_semantics["risk_accounting"] == "engine_canonical_R"
