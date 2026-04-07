from bt.hypotheses.contract import HypothesisContract


def test_l1_h7a_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h7a_squeeze_expansion_pullback.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H7A"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["signal_timeframe"] == "15m"
    assert sem["exit_monitoring_timeframe"] == "1m"
    assert sem["family_pattern"] == "squeeze_expansion_pullback"
    assert sem["risk_accounting"] == "engine_canonical_R"
    assert sem["no_pyramiding"] is True


def test_l1_h7a_grid_is_exactly_24() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h7a_squeeze_expansion_pullback.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 24
