from bt.hypotheses.contract import HypothesisContract


def test_l1_h7e_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h7e_flow_sensitive_runner.yaml")
    assert contract.schema.metadata.hypothesis_id == "L1-H7E"
    assert contract.schema.execution_semantics["risk_accounting"] == "engine_canonical_R"


def test_l1_h7e_grid_is_exactly_12() -> None:
    rows = HypothesisContract.from_yaml("research/hypotheses/l1_h7e_flow_sensitive_runner.yaml").materialize_grid()
    assert len(rows) == 12
