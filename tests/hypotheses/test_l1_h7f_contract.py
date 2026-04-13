from bt.hypotheses.contract import HypothesisContract


def test_l1_h7f_contract_loads_and_locks_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h7f_flow_gated_entry.yaml")
    assert contract.schema.metadata.hypothesis_id == "L1-H7F"
    assert contract.schema.execution_semantics["no_pyramiding"] is True


def test_l1_h7f_grid_is_exactly_12() -> None:
    rows = HypothesisContract.from_yaml("research/hypotheses/l1_h7f_flow_gated_entry.yaml").materialize_grid()
    assert len(rows) == 12
