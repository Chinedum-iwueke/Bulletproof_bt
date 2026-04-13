from bt.hypotheses.contract import HypothesisContract


def test_l1_h9b_contract_grid_and_runner_shape() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h9b_breakout_partial_runner.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 24
    assert contract.schema.exit["runner_trail_model"] == "atr_multiple_after_tp1"
    assert contract.schema.execution_semantics["risk_accounting"] == "engine_canonical_R"
