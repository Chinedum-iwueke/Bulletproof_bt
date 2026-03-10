from bt.hypotheses.contract import HypothesisContract


def test_l1_h3_contract_loads_and_locked_semantics() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3_har_rv_gate_trend.yaml")
    sem = contract.schema.execution_semantics
    assert contract.schema.metadata.hypothesis_id == "L1-H3"
    assert sem["signal_timeframe"] == "15m"
    assert sem["base_data_frequency_expected"] == "1m"
    assert sem["gate_model"] == "har_rv_percentile"
    assert sem["stop_model"] == "fixed_close_sqrt_rvhat_multiple"
    assert sem["coefficient_refit_cadence"] == "daily_on_completed_signal_day"
    assert sem["fit_method"] == "deterministic_ols"
    assert sem["hold_time_unit"] == "signal_bars"


def test_l1_h3_grid_deterministic_and_preregistered() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h3_har_rv_gate_trend.yaml")
    one = contract.materialize_grid()
    two = contract.materialize_grid()
    assert one == two
    assert {row["params"]["fit_window_days"] for row in one} == {180, 365}
    assert {row["params"]["gate_quantile"] for row in one} == {0.3, 0.7}
    assert {row["params"]["k"] for row in one} == {1.5, 2.0}
