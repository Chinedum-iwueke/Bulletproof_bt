import pandas as pd

from bt.strategy.l1_h3c_har_regime_switch import L1H3CHarRegimeSwitchStrategy


def test_l1_h3c_artifacts_include_coefficients_and_split_manifest() -> None:
    strategy = L1H3CHarRegimeSwitchStrategy(fit_window_days=180)
    st = strategy._state_for("BTCUSDT")
    start = pd.Timestamp("2023-01-01", tz="UTC")
    for i in range(96 * 220):
        st.rv_forecaster.update(start + pd.Timedelta(minutes=15 * i), 100 + 0.001 * i)
    payload = strategy.strategy_artifacts()
    assert "har_coefficients" in payload
    assert "har_split_manifest" in payload
    assert payload["har_coefficients"]["signal_basis"] == "15m"
    assert "BTCUSDT" in payload["har_coefficients"]["rows"]
