import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h3c_har_regime_switch import L1H3CHarRegimeSwitchStrategy


def _bar(ts: pd.Timestamp, close: float = 100.0) -> Bar:
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2, low=close - 0.2, close=close, volume=1000)


def test_l1_h3c_high_vol_selects_trend_branch(monkeypatch) -> None:
    s = L1H3CHarRegimeSwitchStrategy(q_low=0.2, q_high=0.7, z0=0.0)
    st = s._state_for("BTCUSDT")
    st.regime_label = "high_vol_trend"
    st.regime_branch = "L1-H1"
    st.regime_rv_hat_t = 0.01
    st.regime_rvhat_pct_t = 0.95
    st.regime_rv_payload = {"rv1_t": 0.01, "rv_d": 0.01, "rv_w": 0.01, "rv_m": 0.01}
    st.ema_fast._ema.value = 101.0
    st.ema_slow._ema.value = 100.0
    monkeypatch.setattr(s, "_update_regime", lambda *_: None)

    ts = pd.Timestamp("2023-01-01 00:15:00", tz="UTC")
    out = s.on_bars(ts, {"BTCUSDT": _bar(ts)}, {"BTCUSDT"}, {"positions": {}, "htf": {"15m": {"BTCUSDT": _bar(ts)}, "5m": {}}})
    assert out
    assert out[0].metadata["branch_selected"] == "L1-H1"


def test_l1_h3c_low_vol_selects_reversion_branch(monkeypatch) -> None:
    s = L1H3CHarRegimeSwitchStrategy(q_low=0.2, q_high=0.7, z0=0.0)
    monkeypatch.setattr(s, "_update_regime", lambda *_: None)

    for i in range(20):
        ts = pd.Timestamp("2023-01-01", tz="UTC") + pd.Timedelta(minutes=5 * i)
        s.on_bars(ts, {"BTCUSDT": _bar(ts)}, {"BTCUSDT"}, {"positions": {}, "htf": {"15m": {}, "5m": {"BTCUSDT": _bar(ts)}}})

    st = s._state_for("BTCUSDT")
    st.regime_label = "low_vol_reversion"
    st.regime_branch = "L1-H2"
    st.regime_rv_hat_t = 0.01
    st.regime_rvhat_pct_t = 0.1
    st.regime_rv_payload = {"rv1_t": 0.01, "rv_d": 0.01, "rv_w": 0.01, "rv_m": 0.01}

    ts = pd.Timestamp("2023-01-01 02:00:00", tz="UTC")
    out = s.on_bars(ts, {"BTCUSDT": _bar(ts)}, {"BTCUSDT"}, {"positions": {}, "htf": {"15m": {}, "5m": {"BTCUSDT": _bar(ts)}}})
    assert out
    assert out[0].metadata["branch_selected"] == "L1-H2"


def test_l1_h3c_neutral_regime_emits_no_trade(monkeypatch) -> None:
    s = L1H3CHarRegimeSwitchStrategy()
    monkeypatch.setattr(s, "_update_regime", lambda *_: None)
    st = s._state_for("BTCUSDT")
    st.regime_label = "neutral"
    st.regime_branch = None
    ts = pd.Timestamp("2023-01-01 00:05:00", tz="UTC")
    out = s.on_bars(ts, {"BTCUSDT": _bar(ts)}, {"BTCUSDT"}, {"positions": {}, "htf": {"15m": {"BTCUSDT": _bar(ts)}, "5m": {"BTCUSDT": _bar(ts)}}})
    assert out == []
