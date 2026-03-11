import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h3c_har_regime_switch import L1H3CHarRegimeSwitchStrategy


def _bar(ts: pd.Timestamp, close: float = 100.0, *, low: float | None = None, high: float | None = None) -> Bar:
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2 if high is None else high, low=close - 0.2 if low is None else low, close=close, volume=1000)


def test_l1_h3c_trend_branch_requires_completed_15m_bar(monkeypatch) -> None:
    s = L1H3CHarRegimeSwitchStrategy(z0=0.0)
    st = s._state_for("BTCUSDT")
    st.regime_label = "high_vol_trend"
    st.regime_branch = "L1-H1"
    st.regime_rv_hat_t = 0.01
    st.regime_rvhat_pct_t = 0.9
    st.regime_rv_payload = {"rv1_t": 0.01, "rv_d": 0.01, "rv_w": 0.01, "rv_m": 0.01}
    st.ema_fast._ema.value = 101
    st.ema_slow._ema.value = 100
    monkeypatch.setattr(s, "_update_regime", lambda *_: None)

    ts = pd.Timestamp("2023-01-01 00:01:00", tz="UTC")
    out = s.on_bars(ts, {"BTCUSDT": _bar(ts)}, {"BTCUSDT"}, {"positions": {}, "htf": {"15m": {}, "5m": {}}})
    assert out == []


def test_l1_h3c_reversion_branch_requires_completed_5m_bar(monkeypatch) -> None:
    s = L1H3CHarRegimeSwitchStrategy(z0=0.0)
    st = s._state_for("BTCUSDT")
    st.regime_label = "low_vol_reversion"
    st.regime_branch = "L1-H2"
    st.regime_rv_hat_t = 0.01
    st.regime_rvhat_pct_t = 0.1
    st.regime_rv_payload = {"rv1_t": 0.01, "rv_d": 0.01, "rv_w": 0.01, "rv_m": 0.01}
    st.atr_signal._rma.value = 1.0
    st.signal_vwap._cum_pv = 1000.0
    st.signal_vwap._cum_vol = 10.0
    monkeypatch.setattr(s, "_update_regime", lambda *_: None)

    ts = pd.Timestamp("2023-01-01 00:01:00", tz="UTC")
    out = s.on_bars(ts, {"BTCUSDT": _bar(ts)}, {"BTCUSDT"}, {"positions": {}, "htf": {"15m": {}, "5m": {}}})
    assert out == []


def test_l1_h3c_frozen_stop_not_recomputed() -> None:
    s = L1H3CHarRegimeSwitchStrategy()
    st = s._state_for("BTCUSDT")
    st.active_branch = "L1-H1"
    st.stop_price_frozen = 99.0
    st.stop_distance_frozen = 1.0
    st.rv_hat_entry = 0.01
    st.signal_bars_held = 0

    ts = pd.Timestamp("2023-01-01 00:01:00", tz="UTC")
    hold = s.on_bars(ts, {"BTCUSDT": _bar(ts, 100.0, low=99.5)}, {"BTCUSDT"}, {"positions": {"BTCUSDT": {"side": "buy"}}, "htf": {"15m": {}, "5m": {}}})
    assert hold == []

    ts2 = pd.Timestamp("2023-01-01 00:02:00", tz="UTC")
    hit = s.on_bars(ts2, {"BTCUSDT": _bar(ts2, 100.0, low=98.9)}, {"BTCUSDT"}, {"positions": {"BTCUSDT": {"side": "buy"}}, "htf": {"15m": {}, "5m": {}}})
    assert hit
    assert hit[0].metadata["exit_reason"] == "rvhat_stop"
    assert float(hit[0].metadata["stop_price"]) == 99.0
