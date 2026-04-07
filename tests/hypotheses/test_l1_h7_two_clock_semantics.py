import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h7_squeeze_expansion_pullback import L1H7SqueezeExpansionPullbackStrategy


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2, low=close - 0.2, close=close, volume=1000)


def test_entries_only_on_new_completed_signal_bar() -> None:
    s = L1H7SqueezeExpansionPullbackStrategy(timeframe="15m", adx_min=0.0)
    b = _bar(1, 100.0)
    s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"htf": {"15m": {"BTCUSDT": b}}, "positions": {}})
    out = s.on_bars(_bar(2, 100.1).ts, {"BTCUSDT": _bar(2, 100.1)}, {"BTCUSDT"}, {"htf": {"15m": {"BTCUSDT": b}}, "positions": {}})
    assert out == []


def test_exit_monitoring_runs_on_1m_bars() -> None:
    s = L1H7SqueezeExpansionPullbackStrategy(timeframe="15m", adx_min=0.0)
    st = s._state_for("BTCUSDT")
    st.base_position = None
    st.entry_price = 100.0
    st.stop_price_frozen = 99.0
    st.stop_distance_frozen = 1.0
    st.atr_entry = 0.5
    st.tp1_target_price = 101.0
    st.base_position = None

    st2 = s._state_for("BTCUSDT")
    st2.entry_price = 100.0
    st2.stop_price_frozen = 99.0
    st2.stop_distance_frozen = 1.0
    st2.atr_entry = 0.5
    st2.tp1_target_price = 101.0
    st2.base_position = None

    bar = _bar(3, 98.9)
    out = s.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, {"htf": {"15m": {}}, "positions": {"BTCUSDT": {"side": "buy"}}})
    assert out and out[0].metadata["exit_monitoring_timeframe"] == "1m"
