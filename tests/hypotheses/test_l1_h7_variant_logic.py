import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h7_squeeze_expansion_pullback import L1H7SqueezeExpansionPullbackStrategy


def _bar(i: int, close: float, *, high: float | None = None, low: float | None = None, open_: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close if open_ is None else open_, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, tf: str = "15m") -> dict:
    return {"htf": {tf: {"BTCUSDT": signal_bar}}, "positions": {}}


def _prime(s: L1H7SqueezeExpansionPullbackStrategy, tf: str = "15m") -> None:
    for i in range(40):
        b = _bar(i, 100.0 + (0.02 if i % 2 else -0.01), high=100.15, low=99.85)
        s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, tf))


def test_h7d_runner_mode_and_h7e_flow_mode_are_logged() -> None:
    s = L1H7SqueezeExpansionPullbackStrategy(timeframe="15m", adx_min=0.0, runner_mode="DRAGON", runner_flow_mode="strict", sigma_z_threshold=1.0, pullback_ema_period=3, pullback_use_session_vwap=False)
    _prime(s)
    st = s._state_for("BTCUSDT")
    st.squeeze_qualified = True
    b1 = _bar(50, 105.0, high=105.1, low=104.8)
    s.on_bars(b1.ts, {"BTCUSDT": b1}, {"BTCUSDT"}, _ctx(b1))
    st.expansion_direction = Side.BUY
    b2 = _bar(51, 104.9, high=105.0, low=100.0)
    out = s.on_bars(b2.ts, {"BTCUSDT": b2}, {"BTCUSDT"}, _ctx(b2))
    assert out
    assert out[0].metadata["runner_mode"] == "DRAGON"
    assert out[0].metadata["runner_flow_mode"] == "strict"


def test_h7f_strict_flow_gate_can_block_entry() -> None:
    s = L1H7SqueezeExpansionPullbackStrategy(timeframe="15m", adx_min=0.0, entry_flow_gate="strict", imbalance_threshold="strict", pullback_ema_period=3, pullback_use_session_vwap=False)
    _prime(s)
    st = s._state_for("BTCUSDT")
    st.squeeze_qualified = True
    b1 = _bar(60, 105.0, high=105.2, low=104.8)
    s.on_bars(b1.ts, {"BTCUSDT": b1}, {"BTCUSDT"}, _ctx(b1))
    st.expansion_direction = Side.BUY
    # weak directional imbalance => blocked under strict gate
    b2 = _bar(61, 104.9, open_=104.89, high=105.0, low=100.0)
    out = s.on_bars(b2.ts, {"BTCUSDT": b2}, {"BTCUSDT"}, _ctx(b2))
    assert out == []
