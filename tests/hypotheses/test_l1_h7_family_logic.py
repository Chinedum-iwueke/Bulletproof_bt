import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h7_squeeze_expansion_pullback import L1H7SqueezeExpansionPullbackStrategy


def _bar(i: int, close: float, *, high: float | None = None, low: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, side: str | None = None, tf: str = "15m") -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {tf: {"BTCUSDT": signal_bar}}, "positions": positions}


def _prime(strategy: L1H7SqueezeExpansionPullbackStrategy, tf: str = "15m") -> None:
    for i in range(30):
        b = _bar(i, 100.0 + (0.01 if i % 2 else -0.01), high=100.1, low=99.9)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, tf=tf))


def test_entry_requires_expansion_then_pullback() -> None:
    strategy = L1H7SqueezeExpansionPullbackStrategy(timeframe="15m", squeeze_min_bars=2, adx_min=0.0, pullback_ema_period=3, pullback_use_session_vwap=False)
    _prime(strategy)
    st = strategy._state_for("BTCUSDT")
    st.squeeze_qualified = True

    expand = _bar(31, 105.0, high=105.2, low=104.8)
    strategy.on_bars(expand.ts, {"BTCUSDT": expand}, {"BTCUSDT"}, _ctx(expand))
    st.expansion_direction = Side.BUY

    pullback = _bar(32, 104.9, high=105.0, low=101.0)
    out = strategy.on_bars(pullback.ts, {"BTCUSDT": pullback}, {"BTCUSDT"}, _ctx(pullback))
    assert out
    assert out[0].signal_type == "l1_h7_entry"
    assert out[0].metadata["pullback_max_wait"] == 12


def test_pullback_window_expiry_blocks_late_entry() -> None:
    strategy = L1H7SqueezeExpansionPullbackStrategy(timeframe="15m", squeeze_min_bars=2, adx_min=0.0, pullback_max_wait=1, pullback_ema_period=3, pullback_use_session_vwap=False)
    _prime(strategy)
    st = strategy._state_for("BTCUSDT")
    st.squeeze_qualified = True

    expand = _bar(31, 105.0, high=105.2, low=104.8)
    strategy.on_bars(expand.ts, {"BTCUSDT": expand}, {"BTCUSDT"}, _ctx(expand))
    st.expansion_direction = Side.BUY

    neutral = _bar(32, 106.0, high=106.2, low=105.8)
    strategy.on_bars(neutral.ts, {"BTCUSDT": neutral}, {"BTCUSDT"}, _ctx(neutral))
    too_late = _bar(33, 104.6, high=105.0, low=103.8)
    out = strategy.on_bars(too_late.ts, {"BTCUSDT": too_late}, {"BTCUSDT"}, _ctx(too_late))
    assert out == []
