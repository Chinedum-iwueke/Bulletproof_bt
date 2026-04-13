import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h9_momentum_breakout import L1H9MomentumBreakoutStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, side: str | None = None, tf: str = "15m") -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {tf: {"BTCUSDT": signal_bar}}, "positions": positions}


def test_l1_h9_requires_closed_htf_bar_and_freezes_stop() -> None:
    strategy = L1H9MomentumBreakoutStrategy(timeframe="15m", adx_min=0.0, breakout_atr_mult=0.1)
    for i in range(40):
        b = _bar(i, 100.0 + i * 0.2, low=99.5 + i * 0.2, high=100.5 + i * 0.2)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))

    trigger = _bar(41, 112.0, low=111.5, high=112.5)
    out = strategy.on_bars(trigger.ts, {"BTCUSDT": trigger}, {"BTCUSDT"}, _ctx(trigger))
    assert out and out[0].side == Side.BUY
    stop_1 = out[0].metadata["stop_price"]

    # no new HTF signal bar on next minute: strategy should not emit a fresh entry
    out2 = strategy.on_bars(trigger.ts + pd.Timedelta(minutes=1), {"BTCUSDT": _bar(42, 112.2)}, {"BTCUSDT"}, {"htf": {"15m": {}}, "positions": {}})
    assert out2 == []
    assert out[0].metadata["stop_price"] == stop_1
