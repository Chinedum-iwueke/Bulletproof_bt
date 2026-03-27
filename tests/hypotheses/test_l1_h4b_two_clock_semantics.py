import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h4b_liquidity_gate_size_adjusted_mean_reversion import L1H4BLiquidityGateSizeAdjustedMeanReversionStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, side: Side | None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side.value.lower()}}
    return {"htf": {"5m": {"BTCUSDT": signal_bar}}, "positions": positions}


def _entry(strategy: L1H4BLiquidityGateSizeAdjustedMeanReversionStrategy) -> tuple[object, int]:
    for i in range(20):
        b = _bar(i, 100.0, high=100.2, low=99.8)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, None))
    st = strategy._state_for("BTCUSDT")
    st.gate._history.extend([0.02] * st.gate._history.maxlen)
    st.spread_ref._history.extend([0.01] * st.spread_ref._history.maxlen)
    b = _bar(21, 98.0, high=98.2, low=97.8)
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, None))
    return out[0], 21


def test_entry_on_completed_signal_bar_only() -> None:
    strategy = L1H4BLiquidityGateSizeAdjustedMeanReversionStrategy(timeframe="5m")
    entry, _ = _entry(strategy)
    assert entry.metadata["signal_timeframe"] == "5m"
    b = _bar(22, 98.0)
    assert strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(_bar(21, 98.0), None)) == []


def test_stop_and_time_stop_semantics_preserved() -> None:
    strategy = L1H4BLiquidityGateSizeAdjustedMeanReversionStrategy(timeframe="5m", T_hold=2)
    entry, minute = _entry(strategy)
    frozen = float(entry.metadata["stop_price"])

    hit = _bar(minute + 1, 98.5, low=frozen - 0.01, high=98.7)
    out_stop = strategy.on_bars(hit.ts, {"BTCUSDT": hit}, {"BTCUSDT"}, _ctx(hit, Side.BUY))
    assert out_stop and out_stop[0].metadata["exit_reason"] == "atr_stop"

    strategy2 = L1H4BLiquidityGateSizeAdjustedMeanReversionStrategy(timeframe="5m", T_hold=2)
    _, minute2 = _entry(strategy2)
    sig1 = _bar(minute2 + 5, 99.0)
    assert strategy2.on_bars(sig1.ts, {"BTCUSDT": sig1}, {"BTCUSDT"}, _ctx(sig1, Side.BUY)) == []
    sig2 = _bar(minute2 + 10, 99.0)
    out_time = strategy2.on_bars(sig2.ts, {"BTCUSDT": sig2}, {"BTCUSDT"}, _ctx(sig2, Side.BUY))
    assert out_time and out_time[0].metadata["exit_reason"] == "time_stop"
    assert out_time[0].metadata["hold_time_unit"] == "signal_bars"
