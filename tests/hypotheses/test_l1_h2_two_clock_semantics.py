import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h2_compression_mean_reversion import L1H2CompressionMeanReversionStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, side: Side | None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side.value.lower()}}
    return {"htf": {"5m": {"BTCUSDT": signal_bar}}, "positions": positions}


def _entry(strategy: L1H2CompressionMeanReversionStrategy) -> tuple[object, int]:
    for i in range(20):
        b = _bar(i, 100.0)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, None))
    st = strategy._state_for("BTCUSDT")
    st.gate._history.extend([0.03] * st.gate._history.maxlen)
    b = _bar(21, 98.0)
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, None))
    return out[0], 21


def test_frozen_stop_does_not_move_when_atr_changes() -> None:
    strategy = L1H2CompressionMeanReversionStrategy(timeframe="5m", k_atr=1.5)
    entry, minute = _entry(strategy)
    frozen = float(entry.metadata["stop_price"])

    for i in range(minute + 1, minute + 5):
        b = _bar(i, 98.5, low=98.1, high=98.7)
        assert strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, Side.BUY)) == []

    hit = _bar(minute + 5, 98.5, low=frozen - 0.01, high=98.7)
    out = strategy.on_bars(hit.ts, {"BTCUSDT": hit}, {"BTCUSDT"}, _ctx(hit, Side.BUY))
    assert out and out[0].metadata["exit_reason"] == "atr_stop"
    assert float(out[0].metadata["stop_price"]) == frozen


def test_vwap_touch_exit_is_monitored_on_base_1m() -> None:
    strategy = L1H2CompressionMeanReversionStrategy(timeframe="5m")
    entry, minute = _entry(strategy)
    assert entry.metadata["exit_monitoring_timeframe"] == "1m"

    b = _bar(minute + 1, 101.0)
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, Side.BUY))
    assert out and out[0].metadata["exit_reason"] == "vwap_touch"


def test_t_hold_counts_signal_bars_only() -> None:
    strategy = L1H2CompressionMeanReversionStrategy(timeframe="5m", T_hold=2)
    _, minute = _entry(strategy)

    # same signal ts repeatedly -> no completed signal bars
    same_signal = _bar(minute, 99.0)
    for i in range(minute + 1, minute + 4):
        b = _bar(i, 99.0)
        assert strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(same_signal, Side.BUY)) == []

    sig1 = _bar(minute + 5, 99.0)
    assert strategy.on_bars(sig1.ts, {"BTCUSDT": sig1}, {"BTCUSDT"}, _ctx(sig1, Side.BUY)) == []
    sig2 = _bar(minute + 10, 99.0)
    out = strategy.on_bars(sig2.ts, {"BTCUSDT": sig2}, {"BTCUSDT"}, _ctx(sig2, Side.BUY))
    assert out and out[0].metadata["exit_reason"] == "time_stop"
    assert out[0].metadata["hold_time_unit"] == "signal_bars"
