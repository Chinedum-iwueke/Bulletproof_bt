import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h2_compression_mean_reversion import L1H2CompressionMeanReversionStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, *, side: str | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {"5m": {"BTCUSDT": signal_bar}}, "positions": positions}


def test_no_entry_before_indicators_and_history_ready() -> None:
    strategy = L1H2CompressionMeanReversionStrategy(timeframe="5m")
    seen = []
    for i in range(20):
        b = _bar(i, 100.0 + i * 0.01)
        seen.extend(strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b)))
    assert seen == []


def test_compression_gate_and_zvwap_generate_fade_entry() -> None:
    strategy = L1H2CompressionMeanReversionStrategy(timeframe="5m", q_comp=0.2, z0=0.8, k_atr=1.5)
    # Warm ATR + session VWAP.
    for i in range(20):
        b = _bar(i, 100.0)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))

    st = strategy._state_for("BTCUSDT")
    st.gate._history.extend([0.03] * st.gate._history.maxlen)

    trigger = _bar(21, 98.0, high=98.0, low=98.0)
    out = strategy.on_bars(trigger.ts, {"BTCUSDT": trigger}, {"BTCUSDT"}, _ctx(trigger))
    assert out
    assert out[0].signal_type == "l1_h2_compression_mean_reversion"
    assert out[0].metadata["comp_gate_t"] is True
    assert out[0].metadata["entry_reason"] == "compression_fade_long"
    assert out[0].metadata["vwap_mode"] == "session"
