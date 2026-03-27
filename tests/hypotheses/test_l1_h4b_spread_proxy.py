import pandas as pd

from bt.core.types import Bar
from bt.hypotheses.l1_h4b import RollingMedianReference, RollingQuantileGate, spread_proxy_from_bar


def _bar(i: int, close: float, *, low: float, high: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=high, low=low, close=close, volume=1000)


def test_spread_proxy_formula() -> None:
    bar = _bar(0, 100.0, low=99.0, high=101.0)
    assert spread_proxy_from_bar(bar) == 0.01


def test_quantile_gate_and_rolling_median_are_causal() -> None:
    gate = RollingQuantileGate(lookback_bars=3, q=0.6)
    ref = RollingMedianReference(lookback_bars=3)
    assert gate.update(0.02) == (None, None)
    assert ref.update(0.02) is None
    assert gate.update(0.03) == (None, None)
    assert ref.update(0.03) is None
    assert gate.update(0.01) == (None, None)
    assert ref.update(0.01) is None

    threshold, passed = gate.update(0.025)
    assert threshold is not None and round(threshold, 6) == 0.022
    assert passed is False
    median_ref = ref.update(0.025)
    assert median_ref == 0.02
