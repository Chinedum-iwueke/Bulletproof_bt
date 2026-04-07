import pandas as pd

from bt.core.types import Bar
from bt.indicators.squeeze import BBKCSqueeze


def _bar(i: int, close: float, *, high: float | None = None, low: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def test_squeeze_indicator_warmup_and_duration() -> None:
    ind = BBKCSqueeze(bb_period=3, bb_std_mult=1.0, kc_period=3, kc_atr_mult=3.0)
    for i, c in enumerate([100.0, 100.0, 100.0, 100.0]):
        ind.update(_bar(i, c, high=100.1, low=99.9))
    assert ind.values["squeeze_on"] is True
    assert ind.values["squeeze_duration"] == 1

    assert ind.values["squeeze_on"] is True
    assert ind.values["squeeze_duration"] >= 1


def test_squeeze_turns_off_when_bb_exits_kc() -> None:
    ind = BBKCSqueeze(bb_period=3, bb_std_mult=2.0, kc_period=3, kc_atr_mult=0.5)
    for i, c in enumerate([100.0, 100.1, 99.9, 104.0]):
        ind.update(_bar(i, c, high=max(c, 104.0), low=min(c, 96.0)))
    assert ind.values["squeeze_on"] is False
    assert ind.values["squeeze_duration"] == 0
