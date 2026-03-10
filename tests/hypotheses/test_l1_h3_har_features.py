import math
import pandas as pd
import pytest

from bt.indicators.har_rv import HarRVForecaster, bars_per_day, rv1_from_close


def test_rv1_definition() -> None:
    value = rv1_from_close(100.0, 101.0)
    assert value is not None
    assert value == pytest.approx((math.log(101.0 / 100.0)) ** 2)


def test_15m_window_mapping() -> None:
    assert bars_per_day("15m") == 96


def test_har_feature_warmup_requires_30_day_window() -> None:
    f = HarRVForecaster(timeframe="15m", fit_window_days=180)
    ts = pd.Timestamp("2024-01-01", tz="UTC")
    rv_m_seen = []
    for i in range(2900):
        out = f.update(ts + pd.Timedelta(minutes=15 * i), 100 + 0.001 * i)
        rv_m_seen.append(out["rv_m"])
    assert rv_m_seen[2878] is None
    assert rv_m_seen[2880] is not None
