"""Regression tests for timezone-aware dtype detection refactor."""
from __future__ import annotations

import pandas as pd
import pytest

from bt.data.loader import load_bars


def test_tz_aware_series_detected() -> None:
    s = pd.Series(pd.date_range("2020-01-01", periods=5, tz="UTC"))
    assert isinstance(s.dtype, pd.DatetimeTZDtype)


def test_tz_naive_series_not_detected() -> None:
    s = pd.Series(pd.date_range("2020-01-01", periods=5))
    assert not isinstance(s.dtype, pd.DatetimeTZDtype)


def test_loader_validation_rejects_naive_timestamps(tmp_path) -> None:
    csv_path = tmp_path / "bars.csv"
    pd.DataFrame(
        [
            {
                "ts": "2020-01-01 00:00:00",
                "symbol": "AAA",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.0,
                "volume": 10.0,
            }
        ]
    ).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="timezone-aware UTC"):
        load_bars(csv_path)
