import pandas as pd

from bt.indicators.har_rv import HarRVForecaster


def test_refit_cadence_is_daily_and_deterministic() -> None:
    f1 = HarRVForecaster(timeframe="15m", fit_window_days=180)
    f2 = HarRVForecaster(timeframe="15m", fit_window_days=180)
    start = pd.Timestamp("2023-01-01", tz="UTC")
    for i in range(96 * 220):
        ts = start + pd.Timedelta(minutes=15 * i)
        close = 100.0 + (i % 11) * 0.01
        f1.update(ts, close)
        f2.update(ts, close)
    assert f1.fit_history
    assert f1.fit_history == f2.fit_history

    days = [row.fit_ts.normalize() for row in f1.fit_history]
    assert len(days) == len(set(days))


def test_forecast_uses_prior_fit_only_after_warmup() -> None:
    f = HarRVForecaster(timeframe="15m", fit_window_days=180)
    start = pd.Timestamp("2023-01-01", tz="UTC")
    first_forecast_ts = None
    for i in range(96 * 240):
        ts = start + pd.Timedelta(minutes=15 * i)
        out = f.update(ts, 100.0 + 0.001 * i)
        if out["rv_hat_t"] is not None:
            first_forecast_ts = ts
            break
    assert first_forecast_ts is not None
    assert f.fit_history
    assert f.fit_history[-1].fit_ts <= first_forecast_ts
