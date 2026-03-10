import pandas as pd

from bt.core.types import Bar
from bt.data.resample import TimeframeResampler
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.strategy.l1_h3_har_rv_gate_trend import L1H3HarRVGateTrendStrategy


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2023-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2, low=close - 0.2, close=close, volume=1000)


def test_signal_waits_for_ema_and_har_readiness() -> None:
    s = HTFContextStrategyAdapter(
        inner=L1H3HarRVGateTrendStrategy(timeframe="15m", gate_quantile=0.0, fit_window_days=10),
        resampler=TimeframeResampler(timeframes=["15m"], strict=True),
    )
    seen = []
    for i in range(1000):
        b = _bar(i, 100 + i * 0.01)
        seen.extend(s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"positions": {}}))
    assert seen == []


def test_har_gate_and_trend_emit_signal_after_warmup() -> None:
    s = HTFContextStrategyAdapter(
        inner=L1H3HarRVGateTrendStrategy(timeframe="15m", gate_quantile=0.0, fit_window_days=10),
        resampler=TimeframeResampler(timeframes=["15m"], strict=True),
    )
    seen = []
    for i in range(60000):
        b = _bar(i, 100 + i * 0.001)
        seen.extend(s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"positions": {}}))
    assert seen
    assert seen[-1].metadata["gate_pass"] is True
    assert seen[-1].metadata["trend_dir_t"] in (1, -1)
    assert seen[-1].metadata["RV_hat_t"] is not None
