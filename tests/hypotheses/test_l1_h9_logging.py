import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h9_momentum_breakout import L1H9MomentumBreakoutStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, tf: str = "15m") -> dict:
    return {"htf": {tf: {"BTCUSDT": signal_bar}}, "positions": {}}


def test_l1_h9_entry_logs_breakout_diagnostics_fields() -> None:
    strategy = L1H9MomentumBreakoutStrategy(timeframe="15m", adx_min=0.0, breakout_atr_mult=0.1)
    for i in range(40):
        b = _bar(i, 100.0 + i * 0.2, low=99.5 + i * 0.2, high=100.5 + i * 0.2)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))
    out = strategy.on_bars(pd.Timestamp("2024-01-01T01:00:00Z"), {"BTCUSDT": _bar(50, 120.0, low=119.5, high=120.5)}, {"BTCUSDT"}, _ctx(_bar(50, 120.0, low=119.5, high=120.5)))
    assert out
    meta = out[0].metadata
    for key in [
        "breakout_level",
        "breakout_distance_atr",
        "breakout_level_type",
        "breakout_close_strength",
        "breakout_bar_range_atr",
        "trend_strength_adx",
        "ema_fast_entry",
        "ema_slow_entry",
        "ema_spread_pct",
        "signal_timeframe",
        "exit_monitoring_timeframe",
        "stop_distance",
        "stop_price",
        "entry_reference_price",
        "tp1_at_r",
        "post_tp1_lock_r",
        "trail_atr_mult",
        "risk_accounting",
    ]:
        assert key in meta
    assert meta["risk_accounting"] == "engine_canonical_R"
