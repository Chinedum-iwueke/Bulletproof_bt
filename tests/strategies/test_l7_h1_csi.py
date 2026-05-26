import json

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l7_h1_csi_gated_displacement_trend import L7H1CSIGatedDisplacementTrendStrategy


def _bar(i: int, open_: float, high: float, low: float, close: float, *, funding: float, oi: float, volume: float = 1000.0) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        extra={
            "mark_close": close * 1.001,
            "index_close": close,
            "funding_rate": funding,
            "funding_source_ts": ts,
            "open_interest": oi,
            "oi_source_ts": ts,
            "basis_close_vs_index": 0.001,
        },
    )


def _ctx(signal_bar: Bar, *, side: str | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {"15m": {"BTCUSDT": signal_bar}}, "positions": positions}


def test_l7_h1_enters_on_displacement_and_csi_gates_with_required_log_fields() -> None:
    strategy = L7H1CSIGatedDisplacementTrendStrategy(
        timeframe="15m",
        atr_period=3,
        d0=1.4,
        theta=0.65,
        theta_low=0.4,
        k_stop=2.0,
        k_trail=2.0,
    )

    warmup = [
        _bar(0, 100.0, 100.5, 99.5, 100.0, funding=0.0001, oi=1000.0),
        _bar(15, 100.0, 100.5, 99.5, 100.1, funding=0.0002, oi=1010.0),
        _bar(30, 100.1, 100.6, 99.6, 100.2, funding=0.0003, oi=1030.0),
        _bar(45, 100.2, 100.7, 99.7, 100.3, funding=0.0004, oi=1060.0),
        _bar(60, 100.3, 100.8, 99.8, 100.4, funding=0.0005, oi=1100.0),
    ]
    for bar in warmup:
        strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(bar))

    event = _bar(75, 100.4, 106.0, 100.2, 105.8, funding=0.0020, oi=1400.0, volume=2500.0)
    out = strategy.on_bars(event.ts, {"BTCUSDT": event}, {"BTCUSDT"}, _ctx(event))

    entries = [signal for signal in out if signal.signal_type == "l7_h1_entry"]
    assert entries
    entry = entries[0]
    assert entry.side == Side.BUY
    meta = entry.metadata
    assert meta["D_t"] >= 1.4
    assert meta["CSI"] >= 0.65

    for key in [
        "strategy",
        "signal_timeframe",
        "exit_monitoring_timeframe",
        "entry_price",
        "stop_distance",
        "stop_model",
        "trailing_stop",
        "D_t",
        "ATR_14",
        "CSI",
        "funding_pct",
        "basis_pct",
        "oi_z",
        "volume_z",
        "S_t",
        "entry_reason",
        "state_vector",
        "entry_state_csi_pctile",
        "entry_state_csi_components_json",
    ]:
        assert key in meta
    state_vector = json.loads(meta["state_vector"])
    assert {"CSI", "ATR_14", "D_t", "S_t", "oi_z", "funding_pct"}.issubset(state_vector)


def test_l7_h1_chandelier_exit_monitors_one_minute_bars() -> None:
    strategy = L7H1CSIGatedDisplacementTrendStrategy(timeframe="15m", atr_period=3, k_stop=2.0, k_trail=2.0)
    state = strategy._state_for("BTCUSDT")
    state.position = Side.BUY
    state.entry_price = 100.0
    state.atr_entry = 1.0
    state.stop_distance = 2.0
    state.stop_price = 98.0
    state.trailing_stop = 98.0
    state.high_since_entry = 101.0
    state.low_since_entry = 100.0
    state.last_features = {"CSI": 0.8, "D_t": 2.0, "ATR_14": 1.0, "state_vector": "{}"}

    bar = _bar(1, 99.0, 99.2, 97.9, 98.1, funding=0.001, oi=1000.0)
    out = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, {"htf": {"15m": {}}, "positions": {"BTCUSDT": {"side": "buy"}}})

    assert out
    assert out[0].signal_type == "l7_h1_exit"
    assert out[0].metadata["exit_monitoring_timeframe"] == "1m"
    assert out[0].metadata["exit_reason"] == "chandelier_trailing_stop"
