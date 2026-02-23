from __future__ import annotations

import pandas as pd

from bt.audit.data_audit import run_data_audit
from bt.audit.signal_audit import inspect_signal_context
from bt.audit.order_audit import inspect_order
from bt.audit.fill_audit import inspect_fill
from bt.audit.position_audit import inspect_realized_transition
from bt.audit.determinism import compare_hashes
from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Fill, Order
from bt.data.resample import TimeframeResampler


def test_data_audit_catches_duplicate_timestamps_and_invalid_ohlc() -> None:
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    df = pd.DataFrame(
        [
            {"ts": ts, "symbol": "BTC", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1.0},
            {"ts": ts, "symbol": "ETH", "open": 10.0, "high": 9.5, "low": 9.0, "close": 9.7, "volume": 1.0},
        ]
    )
    report = run_data_audit(df)
    assert report["passed"] is False
    types = {v["type"] for v in report["violations"]}
    assert "duplicate_timestamp" in types
    assert "invalid_ohlc" in types


def test_resample_audit_detects_incomplete_bucket_usage() -> None:
    resampler = TimeframeResampler(timeframes=["5m"], strict=False)
    t0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    bars = [
        Bar(ts=t0, symbol="BTC", open=1, high=1, low=1, close=1, volume=1),
        Bar(ts=t0 + pd.Timedelta(minutes=1), symbol="BTC", open=1, high=1, low=1, close=1, volume=1),
        Bar(ts=t0 + pd.Timedelta(minutes=4), symbol="BTC", open=1, high=1, low=1, close=1, volume=1),
        Bar(ts=t0 + pd.Timedelta(minutes=5), symbol="BTC", open=1, high=1, low=1, close=1, volume=1),
    ]
    emitted = []
    for bar in bars:
        emitted.extend(resampler.update(bar))
    assert emitted
    assert emitted[0].is_complete is False


def test_signal_audit_catches_synthetic_lookahead_proxy_bug() -> None:
    violations = inspect_signal_context(
        symbol="BTC",
        ts=pd.Timestamp("2024-01-01 00:10:00", tz="UTC"),
        indicators={"bad_feature": (float("nan"), True)},
    )
    assert any(v["type"] == "nan_feature" for v in violations)


def test_order_audit_catches_min_notional_and_missing_price_reference() -> None:
    order = Order(
        id="o1",
        ts_submitted=pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
        symbol="BTC",
        side=Side.BUY,
        qty=0.001,
        order_type=OrderType.MARKET,
        limit_price=10.0,
        state=OrderState.NEW,
        metadata={},
    )
    _, violations = inspect_order(ts=order.ts_submitted, order=order, min_notional=100.0)
    types = {v["type"] for v in violations}
    assert "min_notional_violation" in types
    assert "missing_price_reference" in types


def test_fill_audit_catches_stop_tp_same_bar_ambiguity() -> None:
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    fill = Fill(
        order_id="o1",
        ts=ts,
        symbol="BTC",
        side=Side.BUY,
        qty=1.0,
        price=10.0,
        fee=0.0,
        slippage=0.0,
        metadata={"stop_touched": True, "tp_touched": True, "stop_tp_resolution": "stop_first"},
    )
    bar = Bar(ts=ts, symbol="BTC", open=10, high=11, low=9, close=10, volume=1)
    violations = inspect_fill(ts=ts, fill=fill, bar=bar)
    assert any(v["type"] == "stop_tp_same_bar" for v in violations)


def test_position_audit_catches_cross_through_zero_double_realization() -> None:
    violations = inspect_realized_transition(prev_qty=1.0, next_qty=-0.5, realized_delta=10.0, closed_trades=2)
    assert any(v["type"] == "cross_zero_close_count" for v in violations)


def test_determinism_compare_hashes_passes_for_identical_runs() -> None:
    left = {"fills.jsonl": "abc", "equity.csv": "def"}
    right = {"fills.jsonl": "abc", "equity.csv": "def"}
    report = compare_hashes(left, right)
    assert report["passed"] is True
    assert report["mismatches"] == {}
