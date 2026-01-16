"""Core data types for the backtesting engine."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side


def _ensure_utc(ts: pd.Timestamp, field_name: str) -> None:
    if ts.tz is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if str(ts.tz) != "UTC":
        raise ValueError(f"{field_name} must be in UTC")


@dataclass(frozen=True)
class Bar:
    ts: pd.Timestamp
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be <= min(open, close)")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be >= max(open, close)")
        if self.high < self.low:
            raise ValueError("high must be >= low")
        if self.volume < 0:
            raise ValueError("volume must be >= 0")


@dataclass(frozen=True)
class Signal:
    ts: pd.Timestamp
    symbol: str
    side: Optional[Side]
    signal_type: str
    confidence: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be within [0, 1]")


@dataclass(frozen=True)
class OrderIntent:
    ts: pd.Timestamp
    symbol: str
    side: Side
    qty: float
    order_type: OrderType
    limit_price: Optional[float]
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class Order:
    id: str
    ts_submitted: pd.Timestamp
    symbol: str
    side: Side
    qty: float
    order_type: OrderType
    limit_price: Optional[float]
    state: OrderState
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts_submitted, "ts_submitted")


@dataclass(frozen=True)
class Fill:
    order_id: str
    ts: pd.Timestamp
    symbol: str
    side: Side
    qty: float
    price: float
    fee: float
    slippage: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass
class Position:
    symbol: str
    state: PositionState
    side: Optional[Side]
    qty: float
    avg_entry_price: float
    realized_pnl: float
    unrealized_pnl: float
    mae_price: Optional[float]
    mfe_price: Optional[float]
    opened_ts: Optional[pd.Timestamp]
    closed_ts: Optional[pd.Timestamp]

    def __post_init__(self) -> None:
        if self.opened_ts is not None:
            _ensure_utc(self.opened_ts, "opened_ts")
        if self.closed_ts is not None:
            _ensure_utc(self.closed_ts, "closed_ts")


@dataclass(frozen=True)
class Trade:
    symbol: str
    side: Side
    entry_ts: pd.Timestamp
    exit_ts: pd.Timestamp
    entry_price: float
    exit_price: float
    qty: float
    pnl: float
    fees: float
    slippage: float
    mae_price: Optional[float]
    mfe_price: Optional[float]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.entry_ts, "entry_ts")
        _ensure_utc(self.exit_ts, "exit_ts")
