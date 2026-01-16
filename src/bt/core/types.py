"""Core data types for the backtesting engine."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __post_init__(self) -> None:
        assert self.high >= max(self.open, self.close)
        assert self.low <= min(self.open, self.close)
        assert self.high >= self.low


@dataclass(frozen=True)
class Signal:
    timestamp: datetime
    symbol: str
    side: str  # "buy" or "sell"
    strength: float = 1.0


@dataclass(frozen=True)
class Order:
    timestamp: datetime
    symbol: str
    side: str
    quantity: float


@dataclass(frozen=True)
class Fill:
    timestamp: datetime
    symbol: str
    side: str
    quantity: float
    price: float
    fee: float


@dataclass
class Position:
    symbol: str
    quantity: float = 0.0
    avg_price: Optional[float] = None
