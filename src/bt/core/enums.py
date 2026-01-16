"""Enum definitions for the backtesting engine."""
from enum import Enum


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class OrderState(str, Enum):
    NEW = "new"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class PositionState(str, Enum):
    FLAT = "flat"
    OPENING = "opening"
    OPEN = "open"
    REDUCING = "reducing"
    CLOSED = "closed"


class IntrabarMode(str, Enum):
    WORST_CASE = "worst_case"
    BEST_CASE = "best_case"
    RANDOMIZED = "randomized"
