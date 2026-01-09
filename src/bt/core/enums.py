"""Enum definitions for order and position states."""
from enum import Enum


class OrderState(str, Enum):
    NEW = "new"
    FILLED = "filled"
    REJECTED = "rejected"


class PositionState(str, Enum):
    FLAT = "flat"
    OPEN = "open"
