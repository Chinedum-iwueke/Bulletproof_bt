"""Indicator interfaces."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

import pandas as pd

from bt.core.types import Bar


class Indicator(Protocol):
    name: str

    def update(self, bar: Bar) -> None:
        ...

    @property
    def is_ready(self) -> bool:
        ...

    @property
    def value(self) -> float | None:
        ...


@dataclass
class IndicatorState:
    value: float | None
    is_ready: bool
