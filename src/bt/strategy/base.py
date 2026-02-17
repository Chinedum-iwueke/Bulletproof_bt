"""Strategy interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from bt.core.types import Bar, Signal


class Strategy(ABC):
    @abstractmethod
    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        """
        Called once per timestamp.
        bars_by_symbol includes only symbols that have a bar at ts (gaps preserved).
        tradeable is the universe membership at ts.
        Return a list of Signals (intent only).
        """

    @classmethod
    def smoke_config_overrides(cls) -> dict[str, Any]:
        """Optional integration-smoke config overrides for this strategy."""
        return {}

    @classmethod
    def smoke_assert(cls, run_dir: Path) -> None:
        """Optional integration-smoke assertions for this strategy."""
        _ = run_dir

    @classmethod
    def smoke_requires_stops(cls) -> bool:
        """Whether non-close_only fills are expected to carry stop metrics."""
        return True

