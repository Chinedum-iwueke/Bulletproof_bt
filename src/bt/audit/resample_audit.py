from __future__ import annotations

from dataclasses import asdict
from typing import Any


def make_resample_event(*, symbol: str, timeframe: str, ts: Any, n_bars: int, expected_bars: int, complete: bool) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "ts": ts,
        "n_bars": n_bars,
        "expected_bars": expected_bars,
        "complete": complete,
    }
