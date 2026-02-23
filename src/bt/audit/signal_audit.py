from __future__ import annotations

from typing import Any
import math


def inspect_signal_context(*, symbol: str, ts: Any, indicators: dict[str, tuple[float | None, bool]]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for name, (value, ready) in indicators.items():
        if ready and value is None:
            violations.append({"type": "ready_with_none", "symbol": symbol, "ts": ts, "feature": name})
        if value is not None and isinstance(value, float) and math.isnan(value):
            violations.append({"type": "nan_feature", "symbol": symbol, "ts": ts, "feature": name})
    return violations
