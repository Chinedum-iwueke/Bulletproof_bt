from __future__ import annotations

from typing import Any


def inspect_fill(*, ts: Any, fill: Any, bar: Any | None) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    if bar is not None:
        if not (bar.low <= fill.price <= bar.high):
            violations.append({"type": "fill_outside_bar", "symbol": fill.symbol, "ts": ts, "price": fill.price, "bar_low": bar.low, "bar_high": bar.high})
        stop_hit = bool(fill.metadata.get("stop_touched")) if isinstance(fill.metadata, dict) else False
        tp_hit = bool(fill.metadata.get("tp_touched")) if isinstance(fill.metadata, dict) else False
        if stop_hit and tp_hit:
            violations.append({"type": "stop_tp_same_bar", "resolution": str(fill.metadata.get("stop_tp_resolution", "unknown"))})
    return violations
