from __future__ import annotations

from typing import Any


def inspect_alignment(*, ts: Any, bars_by_symbol: dict[str, Any]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for symbol, bar in bars_by_symbol.items():
        if bar.ts != ts:
            violations.append({"type": "cross_asset_timestamp_mismatch", "symbol": symbol, "bar_ts": bar.ts, "engine_ts": ts})
            break
    return violations
