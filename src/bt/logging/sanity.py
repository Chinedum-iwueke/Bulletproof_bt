"""Per-run sanity counters and artifact writer."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


@dataclass
class SanityCounters:
    run_id: str
    signals_emitted: int = 0
    signals_approved: int = 0
    signals_rejected: int = 0
    signals_rejected_by_reason: dict[str, int] = field(default_factory=dict)
    signals_approved_by_reason: dict[str, int] = field(default_factory=dict)
    fills_count: int = 0
    closed_trades_count: int = 0
    forced_liquidation_count: int = 0

    def record_decision(self, *, approved: bool, reason: Any) -> None:
        key = str(reason) if reason is not None else "unknown"
        if approved:
            self.signals_approved += 1
            self.signals_approved_by_reason[key] = self.signals_approved_by_reason.get(key, 0) + 1
            return
        self.signals_rejected += 1
        self.signals_rejected_by_reason[key] = self.signals_rejected_by_reason.get(key, 0) + 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "signals_emitted": int(self.signals_emitted),
            "signals_approved": int(self.signals_approved),
            "signals_rejected": int(self.signals_rejected),
            "signals_rejected_by_reason": dict(self.signals_rejected_by_reason),
            "signals_approved_by_reason": dict(self.signals_approved_by_reason),
            "fills_count": int(self.fills_count),
            "closed_trades_count": int(self.closed_trades_count),
            "forced_liquidation_count": int(self.forced_liquidation_count),
        }


def write_sanity_json(run_dir: str | Path, counters: SanityCounters, data_scope: dict[str, Any] | None = None) -> None:
    path = Path(run_dir) / "sanity.json"
    payload = counters.to_dict()
    if isinstance(data_scope, dict):
        payload["data_start_ts"] = data_scope.get("data_start_ts")
        payload["data_end_ts"] = data_scope.get("data_end_ts")
    payload["created_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
