"""Per-run sanity counters and artifact writer."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bt.logging.formatting import write_json_deterministic


@dataclass
class SanityCounters:
    run_id: str
    signals_emitted: int = 0
    signals_approved: int = 0
    signals_rejected: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=lambda: {"stop_too_small": 0})
    approved_by_reason: dict[str, int] = field(default_factory=dict)
    fills: int = 0
    closed_trades: int = 0
    forced_liquidations: int = 0

    @staticmethod
    def _normalize_reason(*, approved: bool, reason: Any) -> str:
        key = str(reason) if reason is not None else "unknown"
        if approved:
            return key
        prefix = "risk_rejected:"
        if key.startswith(prefix):
            return key[len(prefix) :].split(":", 1)[0]
        return key

    def record_decision(self, *, approved: bool, reason: Any) -> None:
        key = self._normalize_reason(approved=approved, reason=reason)
        if approved:
            self.signals_approved += 1
            self.approved_by_reason[key] = self.approved_by_reason.get(key, 0) + 1
            return
        self.signals_rejected += 1
        self.rejected_by_reason[key] = self.rejected_by_reason.get(key, 0) + 1

    def to_dict(self) -> dict[str, Any]:
        signals_emitted = int(self.signals_approved + self.signals_rejected)
        return {
            "run_id": self.run_id,
            "signals_emitted": signals_emitted,
            "signals_approved": int(self.signals_approved),
            "signals_rejected": int(self.signals_rejected),
            "approved_by_reason": dict(self.approved_by_reason),
            "rejected_by_reason": dict(self.rejected_by_reason),
            "fills": int(self.fills),
            "closed_trades": int(self.closed_trades),
            "forced_liquidations": int(self.forced_liquidations),
        }


def write_sanity_json(run_dir: str | Path, counters: SanityCounters, data_scope: dict[str, Any] | None = None) -> None:
    path = Path(run_dir) / "sanity.json"
    payload = counters.to_dict()
    if isinstance(data_scope, dict):
        payload["data_start_ts"] = data_scope.get("data_start_ts")
        payload["data_end_ts"] = data_scope.get("data_end_ts")
    payload["created_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    write_json_deterministic(path, payload)
