from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bt.logging.formatting import write_json_deterministic


@dataclass(frozen=True)
class AuditContext:
    run_id: str
    config_hash: str


class AuditManager:
    def __init__(self, *, run_dir: Path, config: dict[str, Any], run_id: str) -> None:
        audit_cfg = config.get("audit") if isinstance(config.get("audit"), dict) else {}
        self.enabled = bool(audit_cfg.get("enabled", False))
        self.level = str(audit_cfg.get("level", "basic"))
        self.max_events = int(audit_cfg.get("max_events_per_file", 5000))
        self.audit_dir = run_dir / "audit"
        self._counts: dict[str, int] = {}
        self._violations: dict[str, int] = {}
        if self.enabled:
            config_hash = hashlib.sha256(
                json.dumps(config, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            self.audit_dir.mkdir(parents=True, exist_ok=True)
        else:
            config_hash = ""
        self._context = AuditContext(run_id=run_id, config_hash=config_hash)

    @property
    def context(self) -> AuditContext:
        return self._context

    def record_event(self, name: str, payload: dict[str, Any], *, violation: bool = False) -> None:
        if not self.enabled:
            return
        current = self._counts.get(name, 0)
        self._counts[name] = current + 1
        if violation:
            self._violations[name] = self._violations.get(name, 0) + 1
        if current >= self.max_events:
            return
        line = {
            "run_id": self._context.run_id,
            "config_hash": self._context.config_hash,
            **payload,
            "violation": violation,
        }
        path = self.audit_dir / f"{name}.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(line, default=str, sort_keys=True) + "\n")

    def write_json(self, name: str, payload: dict[str, Any]) -> None:
        if not self.enabled:
            return
        write_json_deterministic(self.audit_dir / name, payload)

    def write_summary(self) -> None:
        if not self.enabled:
            return
        summary = {
            "run_id": self._context.run_id,
            "counts": self._counts,
            "violations": self._violations,
            "status_by_layer": {
                key: ("fail" if self._violations.get(key, 0) > 0 else "pass")
                for key in self._counts
            },
        }
        self.write_json("stability_report.json", summary)
        print("Stability Report")
        for key in sorted(summary["status_by_layer"]):
            print(f"- {key}: {summary['status_by_layer'][key]}")
