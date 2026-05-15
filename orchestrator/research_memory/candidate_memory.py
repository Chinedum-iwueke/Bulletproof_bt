from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


CANDIDATE_COLUMNS = [
    "id", "candidate_id", "hypothesis_name", "run_id", "dataset_type", "phase", "candidate_status",
    "rank_score", "promotion_score", "ev_r_net", "n_trades", "tail_5r_count", "tail_10r_count",
    "setup_class", "state_profile_json", "recommended_action", "source_path", "created_at",
]


def load_candidate_file(path: Path) -> list[dict[str, Any]]:
    try:
        if path.suffix == ".csv":
            rows = pd.read_csv(path).to_dict("records")
        elif path.suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            rows = data if isinstance(data, list) else data.get("candidates", data.get("alpha_candidates", []))
            if isinstance(rows, dict):
                rows = [rows]
        else:
            return []
        return [normalize_candidate(dict(row), path) for row in rows]
    except Exception:
        return []


def normalize_candidate(row: dict[str, Any], source_path: Path) -> dict[str, Any]:
    candidate_id = _first(row, ["candidate_id", "id", "alpha_id"]) or _first(row, ["run_id"])
    raw = json.dumps(row, sort_keys=True, default=str)
    rid = hashlib.sha1(f"{source_path}|{candidate_id}|{raw}".encode("utf-8")).hexdigest()
    state_profile = _first(row, ["state_profile", "state_profile_json"])
    if isinstance(state_profile, str):
        state_json = state_profile
    else:
        state_json = json.dumps(state_profile or {}, sort_keys=True, default=str)
    return {
        "id": rid,
        "candidate_id": candidate_id,
        "hypothesis_name": _first(row, ["hypothesis_name", "name"]),
        "run_id": _first(row, ["run_id"]),
        "dataset_type": _first(row, ["dataset_type"]),
        "phase": _first(row, ["phase"]),
        "candidate_status": _first(row, ["candidate_status", "status"]),
        "rank_score": _num(_first(row, ["rank_score"])),
        "promotion_score": _num(_first(row, ["promotion_score"])),
        "ev_r_net": _num(_first(row, ["ev_r_net", "mean_r_net"])),
        "n_trades": _int(_first(row, ["n_trades"])),
        "tail_5r_count": _int(_first(row, ["tail_5r_count"])),
        "tail_10r_count": _int(_first(row, ["tail_10r_count"])),
        "setup_class": _first(row, ["setup_class"]),
        "state_profile_json": state_json,
        "recommended_action": _first(row, ["recommended_action", "action"]),
        "source_path": str(source_path),
        "created_at": _now(),
    }


def insert_candidates(conn, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    placeholders = ", ".join(["?"] * len(CANDIDATE_COLUMNS))
    columns = ", ".join(CANDIDATE_COLUMNS)
    updates = ", ".join([f"{c}=excluded.{c}" for c in CANDIDATE_COLUMNS if c != "id"])
    sql = f"INSERT INTO research_memory_candidates ({columns}) VALUES ({placeholders}) ON CONFLICT(id) DO UPDATE SET {updates}"
    conn.executemany(sql, [[rec.get(c) for c in CANDIDATE_COLUMNS] for rec in records])
    return len(records)


def _first(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        val = row.get(key)
        if val is not None and val == val and val != "":
            return val
    return None


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "" or value != value:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    x = _num(value)
    return None if x is None else int(x)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
