"""Read-only command helpers for the Strategy Research Terminal."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any

from orchestrator.db import ResearchDB


def _connect(db_path: str | Path) -> sqlite3.Connection:
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Research DB not found: {db_path}")
    db = ResearchDB(db_path)
    return db.connect()


def research_status(db_path: str | Path) -> dict[str, Any]:
    conn = _connect(db_path)
    return {
        "hypotheses": _count_by(conn, "hypotheses", "status"),
        "experiments": _count_by(conn, "experiments", "status"),
        "pipeline_runs": _count_by(conn, "pipeline_runs", "status"),
        "verdicts": _count_by(conn, "verdicts", "verdict"),
        "terminal_card_artifacts": conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE artifact_type LIKE 'strategy_terminal_card_%'"
        ).fetchone()[0],
    }


def queue_status(db_path: str | Path, *, queue_name: str | None = None) -> dict[str, Any]:
    conn = _connect(db_path)
    if queue_name:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM queues WHERE queue_name = ? GROUP BY status ORDER BY status",
            (queue_name,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT queue_name, status, COUNT(*) AS n FROM queues GROUP BY queue_name, status ORDER BY queue_name, status"
        ).fetchall()
    return {"queues": [dict(row) for row in rows]}


def latest_hypothesis_verdict(db_path: str | Path, *, hypothesis_name: str) -> dict[str, Any]:
    conn = _connect(db_path)
    row = conn.execute(
        """
        SELECT v.*, h.name AS hypothesis_name
        FROM verdicts v
        JOIN hypotheses h ON h.id = v.hypothesis_id
        WHERE h.name = ?
        ORDER BY v.created_at DESC
        LIMIT 1
        """,
        (hypothesis_name,),
    ).fetchone()
    if row is None:
        return {"hypothesis_name": hypothesis_name, "available": False}
    return _row_to_dict(row)


def experiment_comparison(db_path: str | Path, *, hypothesis_name: str | None = None, phase: str | None = None) -> dict[str, Any]:
    conn = _connect(db_path)
    clauses: list[str] = []
    values: list[Any] = []
    if hypothesis_name:
        clauses.append("h.name = ?")
        values.append(hypothesis_name)
    if phase:
        clauses.append("e.phase = ?")
        values.append(phase)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"""
        SELECT h.name AS hypothesis_name, e.name, e.phase, e.dataset_type, e.status, e.experiment_root, e.updated_at
        FROM experiments e
        JOIN hypotheses h ON h.id = e.hypothesis_id
        {where}
        ORDER BY e.updated_at DESC
        LIMIT 100
        """,
        tuple(values),
    ).fetchall()
    return {"experiments": [_row_to_dict(row) for row in rows]}


def failure_explanation(db_path: str | Path, *, hypothesis_name: str | None = None) -> dict[str, Any]:
    conn = _connect(db_path)
    values: list[Any] = []
    where = "WHERE p.status = 'FAILED'"
    if hypothesis_name:
        where += " AND p.name = ?"
        values.append(hypothesis_name)
    row = conn.execute(
        f"""
        SELECT p.*, a.path AS latest_failure_card_path
        FROM pipeline_runs p
        LEFT JOIN artifacts a
          ON a.pipeline_run_id = p.id
         AND a.artifact_type = 'strategy_terminal_card_FailureCauseCard'
        {where}
        ORDER BY p.completed_at DESC
        LIMIT 1
        """,
        tuple(values),
    ).fetchone()
    if row is None:
        return {"available": False, "hypothesis_name": hypothesis_name}
    payload = _row_to_dict(row)
    path = payload.get("latest_failure_card_path")
    if path:
        payload["failure_card"] = _read_json(_resolve_db_relative_path(db_path, Path(path)))
    return payload


def state_bucket_summary(cards_json_path: str | Path) -> dict[str, Any]:
    bundle = _read_json(Path(cards_json_path))
    cards = bundle.get("cards", []) if isinstance(bundle, dict) else []
    for card in cards:
        if card.get("card_type") == "RegimeDependencyCard":
            return card
    return {"available": False, "reason": "RegimeDependencyCard not found"}


def similar_historical_runs(db_path: str | Path, *, card_type: str | None = None, limit: int = 10) -> dict[str, Any]:
    conn = _connect(db_path)
    artifact_type = f"strategy_terminal_card_{card_type}" if card_type else "strategy_terminal_cards_json"
    rows = conn.execute(
        """
        SELECT a.*, h.name AS hypothesis_name, p.phase, p.status AS pipeline_status
        FROM artifacts a
        LEFT JOIN hypotheses h ON h.id = a.hypothesis_id
        LEFT JOIN pipeline_runs p ON p.id = a.pipeline_run_id
        WHERE a.artifact_type = ?
        ORDER BY a.created_at DESC
        LIMIT ?
        """,
        (artifact_type, limit),
    ).fetchall()
    return {"artifacts": [_row_to_dict(row) for row in rows]}


def similar_historical_run_lookup(
    memory_db_path: str | Path,
    *,
    state: dict[str, Any] | None = None,
    state_json: str | Path | None = None,
) -> dict[str, Any]:
    """Look up similar historical trade/run evidence from research memory."""
    if not Path(memory_db_path).exists():
        raise FileNotFoundError(f"Research memory DB not found: {memory_db_path}")
    if state is None:
        if state_json is None:
            raise ValueError("state or state_json is required for similar historical lookup.")
        state = json.loads(Path(state_json).read_text(encoding="utf-8"))
    from orchestrator.research_memory.query_engine import run_query

    conn = sqlite3.connect(str(memory_db_path))
    conn.row_factory = sqlite3.Row
    try:
        return run_query(conn, "similar_state", state=state)
    finally:
        conn.close()


def recommendation_summary(db_path: str | Path, *, limit: int = 20) -> dict[str, Any]:
    conn = _connect(db_path)
    rows = conn.execute(
        """
        SELECT a.path, h.name AS hypothesis_name, p.phase, p.status AS pipeline_status, a.created_at
        FROM artifacts a
        LEFT JOIN hypotheses h ON h.id = a.hypothesis_id
        LEFT JOIN pipeline_runs p ON p.id = a.pipeline_run_id
        WHERE a.artifact_type = 'strategy_terminal_card_NextExperimentCard'
        ORDER BY a.created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    items = []
    for row in rows:
        item = _row_to_dict(row)
        card = _read_json(_resolve_db_relative_path(db_path, Path(str(row["path"]))))
        item["recommended_action"] = ((card.get("data") or {}).get("recommended_action") if isinstance(card, dict) else None)
        item["summary"] = ((card.get("data") or {}).get("promotion_or_scrap_summary") if isinstance(card, dict) else None)
        items.append(item)
    return {"recommendations": items}


def _resolve_db_relative_path(db_path: str | Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    db_parent = Path(db_path).resolve().parent
    candidate = db_parent / path
    if candidate.exists():
        return candidate
    return path


def _count_by(conn: sqlite3.Connection, table: str, column: str) -> dict[str, int]:
    rows = conn.execute(f"SELECT {column}, COUNT(*) AS n FROM {table} GROUP BY {column} ORDER BY {column}").fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    out = dict(row)
    for key in ("metadata_json", "commands_json", "evidence_json", "payload_json"):
        if key in out and out[key]:
            try:
                out[key[:-5] if key.endswith("_json") else key] = json.loads(out[key])
            except Exception:
                pass
    return out


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"available": False, "error": str(exc), "path": str(path)}
