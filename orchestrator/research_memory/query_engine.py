from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


def run_query(conn: sqlite3.Connection, mode: str, **kwargs: Any) -> dict[str, Any]:
    if mode == "similar_state":
        state = kwargs.get("state") or json.loads(Path(kwargs["state_json"]).read_text(encoding="utf-8"))
        return similar_state(conn, state)
    if mode == "best_states":
        return {"states": _rows(conn, "SELECT * FROM research_memory_state_buckets WHERE ev_r_net > 0 ORDER BY ev_r_net DESC, n_trades DESC LIMIT 50")}
    if mode == "avoid_states":
        return {"states": _rows(conn, "SELECT * FROM research_memory_state_buckets WHERE ev_r_net < 0 OR avg_cost_drag_r >= 0.5 ORDER BY ev_r_net ASC, avg_cost_drag_r DESC LIMIT 50")}
    if mode == "setup_profile":
        return setup_profile(conn, kwargs["setup_class"])
    if mode == "hypothesis_profile":
        return hypothesis_profile(conn, kwargs["hypothesis_name"])
    if mode == "candidate_profile":
        return candidate_profile(conn, kwargs.get("candidate_id"), kwargs.get("run_id"))
    raise ValueError(f"Unsupported query mode: {mode}")


def similar_state(conn: sqlite3.Connection, state: dict[str, Any]) -> dict[str, Any]:
    clauses = ["metrics_valid = 1", "r_net IS NOT NULL"]
    params: list[Any] = []
    setup = state.get("setup_class")
    if setup:
        clauses.append("setup_class = ?")
        params.append(setup)
    windows = {
        "csi_pctile": 0.15,
        "vol_pctile": 0.15,
        "spread_pctile": 0.20,
        "tr_over_atr": 0.75,
        "funding_pctile": 0.15,
        "oi_accel_pctile": 0.15,
        "basis_pctile": 0.15,
        "crowding_proxy_pctile": 0.20,
        "constraint_stress_pctile": 0.20,
    }
    for field, width in windows.items():
        if state.get(field) is not None:
            clauses.append(f"{field} BETWEEN ? AND ?")
            params.extend([float(state[field]) - width, float(state[field]) + width])
    sql = f"SELECT * FROM research_memory_trades WHERE {' AND '.join(clauses)}"
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return {
            "matching_historical_trades": [],
            "n_matches": 0,
            "recommendation": "inconclusive",
            "evidence_quality": "none",
        }
    r = pd.to_numeric(df["r_net"], errors="coerce").dropna()
    result = {
        "matching_historical_trades": df.head(50).to_dict("records"),
        "n_matches": int(len(r)),
        "ev_r_net": float(r.mean()),
        "median_r": float(r.median()),
        "win_rate": float((r > 0).mean()),
        "tail_3r_rate": float((r >= 3).mean()),
        "tail_5r_rate": float((r >= 5).mean()),
        "avg_mfe_r": _mean(df, "mfe_r"),
        "avg_mae_r": _mean(df, "mae_r"),
        "avg_exit_efficiency": _mean(df, "exit_efficiency"),
        "avg_cost_drag_r": _mean(df, "cost_drag_r"),
    }
    result["evidence_quality"] = "strong" if len(r) >= 30 else "moderate" if len(r) >= 10 else "thin"
    result["recommendation"] = _similar_recommendation(result)
    return result


def setup_profile(conn: sqlite3.Connection, setup_class: str) -> dict[str, Any]:
    return {
        "setup_class": setup_class,
        "state_buckets": _rows(conn, "SELECT * FROM research_memory_state_buckets WHERE setup_class = ? ORDER BY ev_r_net DESC", (setup_class,)),
        "recommendations": _rows(conn, "SELECT * FROM research_memory_recommendations WHERE setup_class = ? ORDER BY evidence_score DESC", (setup_class,)),
    }


def hypothesis_profile(conn: sqlite3.Connection, hypothesis_name: str) -> dict[str, Any]:
    return {
        "hypothesis_name": hypothesis_name,
        "runs": _rows(conn, "SELECT run_id, dataset_type, phase, COUNT(*) AS n_trades, AVG(r_net) AS ev_r_net FROM research_memory_trades WHERE hypothesis_name = ? GROUP BY run_id, dataset_type, phase", (hypothesis_name,)),
        "state_findings": _rows(conn, "SELECT * FROM research_memory_state_buckets WHERE hypothesis_name = ? ORDER BY ev_r_net DESC", (hypothesis_name,)),
        "candidates": _rows(conn, "SELECT * FROM research_memory_candidates WHERE hypothesis_name = ? ORDER BY promotion_score DESC", (hypothesis_name,)),
    }


def candidate_profile(conn: sqlite3.Connection, candidate_id: str | None = None, run_id: str | None = None) -> dict[str, Any]:
    if candidate_id:
        return {"candidates": _rows(conn, "SELECT * FROM research_memory_candidates WHERE candidate_id = ?", (candidate_id,))}
    return {"candidates": _rows(conn, "SELECT * FROM research_memory_candidates WHERE run_id = ?", (run_id,))}


def _similar_recommendation(result: dict[str, Any]) -> str:
    if result["n_matches"] < 5:
        return "inconclusive"
    ev = result.get("ev_r_net") or 0
    cost = result.get("avg_cost_drag_r") or 0
    if ev > 0.15 and cost < 0.4:
        return "allow"
    if ev > 0 and cost < 0.75:
        return "reduce_size"
    if ev < -0.10 or cost >= 0.75:
        return "block"
    return "inconclusive"


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if col not in df.columns:
        return None
    val = pd.to_numeric(df[col], errors="coerce").mean()
    return None if pd.isna(val) else float(val)


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute(sql, params).fetchall()]
