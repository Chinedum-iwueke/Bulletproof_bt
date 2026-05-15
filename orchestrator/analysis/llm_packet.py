from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _trim_hypothesis(text: str, max_chars: int = 2500) -> str:
    trimmed = text.strip()
    if len(trimmed) <= max_chars:
        return trimmed
    return trimmed[:max_chars] + "\n... [truncated]"


def _prompt_run(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "run_id",
        "signal_timeframe",
        "theta_vol",
        "k_atr",
        "ev_r_net",
        "ev_r_gross",
        "robustness_score",
        "num_trades",
        "win_rate",
        "max_drawdown_r",
        "mfe_mean_r",
        "mae_mean_r",
        "frac_trades_hit_2r",
        "frac_trades_hit_3r",
        "long_ev_r_net",
        "short_ev_r_net",
    ]
    return {key: row.get(key) for key in keys if row.get(key) not in (None, "")}


def _prompt_finding(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "finding_type",
        "state_variable",
        "bucket",
        "dataset_type",
        "n_trades",
        "ev_r_net",
        "ev_r_gross",
        "win_rate",
        "tail_5r_rate",
        "avg_mfe_r",
        "avg_exit_efficiency",
        "avg_cost_drag_r",
        "finding_score",
    ]
    return {key: row.get(key) for key in keys if row.get(key) not in (None, "")}


def _compact_packet_for_prompt(packet: dict[str, Any]) -> dict[str, Any]:
    state = packet.get("state_discovery_summary")
    if not isinstance(state, dict):
        state = {}
    return {
        "name": packet.get("name"),
        "hypothesis_text_excerpt": _trim_hypothesis(str(packet.get("hypothesis_text_excerpt") or ""), max_chars=900),
        "run_counts": packet.get("run_counts"),
        "top_runs": [_prompt_run(row) for row in (packet.get("top_runs") or [])[:3] if isinstance(row, dict)],
        "bottom_runs": [_prompt_run(row) for row in (packet.get("bottom_runs") or [])[:2] if isinstance(row, dict)],
        "dataset_comparison": packet.get("dataset_comparison"),
        "state_discovery_summary": {
            "state_discovery_available": state.get("state_discovery_available"),
            "strongest_positive_states": [_prompt_finding(row) for row in (state.get("strongest_positive_states") or [])[:2] if isinstance(row, dict)],
            "tail_generation_states": [_prompt_finding(row) for row in (state.get("tail_generation_states") or [])[:2] if isinstance(row, dict)],
            "cost_killed_states": [_prompt_finding(row) for row in (state.get("cost_killed_states") or [])[:2] if isinstance(row, dict)],
            "exit_failure_states": [_prompt_finding(row) for row in (state.get("exit_failure_states") or [])[:2] if isinstance(row, dict)],
        },
        "salvage_candidates": (packet.get("salvage_candidates") or [])[:4],
        "failure_mode": packet.get("failure_mode"),
        "preliminary_verdict": packet.get("preliminary_verdict"),
        "allowed_verdicts": packet.get("allowed_verdicts"),
    }


def _top_bottom(rows: list[dict[str, Any]], top_n: int, bottom_n: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ranked = sorted(rows, key=lambda r: float(r.get("robustness_score", -999999) or -999999), reverse=True)
    return ranked[:top_n], list(reversed(ranked[-bottom_n:])) if bottom_n > 0 else []


RUN_CONTEXT_FIELDS = [
    "dataset",
    "run_id",
    "variant_id",
    "tier",
    "signal_timeframe",
    "theta_vol",
    "k_atr",
    "m_atr",
    "T_hold",
    "tp_enabled",
    "ev_r_net",
    "ev_r_gross",
    "robustness_score",
    "num_trades",
    "win_rate",
    "payoff_ratio",
    "max_drawdown_r",
    "max_consecutive_losses",
    "mfe_mean_r",
    "mae_mean_r",
    "mfe_median_r",
    "mae_median_r",
    "frac_trades_hit_1r",
    "frac_trades_hit_2r",
    "frac_trades_hit_3r",
    "long_trade_count",
    "short_trade_count",
    "long_ev_r_net",
    "short_ev_r_net",
    "capture_ratio_mean",
]


def _compact_run_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in RUN_CONTEXT_FIELDS if row.get(key) not in (None, "")}


def _bucket_level_context(
    rows: list[dict[str, Any]],
    structural_rows: list[dict[str, Any]] | None,
    dataset: str,
    limit: int = 3,
) -> list[dict[str, Any]]:
    score_by_run = {str(r.get("run_id")): r for r in rows if r.get("run_id")}
    source_rows = structural_rows if structural_rows else rows
    out: list[dict[str, Any]] = []
    for row in source_rows:
        run_id = row.get("run_id")
        scored = score_by_run.get(str(run_id), {})
        item = {
            "dataset": dataset,
            "run_id": run_id,
            "ev_r_net": row.get("ev_r_net") if row.get("ev_r_net") not in (None, "") else scored.get("ev_r_net"),
            "robustness_score": scored.get("robustness_score", row.get("robustness_score")),
            "best_bucket_type": row.get("best_bucket_type"),
            "best_bucket": row.get("best_bucket"),
            "best_bucket_ev_r_net": row.get("best_bucket_ev_r_net"),
            "best_bucket_n_trades": row.get("best_bucket_n_trades"),
            "worst_bucket_type": row.get("worst_bucket_type"),
            "worst_bucket": row.get("worst_bucket"),
            "worst_bucket_ev_r_net": row.get("worst_bucket_ev_r_net"),
            "tail_bucket_type": row.get("tail_bucket_type"),
            "tail_bucket": row.get("tail_bucket"),
            "tail_5r_count": row.get("tail_5r_count"),
            "needs_state_filter": row.get("needs_state_filter"),
            "needs_exit_refinement": row.get("needs_exit_refinement"),
            "cost_fragile": row.get("cost_fragile"),
            "one_bucket_dependency": row.get("one_bucket_dependency"),
        }
        if any(v not in (None, "") for k, v in item.items() if k not in {"dataset", "run_id"}):
            out.append(item)
    out.sort(key=lambda r: float(r.get("robustness_score", -999999) or -999999), reverse=True)
    return out[:limit]


def build_llm_packet(
    *,
    name: str,
    hypothesis_text: str,
    input_files: dict[str, Any],
    stable_rows: list[dict[str, Any]],
    vol_rows: list[dict[str, Any]],
    stable_structural_rows: list[dict[str, Any]] | None = None,
    vol_structural_rows: list[dict[str, Any]] | None = None,
    diagnostics: dict[str, Any],
    preliminary: dict[str, Any],
    max_top_runs: int,
    max_bottom_runs: int,
) -> dict[str, Any]:
    combined = list(stable_rows) + list(vol_rows)
    top_runs_raw, bottom_runs_raw = _top_bottom(combined, max_top_runs, max_bottom_runs)
    top_runs = [_compact_run_row(row) for row in top_runs_raw]
    bottom_runs = [_compact_run_row(row) for row in bottom_runs_raw]
    bucket_context = diagnostics.get("bucket_level_context")
    if not isinstance(bucket_context, dict):
        bucket_context = {
            "source": "run_structural_summary.csv when available; run summary rows otherwise",
            "stable": _bucket_level_context(stable_rows, stable_structural_rows, "stable"),
            "volatile": _bucket_level_context(vol_rows, vol_structural_rows, "volatile"),
        }

    return {
        "name": name,
        "hypothesis_text_excerpt": _trim_hypothesis(hypothesis_text),
        "input_files": input_files,
        "run_counts": {
            "stable": len(stable_rows),
            "volatile": len(vol_rows),
            "total": len(combined),
        },
        "top_runs": top_runs,
        "bottom_runs": bottom_runs,
        "dataset_comparison": diagnostics.get("dataset_comparison", {}),
        "bucket_level_context": bucket_context,
        "promotion_candidates": diagnostics.get("promotion_candidates", []),
        "salvage_candidates": diagnostics.get("salvage_candidates", [])[:8],
        "scrap_evidence": diagnostics.get("scrap_evidence", []),
        "failure_mode": diagnostics.get("failure_mode"),
        "preliminary_verdict": preliminary.get("preliminary_verdict"),
        "allowed_verdicts": preliminary.get("allowed_verdicts", []),
    }


def build_llm_prompt(packet: dict[str, Any]) -> str:
    instructions = """You are a rigorous systematic trading research analyst reviewing Bulletproof_bt experiment results.

Your job is to decide whether this hypothesis should be scrapped, refined, promoted to Tier3, or marked inconclusive.

Important principles:
- Do not promote based only on best EV.
- Consider sample size, drawdown, tail behavior, gross-to-net cost drag, MFE/MAE, stable vs volatile consistency, and parameter logic.
- Use bucket-level EV/robustness context when present to distinguish broad edge from one-bucket dependency.
- Negative EV can still be informative if conditional structure exists.
- Positive EV can still be fragile if sample size is weak or driven by one lucky tail trade.
- If evidence is insufficient, say inconclusive.
- Do not invent values not present in the packet.
- Return only valid JSON. Do not include markdown, prose, comments, or code fences.

Return one JSON object matching this exact top-level schema. The top-level "verdict" field is required:
{
  "verdict": "...",
  "confidence": 0.0,
  "summary": "...",
  "primary_reason": "...",
  "promote_runs": [],
  "refine_from_runs": [],
  "scrap_reason": null,
  "dominant_failure_mode": "...",
  "evidence": {
    "best_ev_r_net": null,
    "median_ev_r_net": null,
    "positive_runs": null,
    "best_dataset": null,
    "notes": []
  },
  "recommended_next_tests": [
    {
      "type": "...",
      "description": "...",
      "grid_size": 24,
      "parent_runs": []
    }
  ],
  "human_approval_required": true
}
"""
    return instructions + "\n\nPACKET_JSON:\n" + json.dumps(_compact_packet_for_prompt(packet), indent=2)


def write_packet_files(output_dir: Path, name: str, packet: dict[str, Any], prompt: str) -> tuple[Path, Path]:
    packet_path = output_dir / f"{name}_llm_packet.json"
    prompt_path = output_dir / f"{name}_llm_prompt.txt"
    packet_path.write_text(json.dumps(packet, indent=2), encoding="utf-8")
    prompt_path.write_text(prompt, encoding="utf-8")
    return packet_path, prompt_path
