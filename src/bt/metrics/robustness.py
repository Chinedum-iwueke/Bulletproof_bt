"""Robustness metrics for stress-suite outputs."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pandas.errors import EmptyDataError


@dataclass
class RobustnessResult:
    strategy_name: str
    baseline_run_id: str
    breaking_fee_mult: Optional[float]
    breaking_delay_bars: Optional[int]
    robustness_score: float
    per_scenario: List[Dict[str, Any]]


def compute_robustness(
    *,
    stress_summary: Dict[str, Any],
    output_root: str = "outputs/runs",
    strategy_name: str = "unknown",
    ev_floor: float = 0.0,
) -> RobustnessResult:
    """
    Loads per-scenario outputs, computes survivability metrics,
    and returns a robustness result.
    """
    summary = stress_summary.get("summary", {})
    if not summary:
        raise ValueError("stress_summary must include a non-empty summary")

    scenario_params = _extract_scenario_params(stress_summary)
    scenario_names = list(summary.keys())
    scenarios: list[dict[str, Any]] = []
    if scenario_params:
        for name in scenario_names:
            params = scenario_params.get(name, {})
            scenarios.append(
                {
                    "name": name,
                    "fee_mult": float(params.get("fee_mult", 1.0)),
                    "slippage_mult": float(params.get("slippage_mult", 1.0)),
                    "add_delay_bars": int(params.get("add_delay_bars", 0)),
                    "drop_fill_prob": float(params.get("drop_fill_prob", 0.0)),
                }
            )
        baseline_candidates = [
            scenario
            for scenario in scenarios
            if scenario["fee_mult"] == 1.0
            and scenario["slippage_mult"] == 1.0
            and scenario["add_delay_bars"] == 0
            and scenario["drop_fill_prob"] == 0.0
        ]
        if not baseline_candidates:
            raise ValueError("Baseline scenario not found")
        baseline_name = None
        for candidate in baseline_candidates:
            if candidate["name"] == "baseline":
                baseline_name = candidate["name"]
                break
        if baseline_name is None:
            if len(baseline_candidates) == 1:
                baseline_name = baseline_candidates[0]["name"]
            else:
                raise ValueError("Baseline scenario not found")
    else:
        baseline_name = "baseline"
        if baseline_name not in scenario_names:
            raise ValueError("Baseline scenario not found")
        for name in scenario_names:
            if name == baseline_name:
                scenarios.append(
                    {
                        "name": name,
                        "fee_mult": 1.0,
                        "slippage_mult": 1.0,
                        "add_delay_bars": 0,
                        "drop_fill_prob": 0.0,
                    }
                )
            else:
                scenarios.append(
                    {
                        "name": name,
                        "fee_mult": None,
                        "slippage_mult": None,
                        "add_delay_bars": None,
                        "drop_fill_prob": None,
                    }
                )
    baseline_summary = summary[baseline_name]
    baseline_run_id = str(baseline_summary.get("run_id", baseline_name))
    baseline_run_dir = _resolve_run_dir(
        baseline_summary, output_root=output_root, run_id=baseline_run_id
    )

    baseline_trades_path = baseline_run_dir / "trades.csv"
    baseline_equity_path = baseline_run_dir / "equity.csv"
    ev_baseline = _compute_ev(baseline_trades_path)
    baseline_final_equity = _final_equity(baseline_equity_path)

    baseline_fills = int(baseline_summary.get("n_fills", 0))

    per_scenario: list[dict[str, Any]] = []
    for scenario in scenarios:
        name = scenario["name"]
        scenario_summary = summary[name]
        run_id = str(scenario_summary.get("run_id", name))
        run_dir = _resolve_run_dir(
            scenario_summary, output_root=output_root, run_id=run_id
        )
        trades_path = run_dir / "trades.csv"
        equity_path = run_dir / "equity.csv"

        ev_scenario = _compute_ev(trades_path)
        final_equity = _final_equity(equity_path)
        delta_ev = ev_scenario - ev_baseline
        delta_final_equity_pct = (
            (final_equity / baseline_final_equity) - 1.0
            if baseline_final_equity != 0
            else 0.0
        )

        fills = int(scenario_summary.get("n_fills", 0))
        survival_rate = _clamp(
            fills / baseline_fills if baseline_fills > 0 else 0.0,
            low=0.0,
            high=1.0,
        )

        cost_sensitivity = None
        if scenario["fee_mult"] not in (None, 1.0):
            cost_sensitivity = (
                (final_equity - baseline_final_equity) / scenario["fee_mult"]
                if scenario["fee_mult"] != 0
                else None
            )

        per_scenario.append(
            {
                "scenario": name,
                "run_id": run_id,
                "run_dir": str(run_dir),
                "fee_mult": scenario["fee_mult"],
                "slippage_mult": scenario["slippage_mult"],
                "add_delay_bars": scenario["add_delay_bars"],
                "drop_fill_prob": scenario["drop_fill_prob"],
                "ev": ev_scenario,
                "delta_ev": delta_ev,
                "final_equity": final_equity,
                "delta_final_equity_pct": delta_final_equity_pct,
                "survival_rate": survival_rate,
                "cost_sensitivity": cost_sensitivity,
            }
        )

    breaking_fee_mult = _breaking_fee_mult(per_scenario, ev_floor=ev_floor)
    breaking_delay_bars = _breaking_delay_bars(per_scenario, ev_floor=ev_floor)
    robustness_score = _robustness_score(
        per_scenario,
        breaking_fee_mult=breaking_fee_mult,
        breaking_delay_bars=breaking_delay_bars,
    )

    result = RobustnessResult(
        strategy_name=strategy_name,
        baseline_run_id=baseline_run_id,
        breaking_fee_mult=breaking_fee_mult,
        breaking_delay_bars=breaking_delay_bars,
        robustness_score=robustness_score,
        per_scenario=per_scenario,
    )

    _write_artifacts(
        baseline_run_dir,
        result,
    )

    return result


def _extract_scenario_params(stress_summary: Dict[str, Any]) -> dict[str, dict[str, Any]]:
    scenarios = stress_summary.get("scenarios", [])
    params: dict[str, dict[str, Any]] = {}
    if isinstance(scenarios, list):
        for scenario in scenarios:
            if isinstance(scenario, dict) and "name" in scenario:
                params[str(scenario["name"])] = dict(scenario)
    scenario_params = stress_summary.get("scenario_params")
    if isinstance(scenario_params, dict):
        for name, values in scenario_params.items():
            if isinstance(values, dict):
                params[str(name)] = dict(values)
    return params


def _resolve_run_dir(summary_entry: dict[str, Any], *, output_root: str, run_id: str) -> Path:
    if "run_dir" in summary_entry:
        return Path(summary_entry["run_dir"])
    return Path(output_root) / run_id


def _compute_ev(trades_path: Path) -> float:
    if not trades_path.exists():
        return 0.0
    try:
        trades_df = pd.read_csv(trades_path)
    except EmptyDataError:
        return 0.0
    if trades_df.empty:
        return 0.0
    if "pnl_net" in trades_df.columns:
        values = trades_df["pnl_net"]
    elif "pnl" in trades_df.columns:
        values = trades_df["pnl"]
    else:
        return 0.0
    if values.empty:
        return 0.0
    return float(values.mean())


def _final_equity(equity_path: Path) -> float:
    if not equity_path.exists():
        return 0.0
    equity_df = pd.read_csv(equity_path)
    if equity_df.empty or "equity" not in equity_df.columns:
        return 0.0
    return float(equity_df["equity"].iloc[-1])


def _clamp(value: float, *, low: float, high: float) -> float:
    return max(low, min(high, value))


def _breaking_fee_mult(
    per_scenario: list[dict[str, Any]], *, ev_floor: float
) -> Optional[float]:
    fee_candidates = [
        row["fee_mult"]
        for row in per_scenario
        if row["ev"] < ev_floor
        and row["fee_mult"] is not None
        and row["fee_mult"] != 1.0
    ]
    return min(fee_candidates) if fee_candidates else None


def _breaking_delay_bars(
    per_scenario: list[dict[str, Any]], *, ev_floor: float
) -> Optional[int]:
    delay_candidates = [
        int(row["add_delay_bars"])
        for row in per_scenario
        if row["ev"] < ev_floor
        and row["add_delay_bars"] is not None
        and row["add_delay_bars"] > 0
    ]
    return min(delay_candidates) if delay_candidates else None


def _robustness_score(
    per_scenario: list[dict[str, Any]],
    *,
    breaking_fee_mult: Optional[float],
    breaking_delay_bars: Optional[int],
) -> float:
    score = 0.0
    if breaking_fee_mult is None:
        score += 1.0
    else:
        score += 1.0 / breaking_fee_mult

    if breaking_delay_bars is None:
        score += 1.0
    else:
        score += 1.0 / (1.0 + breaking_delay_bars)

    survival_rates = [row["survival_rate"] for row in per_scenario]
    if survival_rates:
        score += sum(survival_rates) / len(survival_rates)

    delta_evs = [row["delta_ev"] for row in per_scenario]
    if delta_evs:
        mean_delta_ev = sum(delta_evs) / len(delta_evs)
        score += _clamp(mean_delta_ev, low=-1.0, high=1.0)

    return _clamp(score, low=0.0, high=4.0)


def _write_artifacts(run_dir: Path, result: RobustnessResult) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)

    robustness_path = run_dir / "robustness.json"
    with robustness_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "strategy_name": result.strategy_name,
                "baseline_run_id": result.baseline_run_id,
                "breaking_fee_mult": result.breaking_fee_mult,
                "breaking_delay_bars": result.breaking_delay_bars,
                "robustness_score": result.robustness_score,
                "per_scenario": result.per_scenario,
            },
            handle,
            indent=2,
        )

    rows = []
    for row in result.per_scenario:
        rows.append(
            {
                **row,
                "robustness_score": result.robustness_score,
                "breaking_fee_mult": result.breaking_fee_mult,
                "breaking_delay_bars": result.breaking_delay_bars,
            }
        )
    pd.DataFrame(rows).to_csv(run_dir / "robustness.csv", index=False)
