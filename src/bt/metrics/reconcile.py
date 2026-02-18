"""Execution-cost reconciliation checks."""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

ABS_TOL = 1e-9
REL_TOL = 1e-12


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"run_dir={path.parent}: missing artifact '{path.name}'") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"run_dir={path.parent}: invalid JSON in '{path.name}': {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"run_dir={path.parent}: expected object in '{path.name}'")
    return payload


def _coerce_non_negative_cost(*, value: Any, field_name: str, run_dir: Path, fill_ref: str) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"run_dir={run_dir}: fill {fill_ref} has non-numeric {field_name}={value!r}"
        ) from exc

    if numeric < 0:
        raise ValueError(
            f"run_dir={run_dir}: fill {fill_ref} has negative {field_name}={numeric}"
        )
    return numeric


def _sum_fill_costs(fills_path: Path, run_dir: Path) -> dict[str, float]:
    totals = {"fee_total": 0.0, "slippage_total": 0.0, "spread_total": 0.0}

    try:
        lines = fills_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError as exc:
        raise ValueError(f"run_dir={run_dir}: missing artifact '{fills_path.name}'") from exc

    for idx, raw_line in enumerate(lines):
        line = raw_line.strip()
        if not line:
            continue

        try:
            fill = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"run_dir={run_dir}: invalid JSON in {fills_path.name} at line {idx + 1}: {exc}"
            ) from exc

        if not isinstance(fill, dict):
            raise ValueError(
                f"run_dir={run_dir}: fill line {idx + 1} in {fills_path.name} must be a JSON object"
            )

        fill_id = fill.get("order_id")
        fill_ref = f"index={idx}" if fill_id in (None, "") else f"index={idx}, order_id={fill_id}"

        if "fee_cost" not in fill:
            raise ValueError(
                f"run_dir={run_dir}: fill {fill_ref} missing required cost field 'fee_cost'"
            )
        if "slippage_cost" not in fill:
            raise ValueError(
                f"run_dir={run_dir}: fill {fill_ref} missing required cost field 'slippage_cost'"
            )

        totals["fee_total"] += _coerce_non_negative_cost(
            value=fill["fee_cost"],
            field_name="fee_cost",
            run_dir=run_dir,
            fill_ref=fill_ref,
        )
        totals["slippage_total"] += _coerce_non_negative_cost(
            value=fill["slippage_cost"],
            field_name="slippage_cost",
            run_dir=run_dir,
            fill_ref=fill_ref,
        )

        spread_value = fill.get("spread_cost", 0.0)
        totals["spread_total"] += _coerce_non_negative_cost(
            value=spread_value,
            field_name="spread_cost",
            run_dir=run_dir,
            fill_ref=fill_ref,
        )

    return totals


def _assert_close(*, metric: str, expected: float, actual: float, run_dir: Path) -> None:
    if math.isclose(actual, expected, rel_tol=REL_TOL, abs_tol=ABS_TOL):
        return
    raise ValueError(
        "run_dir="
        f"{run_dir}: execution cost reconciliation failed for {metric}: "
        f"expected(performance)={expected:.16g}, actual(sum_fills)={actual:.16g}, "
        f"abs_tol={ABS_TOL}, rel_tol={REL_TOL}"
    )


def reconcile_execution_costs(run_dir: Path) -> None:
    """
    Raises ValueError if per-fill cost sums do not match performance.json totals
    within a tight tolerance.

    Compares:
      sum(fee_cost)      vs performance.json["fee_total"]
      sum(slippage_cost) vs performance.json["slippage_total"]
      sum(spread_cost)   vs performance.json["spread_total"]
    """
    run_path = Path(run_dir)
    performance_path = run_path / "performance.json"
    fills_path = run_path / "fills.jsonl"

    performance = _read_json(performance_path)
    fill_totals = _sum_fill_costs(fills_path, run_path)

    for field in ("fee_total", "slippage_total", "spread_total"):
        if field not in performance:
            raise ValueError(
                f"run_dir={run_path}: missing required field '{field}' in performance.json"
            )
        try:
            expected = float(performance[field])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"run_dir={run_path}: performance.json field '{field}' must be numeric, got {performance[field]!r}"
            ) from exc
        _assert_close(metric=field, expected=expected, actual=fill_totals[field], run_dir=run_path)
