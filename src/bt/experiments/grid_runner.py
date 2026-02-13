"""Deterministic experiment grid runner library API."""
from __future__ import annotations

import copy
import csv
import itertools
import json
import re
import shutil
import traceback
from pathlib import Path
from typing import Any

import yaml

from bt.config import deep_merge
from bt.core.config_resolver import resolve_config
from bt.data.load_feed import load_feed
from bt.logging.trades import write_data_scope
from bt.metrics.performance import compute_performance, write_performance_artifacts


_TEMPLATE_PATTERN = re.compile(r"{([^{}]+)}")
_SUMMARY_COLUMNS = [
    "run_name",
    "strategy_adx_min",
    "strategy_vol_floor_pct",
    "total_trades",
    "ev_net",
    "ev_gross",
    "final_equity",
    "max_drawdown_pct",
    "max_drawdown_duration_bars",
    "tail_loss_p95",
    "tail_loss_p99",
    "sharpe",
    "sortino",
    "mar",
    "max_consecutive_losses",
    "worst_streak_loss",
    "fee_total",
    "slippage_total",
    "win_rate",
    "status",
    "error_type",
    "error_message",
]


def set_by_dotpath(cfg: dict[str, Any], dotpath: str, value: Any) -> None:
    parts = dotpath.split(".")
    current = cfg
    for part in parts[:-1]:
        next_value = current.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            current[part] = next_value
        current = next_value
    current[parts[-1]] = value


def get_by_dotpath(cfg: dict[str, Any], dotpath: str) -> Any:
    current: Any = cfg
    for part in dotpath.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(dotpath)
        current = current[part]
    return current


def _validate_experiment(exp_cfg: dict[str, Any]) -> None:
    if exp_cfg.get("version") != 1:
        raise ValueError("Experiment config version must be 1")
    fixed = exp_cfg.get("fixed")
    if fixed is not None and not isinstance(fixed, dict):
        raise ValueError("Experiment config fixed must be a mapping when provided")
    grid = exp_cfg.get("grid")
    if not isinstance(grid, dict) or not grid:
        raise ValueError("Experiment config grid must be a non-empty mapping")
    for key, values in grid.items():
        if not isinstance(values, list) or not values:
            raise ValueError(f"Experiment grid values for '{key}' must be non-empty lists")


def _expand_grid(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    keys = sorted(grid.keys())
    values_product = itertools.product(*(grid[key] for key in keys))
    return [dict(zip(keys, values, strict=True)) for values in values_product]


def _render_run_suffix(template: str, context_cfg: dict[str, Any]) -> str:
    def replace(match: re.Match[str]) -> str:
        dotpath = match.group(1)
        try:
            value = get_by_dotpath(context_cfg, dotpath)
        except KeyError as exc:
            raise ValueError(f"Run naming template references missing key: {dotpath}") from exc
        return str(value)

    return _TEMPLATE_PATTERN.sub(replace, template)


def _build_summary_row(
    run_name: str,
    params: dict[str, Any],
    perf: dict[str, Any],
    *,
    status: str,
    error_type: str = "",
    error_message: str = "",
) -> dict[str, Any]:
    return {
        "run_name": run_name,
        "strategy_adx_min": params.get("strategy.adx_min"),
        "strategy_vol_floor_pct": params.get("strategy.vol_floor_pct"),
        "total_trades": perf.get("total_trades"),
        "ev_net": perf.get("ev_net"),
        "ev_gross": perf.get("ev_gross"),
        "final_equity": perf.get("final_equity"),
        "max_drawdown_pct": perf.get("max_drawdown_pct"),
        "max_drawdown_duration_bars": perf.get("max_drawdown_duration_bars"),
        "tail_loss_p95": perf.get("tail_loss_p95"),
        "tail_loss_p99": perf.get("tail_loss_p99"),
        "sharpe": perf.get("sharpe_annualized"),
        "sortino": perf.get("sortino_annualized"),
        "mar": perf.get("mar_ratio"),
        "max_consecutive_losses": perf.get("max_consecutive_losses"),
        "worst_streak_loss": perf.get("worst_streak_loss"),
        "fee_total": perf.get("fee_total"),
        "slippage_total": perf.get("slippage_total"),
        "win_rate": perf.get("win_rate"),
        "status": status,
        "error_type": error_type,
        "error_message": error_message,
    }


def run_grid(
    *,
    config: dict[str, Any],
    experiment_cfg: dict[str, Any],
    data_path: str,
    out_path: Path,
    force: bool = False,
) -> Path:
    from bt.api import _build_engine

    _validate_experiment(experiment_cfg)

    runs_dir = out_path / "runs"
    if runs_dir.exists() and any(runs_dir.iterdir()) and not force:
        raise RuntimeError(f"Output already contains runs: {runs_dir}")
    if force and out_path.exists():
        shutil.rmtree(out_path)

    out_path.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    grid = experiment_cfg["grid"]
    grid_runs = _expand_grid(grid)
    sorted_keys = sorted(grid.keys())

    with (out_path / "grid_used.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "experiment": experiment_cfg,
                "resolved_grid_keys": sorted_keys,
                "grid_runs": grid_runs,
                "paths": {
                    "config": "<in-memory>",
                    "local_config": None,
                    "experiment": "<in-memory>",
                    "data": str(data_path),
                },
            },
            handle,
            sort_keys=False,
        )

    run_template = (
        experiment_cfg.get("run_naming", {}).get("template")
        if isinstance(experiment_cfg.get("run_naming"), dict)
        else None
    ) or "run"
    fixed_overrides = experiment_cfg.get("fixed") or {}

    summary_rows: list[dict[str, Any]] = []

    for index, params in enumerate(grid_runs, start=1):
        run_prefix = f"run_{index:03d}"
        merged_cfg = deep_merge(config, fixed_overrides)

        dotpath_overrides: dict[str, Any] = {}
        for dotpath, value in params.items():
            set_by_dotpath(dotpath_overrides, dotpath, value)
        merged_cfg = deep_merge(merged_cfg, dotpath_overrides)

        run_suffix = "run"
        run_name_error: Exception | None = None
        try:
            run_suffix = _render_run_suffix(run_template, merged_cfg)
        except Exception as exc:
            run_suffix = "template_error"
            run_name_error = exc

        run_name = f"{run_prefix}__{run_suffix}"
        run_dir = runs_dir / run_name
        run_dir.mkdir(parents=True, exist_ok=False)

        try:
            if run_name_error is not None:
                raise ValueError(f"Invalid run naming template for run {run_prefix}: {run_name_error}")

            run_cfg = resolve_config(copy.deepcopy(merged_cfg))

            with (run_dir / "config_used.yaml").open("w", encoding="utf-8") as handle:
                yaml.safe_dump(run_cfg, handle, sort_keys=False)
            write_data_scope(
                run_dir,
                config=run_cfg,
                dataset_dir=data_path if Path(data_path).is_dir() else None,
            )

            datafeed = load_feed(data_path, run_cfg)
            engine = _build_engine(run_cfg, datafeed, run_dir)
            engine.run()

            report = compute_performance(run_dir)
            write_performance_artifacts(report, run_dir)

            performance_path = run_dir / "performance.json"
            if not performance_path.exists():
                raise RuntimeError(f"Missing performance.json for run '{run_name}' at {performance_path}")

            with performance_path.open("r", encoding="utf-8") as handle:
                perf_payload = json.load(handle)

            status_payload = {
                "status": "PASS",
                "error_type": "",
                "error_message": "",
                "traceback": "",
                "run_id": run_name,
            }
            with (run_dir / "run_status.json").open("w", encoding="utf-8") as handle:
                json.dump(status_payload, handle, indent=2, sort_keys=True)
                handle.write("\n")

            summary_rows.append(_build_summary_row(run_name, params, perf_payload, status="PASS"))
        except Exception as exc:
            tb = traceback.format_exc()
            status_payload = {
                "status": "FAIL",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "traceback": tb,
                "run_id": run_name,
            }
            with (run_dir / "run_status.json").open("w", encoding="utf-8") as handle:
                json.dump(status_payload, handle, indent=2, sort_keys=True)
                handle.write("\n")

            summary_rows.append(
                _build_summary_row(
                    run_name,
                    params,
                    {},
                    status="FAIL",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            )

    with (out_path / "summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_SUMMARY_COLUMNS)
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(row)

    with (out_path / "summary.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "metadata": {
                    "config": "<in-memory>",
                    "experiment": "<in-memory>",
                    "data": str(data_path),
                    "out": str(out_path),
                    "run_count": len(summary_rows),
                },
                "runs": summary_rows,
            },
            handle,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")

    return out_path
