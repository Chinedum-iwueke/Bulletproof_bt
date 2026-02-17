"""Deterministic experiment grid runner library API."""
from __future__ import annotations

import copy
import csv
from dataclasses import asdict
import itertools
import json
import re
import shutil
import traceback
from pathlib import Path
from typing import Any

import yaml

from bt.execution.profile import resolve_execution_profile
from bt.execution.intrabar import parse_intrabar_spec
from bt.logging.jsonl import to_jsonable
from bt.risk.stop_resolution import (
    STOP_RESOLUTION_ATR_MULTIPLE,
    STOP_RESOLUTION_EXPLICIT_STOP_PRICE,
    STOP_RESOLUTION_LABELS,
    STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY,
    STOP_RESOLUTION_UNRESOLVED,
)

from bt.benchmark.compare import compare_strategy_vs_benchmark
from bt.benchmark.metrics import compute_benchmark_metrics
from bt.benchmark.spec import parse_benchmark_spec
from bt.benchmark.tracker import BenchmarkTracker, BenchmarkTrackingFeed, write_benchmark_equity_csv
from bt.config import deep_merge
from bt.core.config_resolver import resolve_config
from bt.data.dataset import load_dataset_manifest
from bt.data.load_feed import load_feed
from bt.logging.sanity import SanityCounters, write_sanity_json
from bt.logging.trades import write_data_scope
from bt.metrics.performance import compute_performance, write_performance_artifacts
from bt.validation.config_completeness import validate_resolved_config_completeness


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


def _collect_run_stop_resolution(run_dir: Path) -> tuple[str, bool, bool, list[str]]:
    decisions_path = run_dir / "decisions.jsonl"
    observed_sources: set[str] = set()

    if decisions_path.exists():
        with decisions_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid decisions JSONL at {decisions_path}: {exc}") from exc
                order = payload.get("order")
                if not isinstance(order, dict):
                    continue
                metadata = order.get("metadata")
                if not isinstance(metadata, dict):
                    continue
                stop_source = metadata.get("stop_source")
                if stop_source is None:
                    continue
                if stop_source not in STOP_RESOLUTION_LABELS:
                    raise ValueError(
                        f"Invalid stop_source value {stop_source!r} in {decisions_path}; expected one of {sorted(STOP_RESOLUTION_LABELS)}"
                    )
                observed_sources.add(str(stop_source))

    if STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY in observed_sources:
        stop_resolution = STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY
    elif STOP_RESOLUTION_ATR_MULTIPLE in observed_sources:
        stop_resolution = STOP_RESOLUTION_ATR_MULTIPLE
    elif STOP_RESOLUTION_EXPLICIT_STOP_PRICE in observed_sources:
        stop_resolution = STOP_RESOLUTION_EXPLICIT_STOP_PRICE
    else:
        stop_resolution = STOP_RESOLUTION_UNRESOLVED

    used_legacy_stop_proxy = stop_resolution == STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY
    r_metrics_valid = stop_resolution in {STOP_RESOLUTION_EXPLICIT_STOP_PRICE, STOP_RESOLUTION_ATR_MULTIPLE}

    notes: list[str] = []
    if used_legacy_stop_proxy:
        notes.append(
            "Legacy stop proxy used because stop distance was not resolvable from signal or ATR config."
        )

    return stop_resolution, used_legacy_stop_proxy, r_metrics_valid, notes


def _write_run_status(run_dir: Path, status_payload: dict[str, Any]) -> None:
    path = run_dir / "run_status.json"
    existing_notes: list[str] = []
    if path.exists():
        try:
            existing_payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid existing run_status.json at {path}: {exc}") from exc
        maybe_notes = existing_payload.get("notes") if isinstance(existing_payload, dict) else None
        if maybe_notes is not None:
            if not isinstance(maybe_notes, list) or not all(isinstance(item, str) for item in maybe_notes):
                raise ValueError(f"Invalid notes in existing run_status.json at {path}; expected list[str].")
            existing_notes = list(maybe_notes)

    stop_resolution, used_legacy_stop_proxy, r_metrics_valid, derived_notes = _collect_run_stop_resolution(run_dir)
    merged_notes = list(existing_notes)
    for note in derived_notes:
        if note not in merged_notes:
            merged_notes.append(note)

    payload = dict(status_payload)
    payload["stop_resolution"] = stop_resolution
    payload["used_legacy_stop_proxy"] = used_legacy_stop_proxy
    payload["r_metrics_valid"] = r_metrics_valid
    payload["notes"] = merged_notes

    with path.open("w", encoding="utf-8") as handle:
        json.dump(to_jsonable(payload), handle, indent=2, sort_keys=True)
        handle.write("\n")


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
        sanity_counters = SanityCounters(run_id=run_name)

        try:
            if run_name_error is not None:
                raise ValueError(f"Invalid run naming template for run {run_prefix}: {run_name_error}")

            run_cfg = resolve_config(copy.deepcopy(merged_cfg))
            validate_resolved_config_completeness(run_cfg)

            with (run_dir / "config_used.yaml").open("w", encoding="utf-8") as handle:
                yaml.safe_dump(run_cfg, handle, sort_keys=False)
            write_data_scope(
                run_dir,
                config=run_cfg,
                dataset_dir=data_path if Path(data_path).is_dir() else None,
            )

            benchmark_spec = parse_benchmark_spec(run_cfg)
            benchmark_tracker: BenchmarkTracker | None = None
            if benchmark_spec.enabled:
                benchmark_symbol = benchmark_spec.symbol
                if Path(data_path).is_dir():
                    manifest = load_dataset_manifest(data_path, run_cfg)
                    if benchmark_symbol not in manifest.symbols:
                        raise ValueError(
                            f"benchmark.symbol={benchmark_symbol} not found in dataset scope for dataset_dir={data_path}"
                        )
                benchmark_tracker = BenchmarkTracker(benchmark_spec)

            datafeed = load_feed(data_path, run_cfg)
            if benchmark_tracker is not None:
                datafeed = BenchmarkTrackingFeed(inner_feed=datafeed, tracker=benchmark_tracker)

            engine = _build_engine(run_cfg, datafeed, run_dir, sanity_counters=sanity_counters)
            engine.run()

            benchmark_metrics: dict[str, Any] | None = None
            if benchmark_tracker is not None:
                benchmark_initial_equity = (
                    benchmark_spec.initial_equity
                    if benchmark_spec.initial_equity is not None
                    else float(run_cfg.get("initial_cash", 100000.0))
                )
                benchmark_points = benchmark_tracker.finalize(initial_equity=benchmark_initial_equity)
                write_benchmark_equity_csv(benchmark_points, run_dir / "benchmark_equity.csv")
                benchmark_metrics = compute_benchmark_metrics(equity_points=benchmark_points)
                (run_dir / "benchmark_metrics.json").write_text(
                    json.dumps(benchmark_metrics, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )

            report = compute_performance(run_dir)
            write_performance_artifacts(report, run_dir)

            if benchmark_spec.enabled:
                if benchmark_metrics is None:
                    raise ValueError(
                        f"benchmark enabled but benchmark_metrics.json was not produced for run_dir={run_dir}"
                    )
                comparison_summary = compare_strategy_vs_benchmark(
                    strategy_perf=asdict(report),
                    bench_metrics=benchmark_metrics,
                )
                (run_dir / "comparison_summary.json").write_text(
                    json.dumps(comparison_summary, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )

            performance_path = run_dir / "performance.json"
            if not performance_path.exists():
                raise RuntimeError(f"Missing performance.json for run '{run_name}' at {performance_path}")

            with performance_path.open("r", encoding="utf-8") as handle:
                perf_payload = json.load(handle)

            profile = resolve_execution_profile(run_cfg)
            status_payload = {
                "status": "PASS",
                "error_type": "",
                "error_message": "",
                "traceback": "",
                "run_id": run_name,
                "execution_profile": profile.name,
                "effective_execution": {
                    "maker_fee": profile.maker_fee,
                    "taker_fee": profile.taker_fee,
                    "slippage_bps": profile.slippage_bps,
                    "delay_bars": profile.delay_bars,
                    "spread_bps": profile.spread_bps,
                },
                "intrabar_mode": parse_intrabar_spec(run_cfg).mode,
            }
            _write_run_status(run_dir, status_payload)

            summary_rows.append(_build_summary_row(run_name, params, perf_payload, status="PASS"))
        except Exception as exc:
            tb = traceback.format_exc()
            try:
                intrabar_mode = parse_intrabar_spec(merged_cfg).mode
            except ValueError:
                intrabar_mode = "worst_case"

            fail_profile_payload: dict[str, Any] = {}
            try:
                profile = resolve_execution_profile(merged_cfg)
            except ValueError:
                profile = None
            if profile is not None:
                fail_profile_payload = {
                    "execution_profile": profile.name,
                    "effective_execution": {
                        "maker_fee": profile.maker_fee,
                        "taker_fee": profile.taker_fee,
                        "slippage_bps": profile.slippage_bps,
                        "delay_bars": profile.delay_bars,
                        "spread_bps": profile.spread_bps,
                    },
                    "intrabar_mode": intrabar_mode,
                }

            status_payload = {
                "status": "FAIL",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "traceback": tb,
                "run_id": run_name,
                "intrabar_mode": intrabar_mode,
                **fail_profile_payload,
            }
            _write_run_status(run_dir, status_payload)

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
        finally:
            data_scope_payload: dict[str, Any] | None = None
            data_scope_path = run_dir / "data_scope.json"
            if data_scope_path.exists():
                try:
                    raw_data_scope = json.loads(data_scope_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    raw_data_scope = None
                if isinstance(raw_data_scope, dict):
                    date_range = raw_data_scope.get("date_range")
                    if isinstance(date_range, dict):
                        data_scope_payload = {
                            "data_start_ts": date_range.get("start"),
                            "data_end_ts": date_range.get("end"),
                        }
            write_sanity_json(run_dir, sanity_counters, data_scope=data_scope_payload)

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
