"""Deterministic experiment grid runner."""
from __future__ import annotations

import argparse
import copy
import csv
import itertools
import json
import re
import shutil
import sys
import traceback
from pathlib import Path
from typing import Any

import yaml

from bt.core.engine import BacktestEngine
from bt.core.config_resolver import resolve_config
from bt.data.load_feed import load_feed
from bt.data.resample import TimeframeResampler
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter, write_data_scope
from bt.metrics.performance import compute_performance, write_performance_artifacts
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy import make_strategy
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.universe.universe import UniverseEngine


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


def _load_yaml_file(path: str) -> dict[str, Any]:
    yaml_path = Path(path)
    with yaml_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML mapping at {yaml_path}")
    return data


def _load_yaml(path: Path) -> dict[str, Any]:
    return _load_yaml_file(str(path))


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


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


def _build_strategy(config: dict[str, Any]):
    strategy_cfg = config.get("strategy")
    if not isinstance(strategy_cfg, dict):
        raise ValueError("Strategy config missing 'strategy' mapping")

    strategy_name = strategy_cfg.get("name")
    if not strategy_name:
        raise ValueError("Strategy config missing strategy.name")

    strategy_kwargs = {k: v for k, v in strategy_cfg.items() if k != "name"}
    if strategy_name == "coinflip":
        strategy_kwargs.setdefault("p_trade", float(config.get("p_trade", 0.2)))
        strategy_kwargs.setdefault("cooldown_bars", int(config.get("cooldown_bars", 0)))
    if strategy_name == "volfloor_donchian":
        if "entry_lookback" in strategy_kwargs and "donchian_entry_lookback" not in strategy_kwargs:
            strategy_kwargs["donchian_entry_lookback"] = strategy_kwargs.pop("entry_lookback")
        if "exit_lookback" in strategy_kwargs and "donchian_exit_lookback" not in strategy_kwargs:
            strategy_kwargs["donchian_exit_lookback"] = strategy_kwargs.pop("exit_lookback")
        if "vol_window_days" in strategy_kwargs and "vol_lookback_bars" not in strategy_kwargs:
            strategy_kwargs["vol_lookback_bars"] = int(float(strategy_kwargs.pop("vol_window_days")) * 24 * 4)

    return make_strategy(
        strategy_name,
        seed=int(strategy_kwargs.pop("seed", config.get("seed", 42))),
        **strategy_kwargs,
    )


def _ensure_timeframe_config(config: dict[str, Any]) -> None:
    timeframe = config.get("timeframe")
    if timeframe is None:
        return

    htf_resampler = config.get("htf_resampler")
    has_structured_resampler = isinstance(htf_resampler, dict) and bool(htf_resampler.get("enabled"))
    htf_timeframes = config.get("htf_timeframes")
    has_timeframe_list = isinstance(htf_timeframes, list) and str(timeframe) in {
        str(item) for item in htf_timeframes
    }

    if not has_structured_resampler and not has_timeframe_list:
        raise ValueError(
            f"HTF resampler not enabled; set config.htf_resampler.enabled=true and timeframe={timeframe}"
        )


def _build_engine(config: dict[str, Any], datafeed, run_dir: Path) -> BacktestEngine:
    htf_timeframes = config.get("htf_timeframes")
    if htf_timeframes:
        config = copy.deepcopy(config)
        config["htf_resampler"] = TimeframeResampler(
            timeframes=[str(tf) for tf in htf_timeframes],
            strict=bool(config.get("htf_strict", True)),
        )

    universe = UniverseEngine(
        min_history_bars=int(config.get("min_history_bars", 1)),
        lookback_bars=int(config.get("lookback_bars", 1)),
        min_avg_volume=float(config.get("min_avg_volume", 0.0)),
        lag_bars=int(config.get("lag_bars", 0)),
    )
    strategy = _build_strategy(config)
    htf_resampler = config.get("htf_resampler")
    if isinstance(htf_resampler, TimeframeResampler):
        strategy = HTFContextStrategyAdapter(inner=strategy, resampler=htf_resampler)
    risk_cfg = config.get("risk", {}) if isinstance(config.get("risk"), dict) else {}
    risk = RiskEngine(
        max_positions=int(risk_cfg.get("max_positions", 1)),
        risk_per_trade_pct=float(risk_cfg.get("risk_per_trade_pct", 0.01)),
        max_notional_per_symbol=config.get("max_notional_per_symbol"),
        margin_buffer_tier=int(risk_cfg.get("margin_buffer_tier", 1)),
        taker_fee_bps=float(config.get("taker_fee_bps", 0.0)),
        slippage_k_proxy=float(config.get("slippage_k", 0.0)),
    )

    fee_model = FeeModel(
        maker_fee_bps=float(config.get("maker_fee_bps", 0.0)),
        taker_fee_bps=float(config.get("taker_fee_bps", 0.0)),
    )
    slippage_model = SlippageModel(
        k=float(config.get("slippage_k", 1.0)),
        atr_pct_cap=float(config.get("atr_pct_cap", 0.20)),
        impact_cap=float(config.get("impact_cap", 0.05)),
    )
    execution = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=int(config.get("signal_delay_bars", 1)),
    )

    portfolio = Portfolio(
        initial_cash=float(config.get("initial_cash", 100000.0)),
        max_leverage=float(config.get("max_leverage", 2.0)),
    )

    return BacktestEngine(
        datafeed=datafeed,
        universe=universe,
        strategy=strategy,
        risk=risk,
        execution=execution,
        portfolio=portfolio,
        decisions_writer=JsonlWriter(run_dir / "decisions.jsonl"),
        fills_writer=JsonlWriter(run_dir / "fills.jsonl"),
        trades_writer=TradesCsvWriter(run_dir / "trades.csv"),
        equity_path=run_dir / "equity.csv",
        config=config,
    )


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
    config_path: Path,
    experiment_path: Path,
    data_path: str,
    out_path: Path,
    *,
    local_config_path: str | None = None,
    force: bool = False,
    allow_failures: bool = False,
) -> int:
    base_cfg = _load_yaml(config_path)
    local_cfg: dict[str, Any] = {}
    if local_config_path is not None:
        local_path = Path(local_config_path)
        if not local_path.exists():
            raise ValueError(f"Invalid --local-config: file does not exist: {local_path}")
        if local_path.suffix.lower() not in {".yaml", ".yml"}:
            raise ValueError(
                f"Invalid --local-config: expected a .yaml/.yml file, got: {local_path}"
            )
        local_cfg = _load_yaml_file(str(local_path))

    exp_cfg = _load_yaml(experiment_path)
    _validate_experiment(exp_cfg)

    runs_dir = out_path / "runs"
    if runs_dir.exists() and any(runs_dir.iterdir()) and not force:
        raise RuntimeError(f"Output already contains runs: {runs_dir}")
    if force and out_path.exists():
        shutil.rmtree(out_path)

    out_path.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    grid = exp_cfg["grid"]
    grid_runs = _expand_grid(grid)
    sorted_keys = sorted(grid.keys())

    with (out_path / "grid_used.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "experiment": exp_cfg,
                "resolved_grid_keys": sorted_keys,
                "grid_runs": grid_runs,
                "paths": {
                    "config": str(config_path),
                    "local_config": local_config_path,
                    "experiment": str(experiment_path),
                    "data": str(data_path),
                },
            },
            handle,
            sort_keys=False,
        )

    run_template = (
        exp_cfg.get("run_naming", {}).get("template")
        if isinstance(exp_cfg.get("run_naming"), dict)
        else None
    ) or "run"
    fixed_overrides = exp_cfg.get("fixed") or {}

    summary_rows: list[dict[str, Any]] = []

    for index, params in enumerate(grid_runs, start=1):
        run_prefix = f"run_{index:03d}"
        overrides = copy.deepcopy(fixed_overrides)
        for dotpath, value in params.items():
            set_by_dotpath(overrides, dotpath, value)

        merged_cfg = deep_merge(base_cfg, local_cfg)
        merged_cfg = deep_merge(merged_cfg, overrides)

        run_suffix = "run"
        run_name_error: Exception | None = None
        try:
            run_suffix = _render_run_suffix(run_template, merged_cfg)
        except Exception as exc:  # pragma: no cover - rare invalid run_naming configs
            run_suffix = "template_error"
            run_name_error = exc

        run_name = f"{run_prefix}__{run_suffix}"
        run_dir = runs_dir / run_name
        run_dir.mkdir(parents=True, exist_ok=False)

        try:
            if run_name_error is not None:
                raise ValueError(
                    f"Invalid run naming template for run {run_prefix}: {run_name_error}"
                )

            run_cfg = resolve_config(merged_cfg)
            _ensure_timeframe_config(run_cfg)

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

            summary_rows.append(
                _build_summary_row(
                    run_name,
                    params,
                    perf_payload,
                    status="PASS",
                )
            )
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
                    "config": str(config_path),
                    "experiment": str(experiment_path),
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

    fail_count = sum(1 for row in summary_rows if row.get("status") == "FAIL")
    if fail_count and not allow_failures:
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic backtest experiment grid")
    parser.add_argument("--config", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--local-config")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-failures", action="store_true")
    args = parser.parse_args()

    exit_code = run_grid(
        config_path=Path(args.config),
        experiment_path=Path(args.experiment),
        data_path=args.data,
        out_path=Path(args.out),
        local_config_path=args.local_config,
        force=args.force,
        allow_failures=args.allow_failures,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
