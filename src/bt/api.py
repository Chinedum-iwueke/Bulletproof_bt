"""Public product API for running backtests and experiment grids."""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from bt.config import deep_merge, load_yaml, resolve_paths_relative_to
from bt.core.config_resolver import resolve_config


def _build_engine(config: dict[str, Any], datafeed: Any, run_dir: Path):
    from bt.core.engine import BacktestEngine
    from bt.data.resample import TimeframeResampler
    from bt.execution.execution_model import ExecutionModel
    from bt.execution.fees import FeeModel
    from bt.execution.slippage import SlippageModel
    from bt.logging.jsonl import JsonlWriter
    from bt.logging.trades import TradesCsvWriter
    from bt.portfolio.portfolio import Portfolio
    from bt.risk.risk_engine import RiskEngine
    from bt.risk.spec import parse_risk_spec
    from bt.strategy import make_strategy
    from bt.strategy.htf_context import HTFContextStrategyAdapter, ReadOnlyContextStrategyAdapter
    from bt.universe.universe import UniverseEngine

    universe = UniverseEngine(
        min_history_bars=int(config.get("min_history_bars", 1)),
        lookback_bars=int(config.get("lookback_bars", 1)),
        min_avg_volume=float(config.get("min_avg_volume", 0.0)),
        lag_bars=int(config.get("lag_bars", 0)),
    )

    strategy_cfg = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
    strategy_name = strategy_cfg.get("name", "coinflip")
    strategy_kwargs = {k: v for k, v in strategy_cfg.items() if k != "name"}
    if strategy_name == "volfloor_donchian":
        if "entry_lookback" in strategy_kwargs and "donchian_entry_lookback" not in strategy_kwargs:
            strategy_kwargs["donchian_entry_lookback"] = strategy_kwargs.pop("entry_lookback")
        if "exit_lookback" in strategy_kwargs and "donchian_exit_lookback" not in strategy_kwargs:
            strategy_kwargs["donchian_exit_lookback"] = strategy_kwargs.pop("exit_lookback")
        if "vol_window_days" in strategy_kwargs and "vol_lookback_bars" not in strategy_kwargs:
            strategy_kwargs["vol_lookback_bars"] = int(float(strategy_kwargs.pop("vol_window_days")) * 24 * 4)
    strategy = make_strategy(
        strategy_name,
        seed=int(strategy_kwargs.pop("seed", config.get("seed", 42))),
        **strategy_kwargs,
    )
    strategy = ReadOnlyContextStrategyAdapter(inner=strategy)

    htf_resampler = config.get("htf_resampler")
    if isinstance(htf_resampler, dict):
        htf_resampler = TimeframeResampler(
            timeframes=[str(tf) for tf in htf_resampler.get("timeframes", [])],
            strict=bool(htf_resampler.get("strict", True)),
        )
    if isinstance(htf_resampler, TimeframeResampler):
        strategy = HTFContextStrategyAdapter(inner=strategy, resampler=htf_resampler)

    if not isinstance(config.get("risk"), dict):
        raise ValueError("risk.mode and risk.r_per_trade are required")
    risk_cfg = config["risk"]
    risk_cfg_for_spec = dict(risk_cfg)
    if "mode" not in risk_cfg_for_spec:
        risk_cfg_for_spec["mode"] = "equity_pct"
    if "r_per_trade" not in risk_cfg_for_spec:
        if "risk_per_trade_pct" in risk_cfg_for_spec:
            risk_cfg_for_spec["r_per_trade"] = risk_cfg_for_spec["risk_per_trade_pct"]
        elif "risk_per_trade_pct" in config:
            risk_cfg_for_spec["r_per_trade"] = config["risk_per_trade_pct"]
    risk_spec = parse_risk_spec({"risk": risk_cfg_for_spec})

    risk = RiskEngine(
        max_positions=int(risk_cfg.get("max_positions", 5)),
        max_notional_per_symbol=config.get("max_notional_per_symbol"),
        margin_buffer_tier=int(risk_cfg.get("margin_buffer_tier", 1)),
        taker_fee_bps=float(config.get("taker_fee_bps", 0.0)),
        slippage_k_proxy=float(risk_cfg.get("slippage_k_proxy", 0.0)),
        config={"risk": risk_cfg_for_spec},
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

    portfolio_max_leverage = risk_spec.max_leverage
    if portfolio_max_leverage is None:
        portfolio_max_leverage = float(config.get("max_leverage", 2.0))

    portfolio = Portfolio(
        initial_cash=float(config.get("initial_cash", 100000.0)),
        max_leverage=portfolio_max_leverage,
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


def run_backtest(
    *,
    config_path: str,
    data_path: str,
    out_dir: str,
    override_paths: Optional[list[str]] = None,
    run_name: Optional[str] = None,
) -> str:
    """
    Runs a single backtest and returns the created run directory path.
    """
    from bt.benchmark.compare import compare_strategy_vs_benchmark
    from bt.benchmark.metrics import compute_benchmark_metrics
    from bt.benchmark.spec import parse_benchmark_spec
    from bt.benchmark.tracker import BenchmarkTracker, BenchmarkTrackingFeed, write_benchmark_equity_csv
    from bt.data.dataset import load_dataset_manifest
    from bt.data.load_feed import load_feed
    from bt.logging.trades import make_run_id, prepare_run_dir, write_config_used, write_data_scope
    from bt.metrics.performance import compute_performance, write_performance_artifacts

    base_config = load_yaml(config_path)
    fees_config = load_yaml("configs/fees.yaml")
    slippage_config = load_yaml("configs/slippage.yaml")
    config = deep_merge(base_config, fees_config)
    config = deep_merge(config, slippage_config)
    for override_path in resolve_paths_relative_to(Path(config_path).parent, override_paths):
        config = deep_merge(config, load_yaml(override_path))
    config = resolve_config(config)

    resolved_run_name = run_name or make_run_id()
    run_dir = prepare_run_dir(Path(out_dir), resolved_run_name)
    write_config_used(run_dir, config)
    write_data_scope(
        run_dir,
        config=config,
        dataset_dir=data_path if Path(data_path).is_dir() else None,
    )

    benchmark_spec = parse_benchmark_spec(config)
    benchmark_tracker: BenchmarkTracker | None = None
    if benchmark_spec.enabled:
        benchmark_symbol = benchmark_spec.symbol
        if Path(data_path).is_dir():
            manifest = load_dataset_manifest(data_path, config)
            if benchmark_symbol not in manifest.symbols:
                raise ValueError(
                    f"benchmark.symbol={benchmark_symbol} not found in dataset scope for dataset_dir={data_path}"
                )
        benchmark_tracker = BenchmarkTracker(benchmark_spec)

    datafeed = load_feed(data_path, config)
    benchmark_metrics: dict[str, Any] | None = None
    if benchmark_tracker is not None:
        datafeed = BenchmarkTrackingFeed(inner_feed=datafeed, tracker=benchmark_tracker)

    engine = _build_engine(config, datafeed, run_dir)
    engine.run()

    if benchmark_tracker is not None:
        benchmark_initial_equity = (
            benchmark_spec.initial_equity
            if benchmark_spec.initial_equity is not None
            else float(config.get("initial_cash", 100000.0))
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

    return str(run_dir)


def run_grid(
    *,
    config_path: str,
    experiment_path: str,
    data_path: str,
    out_dir: str,
    override_paths: Optional[list[str]] = None,
    experiment_name: Optional[str] = None,
) -> str:
    """
    Runs an experiment grid and returns the created experiment directory path.
    """
    from bt.experiments.grid_runner import run_grid as run_grid_library

    base = load_yaml(config_path)
    fees = load_yaml("configs/fees.yaml")
    slippage = load_yaml("configs/slippage.yaml")
    config = deep_merge(base, fees)
    config = deep_merge(config, slippage)

    for override_path in resolve_paths_relative_to(Path(config_path).parent, override_paths):
        config = deep_merge(config, load_yaml(override_path))

    experiment_cfg = load_yaml(experiment_path)
    resolved_experiment_name = experiment_name or str(experiment_cfg.get("name") or "experiment")
    experiment_dir = Path(out_dir) / resolved_experiment_name

    run_grid_library(
        config=config,
        experiment_cfg=experiment_cfg,
        data_path=data_path,
        out_path=experiment_dir,
        force=False,
    )

    return str(experiment_dir)
