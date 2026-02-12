"""Public product API for running backtests and experiment grids."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional


def _load_yaml_mapping(path: Path, *, missing_message: str, parse_message: str) -> dict[str, Any]:
    import yaml

    if not path.exists():
        raise ValueError(f"{missing_message}: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - defensive parse mapping
        raise ValueError(f"{parse_message}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"{parse_message}: expected a YAML mapping")
    return data


def _merge_config(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    merged.update(update)
    return merged


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
    from bt.strategy import make_strategy
    from bt.strategy.htf_context import HTFContextStrategyAdapter
    from bt.universe.universe import UniverseEngine

    htf_timeframes = config.get("htf_timeframes")
    if htf_timeframes:
        config = dict(config)
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

    htf_resampler = config.get("htf_resampler")
    if isinstance(htf_resampler, TimeframeResampler):
        strategy = HTFContextStrategyAdapter(inner=strategy, resampler=htf_resampler)

    risk = RiskEngine(
        max_positions=int(config.get("max_positions", 5)),
        risk_per_trade_pct=float(config.get("risk_per_trade_pct", 0.01)),
        max_notional_per_symbol=config.get("max_notional_per_symbol"),
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


def run_backtest(
    *,
    config_path: str,
    data_path: str,
    out_dir: str,
    run_name: Optional[str] = None,
) -> str:
    """
    Runs a single backtest and returns the created run directory path.
    """
    from bt.data.load_feed import load_feed
    from bt.logging.trades import make_run_id, prepare_run_dir, write_config_used, write_data_scope
    from bt.metrics.performance import compute_performance, write_performance_artifacts

    config_file = Path(config_path)
    base_config = _load_yaml_mapping(
        config_file,
        missing_message="Config path not found",
        parse_message="Failed to parse YAML config",
    )
    fees_config = _load_yaml_mapping(
        Path("configs/fees.yaml"),
        missing_message="Config path not found",
        parse_message="Failed to parse YAML config",
    )
    slippage_config = _load_yaml_mapping(
        Path("configs/slippage.yaml"),
        missing_message="Config path not found",
        parse_message="Failed to parse YAML config",
    )

    config = _merge_config(base_config, fees_config)
    config = _merge_config(config, slippage_config)

    resolved_run_name = run_name or make_run_id()
    run_dir = prepare_run_dir(Path(out_dir), resolved_run_name)
    write_config_used(run_dir, config)
    write_data_scope(
        run_dir,
        config=config,
        dataset_dir=data_path if Path(data_path).is_dir() else None,
    )

    datafeed = load_feed(data_path, config)
    engine = _build_engine(config, datafeed, run_dir)
    engine.run()

    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)

    return str(run_dir)


def run_grid(
    *,
    config_path: str,
    experiment_path: str,
    data_path: str,
    out_dir: str,
    experiment_name: Optional[str] = None,
) -> str:
    """
    Runs an experiment grid and returns the created experiment directory path.
    """
    config_file = Path(config_path)
    experiment_file = Path(experiment_path)

    _load_yaml_mapping(
        config_file,
        missing_message="Config path not found",
        parse_message="Failed to parse YAML config",
    )
    experiment_cfg = _load_yaml_mapping(
        experiment_file,
        missing_message="Experiment path not found",
        parse_message="Failed to parse YAML config",
    )

    try:
        from scripts.run_experiment_grid import run_grid as run_grid_script
    except Exception as exc:  # pragma: no cover - only if packaging/layout changes
        raise NotImplementedError("Grid runner script is not importable in this environment") from exc

    resolved_experiment_name = experiment_name or str(experiment_cfg.get("name") or "experiment")
    experiment_dir = Path(out_dir) / resolved_experiment_name
    run_grid_script(
        config_path=config_file,
        experiment_path=experiment_file,
        data_path=data_path,
        out_path=experiment_dir,
        force=False,
    )

    return str(experiment_dir)
