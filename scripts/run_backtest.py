"""CLI entrypoint for v1 backtests."""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from bt.core.engine import BacktestEngine
from bt.data.feed import HistoricalDataFeed
from bt.data.loader import load_dataset
from bt.data.resample import TimeframeResampler
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter, make_run_id, prepare_run_dir, write_config_used
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.coinflip import CoinFlipStrategy
from bt.universe.universe import UniverseEngine


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid config at {path}")
    return data


def _merge_config(base: dict, update: dict) -> dict:
    merged = dict(base)
    merged.update(update)
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description="Run backtest (v1).")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--data", required=True)
    parser.add_argument("--run-id")
    args = parser.parse_args()

    config_path = Path(args.config)
    base_config = _load_yaml(config_path)
    fees_config = _load_yaml(Path("configs/fees.yaml"))
    slippage_config = _load_yaml(Path("configs/slippage.yaml"))
    config = _merge_config(base_config, fees_config)
    config = _merge_config(config, slippage_config)

    htf_timeframes = config.get("htf_timeframes")
    if htf_timeframes:
        config["htf_resampler"] = TimeframeResampler(
            timeframes=[str(tf) for tf in htf_timeframes],
            strict=bool(config.get("htf_strict", True)),
        )

    run_id = args.run_id or make_run_id()
    run_dir = prepare_run_dir(Path("outputs/runs"), run_id)
    write_config_used(run_dir, config)

    dataset_path = args.data
    bars_df = load_dataset(dataset_path)
    datafeed = HistoricalDataFeed(bars_df)

    universe = UniverseEngine(
        min_history_bars=int(config.get("min_history_bars", 1)),
        lookback_bars=int(config.get("lookback_bars", 1)),
        min_avg_volume=float(config.get("min_avg_volume", 0.0)),
        lag_bars=int(config.get("lag_bars", 0)),
    )

    strategy = CoinFlipStrategy(
        seed=int(config.get("seed", 42)),
        p_trade=float(config.get("p_trade", 0.2)),
        cooldown_bars=int(config.get("cooldown_bars", 0)),
    )

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

    decisions_writer = JsonlWriter(run_dir / "decisions.jsonl")
    fills_writer = JsonlWriter(run_dir / "fills.jsonl")
    trades_writer = TradesCsvWriter(run_dir / "trades.csv")
    equity_path = run_dir / "equity.csv"

    engine = BacktestEngine(
        datafeed=datafeed,
        universe=universe,
        strategy=strategy,
        risk=risk,
        execution=execution,
        portfolio=portfolio,
        decisions_writer=decisions_writer,
        fills_writer=fills_writer,
        trades_writer=trades_writer,
        equity_path=equity_path,
        config=config,
    )
    engine.run()

    print(f"run_dir: {run_dir}")
    print(f"final_equity: {portfolio.equity}")


if __name__ == "__main__":
    main()
