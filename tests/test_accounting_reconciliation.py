from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.core.engine import BacktestEngine
from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.metrics.performance import compute_performance
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy
from bt.universe.universe import UniverseEngine


class EntryOnlyStrategy(Strategy):
    def __init__(self) -> None:
        self._entered = False

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list[Signal]:
        if self._entered:
            return []
        self._entered = True
        return [Signal(ts=ts, symbol="AAA", side=Side.BUY, signal_type="entry", confidence=1.0, metadata={"stop_price": 95.0})]


def _run_backtest(tmp_path: Path) -> Path:
    ts_index = pd.date_range("2024-01-01", periods=4, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "ts": ts_index,
            "symbol": ["AAA"] * 4,
            "open": [100.0, 102.0, 104.0, 103.0],
            "high": [101.0, 103.0, 105.0, 104.0],
            "low": [99.0, 101.0, 103.0, 102.0],
            "close": [100.5, 102.5, 104.5, 103.5],
            "volume": [1000.0, 1000.0, 1000.0, 1000.0],
        }
    )

    run_dir = tmp_path / "run"
    run_dir.mkdir()

    engine = BacktestEngine(
        datafeed=HistoricalDataFeed(df),
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=EntryOnlyStrategy(),
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}}),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=10.0, taker_fee_bps=10.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=10_000.0, max_leverage=2.0),
        decisions_writer=JsonlWriter(run_dir / "decisions.jsonl"),
        fills_writer=JsonlWriter(run_dir / "fills.jsonl"),
        trades_writer=TradesCsvWriter(run_dir / "trades.csv"),
        equity_path=run_dir / "equity.csv",
        config={},
    )
    engine.run()
    return run_dir


def test_portfolio_identity_holds_per_snapshot(tmp_path: Path) -> None:
    run_dir = _run_backtest(tmp_path)
    equity_df = pd.read_csv(run_dir / "equity.csv")

    lhs = equity_df["equity"]
    rhs = equity_df["cash"] + equity_df["realized_pnl"] + equity_df["unrealized_pnl"]
    assert (lhs - rhs).abs().max() <= 1e-8


def test_final_equity_reconciles_with_trade_net_pnl_when_flat(tmp_path: Path) -> None:
    run_dir = _run_backtest(tmp_path)
    equity_df = pd.read_csv(run_dir / "equity.csv")
    trades_df = pd.read_csv(run_dir / "trades.csv")

    final_equity = float(equity_df["equity"].iloc[-1])
    initial_equity = 10_000.0
    total_trade_net = float(trades_df["pnl_net"].sum())

    assert final_equity == pytest.approx(initial_equity + total_trade_net, abs=1e-8)


def test_performance_totals_match_trade_and_equity_outputs(tmp_path: Path) -> None:
    run_dir = _run_backtest(tmp_path)

    performance = compute_performance(run_dir)
    equity_df = pd.read_csv(run_dir / "equity.csv")
    trades_df = pd.read_csv(run_dir / "trades.csv")

    assert performance.final_equity == pytest.approx(float(equity_df["equity"].iloc[-1]), abs=1e-8)
    assert performance.net_pnl == pytest.approx(float(trades_df["pnl_net"].sum()), abs=1e-8)
    assert performance.gross_pnl == pytest.approx(float(trades_df["pnl_price"].sum()), abs=1e-8)
