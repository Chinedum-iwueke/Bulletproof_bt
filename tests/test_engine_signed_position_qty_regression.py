from __future__ import annotations

import pandas as pd

from bt.core.engine import BacktestEngine
from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy
from bt.universe.universe import UniverseEngine


class ShortThenExitStrategy(Strategy):
    def __init__(self) -> None:
        self._step = 0

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list[Signal]:
        if self._step == 0:
            self._step += 1
            return [Signal(ts=ts, symbol="AAA", side=Side.SELL, signal_type="entry", confidence=1.0, metadata={"stop_price": 110.0})]
        if self._step == 1:
            self._step += 1
            return [Signal(ts=ts, symbol="AAA", side=Side.BUY, signal_type="h1_volfloor_donchian_exit", confidence=1.0, metadata={"is_exit": True})]
        return []


def _engine(tmp_path) -> BacktestEngine:
    ts_index = pd.date_range("2024-01-01", periods=3, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "ts": ts_index,
            "symbol": ["AAA", "AAA", "AAA"],
            "open": [100.0, 100.0, 100.0],
            "high": [101.0, 101.0, 101.0],
            "low": [99.0, 99.0, 99.0],
            "close": [100.0, 100.0, 100.0],
            "volume": [1000.0, 1000.0, 1000.0],
        }
    )
    return BacktestEngine(
        datafeed=HistoricalDataFeed(df),
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=ShortThenExitStrategy(),
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}}),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=10000.0, max_leverage=2.0),
        decisions_writer=JsonlWriter(tmp_path / "decisions.jsonl"),
        fills_writer=JsonlWriter(tmp_path / "fills.jsonl"),
        trades_writer=TradesCsvWriter(tmp_path / "trades.csv"),
        equity_path=tmp_path / "equity.csv",
        config={},
    )


def test_short_exit_closes_not_flips(tmp_path) -> None:
    engine = _engine(tmp_path)
    engine.run()

    position = engine._portfolio.position_book.get("AAA")
    assert position.qty == 0.0

    trades_lines = (tmp_path / "trades.csv").read_text(encoding="utf-8").strip().splitlines()
    assert len(trades_lines) == 2

