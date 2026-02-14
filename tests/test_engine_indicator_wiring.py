"""Smoke tests for indicator wiring into the engine context."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from bt.core.engine import BacktestEngine
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
from bt.core.types import Bar, Signal


class IndicatorContextStrategy(Strategy):
    def __init__(self) -> None:
        self.ema_ready_flags: list[bool] = []

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        indicators = ctx["indicators"]
        assert "AAA" in indicators
        symbol_indicators = indicators["AAA"]
        assert set(symbol_indicators.keys()) == {"ema_20", "ema_50", "atr_14", "vwap"}
        ema_value, ema_ready = symbol_indicators["ema_20"]
        if ema_ready:
            assert ema_value is not None
        self.ema_ready_flags.append(ema_ready)
        return []


def _make_bars_df() -> pd.DataFrame:
    ts_index = pd.date_range("2024-01-01", periods=25, freq="min", tz="UTC")
    rows = []
    for idx, ts in enumerate(ts_index):
        base = 100 + idx
        rows.append(
            {
                "ts": ts,
                "symbol": "AAA",
                "open": base,
                "high": base + 1,
                "low": base - 1,
                "close": base + 0.5,
                "volume": 1000.0 + idx,
            }
        )
    return pd.DataFrame(rows)


def test_engine_indicator_context_wiring(tmp_path: Path) -> None:
    bars_df = _make_bars_df()
    datafeed = HistoricalDataFeed(bars_df)

    universe = UniverseEngine(
        min_history_bars=1,
        lookback_bars=1,
        min_avg_volume=0.0,
        lag_bars=0,
    )

    strategy = IndicatorContextStrategy()
    risk = RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}})

    fee_model = FeeModel(maker_fee_bps=1.0, taker_fee_bps=2.0)
    slippage_model = SlippageModel(k=0.01)
    execution = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=1,
    )

    portfolio = Portfolio(initial_cash=10000.0, max_leverage=2.0)

    decisions_path = tmp_path / "decisions.jsonl"
    fills_path = tmp_path / "fills.jsonl"
    trades_path = tmp_path / "trades.csv"
    equity_path = tmp_path / "equity.csv"

    decisions_writer = JsonlWriter(decisions_path)
    fills_writer = JsonlWriter(fills_path)
    trades_writer = TradesCsvWriter(trades_path)

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
        config={},
    )
    engine.run()

    assert strategy.ema_ready_flags[18] is False
    assert strategy.ema_ready_flags[19] is True
