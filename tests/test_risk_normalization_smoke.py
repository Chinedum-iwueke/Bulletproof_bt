from __future__ import annotations

import json
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
from bt.metrics.performance import compute_performance, write_performance_artifacts
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy
from bt.universe.universe import UniverseEngine


class _AlternatingStopStrategy(Strategy):
    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
        del tradeable, ctx
        bar = bars_by_symbol["AAA"]
        idx = int(bar.close)
        side = Side.BUY if idx % 2 == 0 else Side.SELL
        stop_price = bar.close - 1.0 if side == Side.BUY else bar.close + 1.0
        return [
            Signal(
                ts=ts,
                symbol="AAA",
                side=side,
                signal_type="entry",
                confidence=1.0,
                metadata={"stop_price": stop_price},
            )
        ]


class _AlternatingNoStopStrategy(Strategy):
    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
        del tradeable, ctx
        bar = bars_by_symbol["AAA"]
        idx = int(bar.close)
        side = Side.BUY if idx % 2 == 0 else Side.SELL
        return [
            Signal(
                ts=ts,
                symbol="AAA",
                side=side,
                signal_type="entry",
                confidence=1.0,
                metadata={},
            )
        ]


def _bars_df() -> pd.DataFrame:
    ts_index = pd.date_range("2024-01-01", periods=10, freq="D", tz="UTC")
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
                "close": float(base),
                "volume": 1000.0,
            }
        )
    return pd.DataFrame(rows)


def _run(tmp_path: Path, strategy: Strategy) -> Path:
    run_dir = tmp_path / "run"
    datafeed = HistoricalDataFeed(_bars_df())
    engine = BacktestEngine(
        datafeed=datafeed,
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=strategy,
        risk=RiskEngine(
            max_positions=1,
            risk_per_trade_pct=0.01,
            config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}},
        ),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=1.0, taker_fee_bps=2.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=10000.0, max_leverage=5.0),
        decisions_writer=JsonlWriter(run_dir / "decisions.jsonl"),
        fills_writer=JsonlWriter(run_dir / "fills.jsonl"),
        trades_writer=TradesCsvWriter(run_dir / "trades.csv"),
        equity_path=run_dir / "equity.csv",
        config={},
    )
    engine.run()
    report = compute_performance(run_dir)
    write_performance_artifacts(report, run_dir)
    return run_dir


def test_risk_normalization_smoke(tmp_path: Path) -> None:
    run_dir = _run(tmp_path, _AlternatingStopStrategy())

    trades_path = run_dir / "trades.csv"
    performance_path = run_dir / "performance.json"
    assert trades_path.exists()
    assert performance_path.exists()

    trades_df = pd.read_csv(trades_path)
    for col in ["risk_amount", "stop_distance", "r_multiple_gross", "r_multiple_net"]:
        assert col in trades_df.columns

    assert not trades_df.empty
    assert trades_df["risk_amount"].notna().any()
    assert trades_df["stop_distance"].notna().any()
    assert trades_df["r_multiple_net"].notna().any()

    perf = json.loads(performance_path.read_text(encoding="utf-8"))
    assert "ev_r_net" in perf


def test_risk_normalization_requires_stop_config_or_signal_stop() -> None:
    engine = RiskEngine(
        max_positions=1,
        risk_per_trade_pct=0.01,
        config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}},
    )
    with pytest.raises(ValueError, match="Provide signal.stop_price or configure risk.stop.mode=atr"):
        engine.compute_position_size_r(
            symbol="AAA",
            side="long",
            entry_price=100.0,
            signal={},
            bars_by_symbol={},
            ctx={},
            equity=10_000.0,
        )
