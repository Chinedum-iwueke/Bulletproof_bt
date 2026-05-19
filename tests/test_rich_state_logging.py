from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.api import _resolve_timeframe_mode
from bt.core.engine import BacktestEngine
from bt.core.enums import Side
from bt.core.types import Signal
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.experiments.dataset_builder import extract_experiment_dataset
from bt.logging.trade_enrichment import enrich_trade_row
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy
from bt.universe.universe import UniverseEngine


class _OneRichStateTradeStrategy(Strategy):
    def on_bars(self, ts, bars, tradeable, ctx):
        bar = bars["BTCUSDT"]
        if ts == pd.Timestamp("2025-01-01T00:10:00Z"):
            return [
                Signal(
                    ts=ts,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    signal_type="entry",
                    confidence=1.0,
                    metadata={
                        "stop_price": bar.close - 2.0,
                        "entry_stop_price": bar.close - 2.0,
                        "stop_distance": 2.0,
                        "entry_state_funding_raw": None,
                        "entry_state_oi_level": float("nan"),
                    },
                )
            ]
        if ts == pd.Timestamp("2025-01-01T00:15:00Z"):
            return [
                Signal(
                    ts=ts,
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    signal_type="exit",
                    confidence=1.0,
                    metadata={"close_only": True},
                )
            ]
        return []


def test_trade_enrichment_preserves_rich_entry_state_fields() -> None:
    row = {
        "r_net": 1.0,
        "entry_state_funding_raw": 0.001,
        "entry_state_oi_accel_pctile": 0.9,
        "entry_state_basis_pctile": 0.8,
        "entry_state_csi_source": "enriched",
    }

    enriched = enrich_trade_row(row)

    assert enriched["entry_state_funding_raw"] == 0.001
    assert enriched["entry_state_oi_accel_pctile"] == 0.9
    assert enriched["entry_state_csi_source"] == "enriched"


def test_engine_trade_log_receives_rich_online_state_snapshot(tmp_path: Path) -> None:
    rows = []
    for i in range(20):
        ts = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
        rows.append(
            {
                "ts": ts,
                "symbol": "BTCUSDT",
                "open": 100 + i,
                "high": 101 + i,
                "low": 99 + i,
                "close": 100 + i,
                "volume": 1000 + i,
                "mark_close": 100.2 + i,
                "index_close": 100 + i,
                "funding_rate": -0.001 + i * 0.0002,
                "funding_source_ts": ts,
                "open_interest": 1_000_000 + i * 1000,
                "oi_source_ts": ts,
                "basis_close_vs_index": 0.002 + i * 0.0001,
                "premium_mark_vs_index": 0.001 + i * 0.0001,
            }
        )

    run_dir = tmp_path / "run"
    engine = BacktestEngine(
        datafeed=HistoricalDataFeed(pd.DataFrame(rows)),
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=_OneRichStateTradeStrategy(),
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}}),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=10_000.0, max_leverage=2.0),
        decisions_writer=JsonlWriter(run_dir / "decisions.jsonl"),
        fills_writer=JsonlWriter(run_dir / "fills.jsonl"),
        trades_writer=TradesCsvWriter(run_dir / "trades.csv"),
        equity_path=run_dir / "equity.csv",
        config={"state_features": {"enabled": True, "profile": "full"}},
    )
    engine.run()

    trades = pd.read_csv(run_dir / "trades.csv")
    assert not trades.empty
    row = trades.iloc[0]
    assert row["entry_state_csi_source"] == "enriched"
    assert pd.notna(row["entry_state_funding_raw"])
    assert pd.notna(row["entry_state_funding_pctile"])
    assert pd.notna(row["entry_state_oi_level"])
    assert pd.notna(row["entry_state_oi_accel_pctile"])
    assert pd.notna(row["entry_state_mark_price"])
    assert pd.notna(row["entry_state_index_price"])
    assert pd.notna(row["entry_state_basis_pct"])
    assert pd.notna(row["entry_state_basis_pctile"])


def test_research_panel_timeframe_does_not_trigger_engine_resample() -> None:
    mode, engine_timeframe, entry_timeframe, exit_timeframe = _resolve_timeframe_mode(
        {
            "data": {
                "dataset_kind": "research_panel",
                "timeframe": "1m",
                "engine_timeframe": None,
                "entry_timeframe": None,
                "exit_timeframe": "1m",
            }
        }
    )

    assert mode == "default"
    assert engine_timeframe is None
    assert entry_timeframe is None
    assert exit_timeframe == "1m"


def test_extract_experiment_dataset_preserves_rich_state_fields(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    run = exp / "runs" / "run_1"
    run.mkdir(parents=True)
    (run / "config_used.yaml").write_text("strategy:\n  name: fixture\n", encoding="utf-8")
    (run / "performance.json").write_text(json.dumps({"net_pnl": 1.0, "trade_count": 1}), encoding="utf-8")
    pd.DataFrame(
        [
            {
                "entry_ts": "2025-01-01T00:00:00Z",
                "exit_ts": "2025-01-01T00:05:00Z",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "qty": 1,
                "entry_price": 100,
                "exit_price": 101,
                "pnl": 1,
                "pnl_net": 1,
                "r_multiple_net": 1,
                "entry_state_funding_raw": 0.001,
                "entry_state_oi_accel_pctile": 0.9,
                "entry_state_basis_pctile": 0.8,
                "entry_state_csi_source": "enriched",
            }
        ]
    ).to_csv(run / "trades.csv", index=False)

    extract_experiment_dataset(experiment_root=exp, overwrite=True)
    out = pd.read_parquet(exp / "research_data" / "trades_dataset.parquet")

    assert "entry_state_funding_raw" in out.columns
    assert "entry_state_oi_accel_pctile" in out.columns
    assert "entry_state_basis_pctile" in out.columns
    assert "entry_state_csi_source" in out.columns
