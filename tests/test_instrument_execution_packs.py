from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt.api import run_backtest
from bt.core.config_resolver import resolve_config
from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Order
from bt.execution.commission import CommissionSpec
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.instruments.spec import InstrumentSpec


def _bar(ts: str, symbol: str, price: float) -> Bar:
    p = pd.Timestamp(ts)
    return Bar(ts=p, symbol=symbol, open=price, high=price, low=price, close=price, volume=1000.0)


def _order(order_id: str, ts: str, symbol: str, side: Side, qty: float) -> Order:
    return Order(
        id=order_id,
        ts_submitted=pd.Timestamp(ts),
        symbol=symbol,
        side=side,
        qty=qty,
        order_type=OrderType.MARKET,
        limit_price=None,
        state=OrderState.NEW,
        metadata={},
    )


def _write_dataset(dataset_dir: Path, symbol: str = "BTCUSDT") -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "manifest.yaml").write_text(
        yaml.safe_dump(
            {"format": "per_symbol_parquet", "symbols": [symbol], "path": "symbols/{symbol}.parquet"},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    rows = []
    for i in range(20):
        px = 100.0 + i * 0.1
        rows.append(
            {
                "ts": pd.Timestamp("2024-01-01T00:00:00Z") + pd.Timedelta(minutes=i),
                "open": px,
                "high": px + 0.2,
                "low": px - 0.2,
                "close": px,
                "volume": float(i + 1),
                "symbol": symbol,
            }
        )
    frame = pd.DataFrame(rows)
    (dataset_dir / "symbols").mkdir(parents=True, exist_ok=True)
    frame.to_parquet(dataset_dir / "symbols" / f"{symbol}.parquet", index=False)


def _final_equity(path: Path) -> float:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return float(rows[-1]["equity"])


def test_fx_spread_applied_entry_and_exit_fixed_pips() -> None:
    instrument = InstrumentSpec(type="forex", symbol="EURUSD", pip_size=0.0001)
    model = ExecutionModel(
        fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
        slippage_model=SlippageModel(k=0.0, fixed_bps=0.0),
        spread_mode="fixed_pips",
        spread_pips=2.0,
        delay_bars=0,
        intrabar_mode="midpoint",
        instrument=instrument,
    )

    ts = "2024-01-01T00:00:00Z"
    bar = _bar(ts, "EURUSD", 1.1000)

    _, buy_fills = model.process(
        ts=pd.Timestamp(ts),
        bars_by_symbol={"EURUSD": bar},
        open_orders=[_order("o1", ts, "EURUSD", Side.BUY, 1.0)],
    )
    _, sell_fills = model.process(
        ts=pd.Timestamp(ts),
        bars_by_symbol={"EURUSD": bar},
        open_orders=[_order("o2", ts, "EURUSD", Side.SELL, 1.0)],
    )

    buy_fill = buy_fills[0]
    sell_fill = sell_fills[0]
    assert buy_fill.price > 1.1
    assert sell_fill.price < 1.1
    realized_pnl = sell_fill.price - buy_fill.price
    assert realized_pnl < 0.0


def test_fx_missing_spread_mode_raises() -> None:
    with pytest.raises(ValueError, match="execution.spread_mode.*FX V1 requires spread modeling"):
        resolve_config(
            {
                "strategy": {"name": "coinflip"},
                "instrument": {"type": "forex", "symbol": "EURUSD", "pip_size": 0.0001, "contract_size": 100000},
                "risk": {"risk_per_trade_pct": 0.001, "fx": {"lot_step": 0.01}},
                "execution": {"profile": "tier2", "spread_mode": "none"},
            }
        )


def test_equity_commission_per_share_applied() -> None:
    instrument = InstrumentSpec(type="equity", symbol="AAPL")
    model = ExecutionModel(
        fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
        slippage_model=SlippageModel(k=0.0, fixed_bps=0.0),
        spread_mode="none",
        delay_bars=0,
        intrabar_mode="midpoint",
        instrument=instrument,
        commission=CommissionSpec(mode="per_share", per_share=0.01),
    )

    ts = "2024-01-01T00:00:00Z"
    bar = _bar(ts, "AAPL", 100.0)
    _, fills = model.process(
        ts=pd.Timestamp(ts),
        bars_by_symbol={"AAPL": bar},
        open_orders=[_order("eq1", ts, "AAPL", Side.BUY, 100.0)],
    )
    fill = fills[0]
    assert fill.metadata["commission_fee"] == pytest.approx(1.0)
    assert fill.fee == pytest.approx(1.0)


def test_crypto_regression_unchanged_without_instrument(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir)

    base_cfg = {
        "initial_cash": 1000.0,
        "max_leverage": 2.0,
        "signal_delay_bars": 1,
        "risk": {"max_positions": 1, "risk_per_trade_pct": 0.001},
        "strategy": {"name": "coinflip", "seed": 1, "p_trade": 0.0, "cooldown_bars": 0},
        "data": {"mode": "streaming", "symbols_subset": ["BTCUSDT"]},
        "execution": {"profile": "tier2", "spread_mode": "none"},
        "benchmark": {"enabled": False},
    }
    cfg_a = tmp_path / "a.yaml"
    cfg_b = tmp_path / "b.yaml"
    cfg_a.write_text(yaml.safe_dump(base_cfg, sort_keys=False), encoding="utf-8")
    base_cfg_with_instrument = dict(base_cfg)
    base_cfg_with_instrument["instrument"] = {"type": "crypto", "symbol": "BTCUSDT"}
    cfg_b.write_text(yaml.safe_dump(base_cfg_with_instrument, sort_keys=False), encoding="utf-8")

    run_a = Path(run_backtest(config_path=str(cfg_a), data_path=str(dataset_dir), out_dir=str(tmp_path / "out_a"), run_name="a"))
    run_b = Path(run_backtest(config_path=str(cfg_b), data_path=str(dataset_dir), out_dir=str(tmp_path / "out_b"), run_name="b"))

    perf_a = json.loads((run_a / "performance.json").read_text(encoding="utf-8"))
    perf_b = json.loads((run_b / "performance.json").read_text(encoding="utf-8"))
    assert perf_a["final_equity"] == pytest.approx(perf_b["final_equity"], abs=1e-12)
    assert _final_equity(run_a / "equity.csv") == pytest.approx(_final_equity(run_b / "equity.csv"), abs=1e-12)


def test_validation_guards() -> None:
    with pytest.raises(ValueError, match="execution.commission.mode=per_lot.*instrument.type=forex"):
        resolve_config(
            {
                "strategy": {"name": "coinflip"},
                "instrument": {"type": "equity", "symbol": "AAPL"},
                "execution": {"commission": {"mode": "per_lot", "per_lot": 1.0}},
            }
        )

    with pytest.raises(ValueError, match="execution.spread_mode=fixed_pips is FX-only"):
        resolve_config(
            {
                "strategy": {"name": "coinflip"},
                "instrument": {"type": "crypto", "symbol": "BTCUSDT"},
                "execution": {"spread_mode": "fixed_pips", "spread_pips": 2.0},
            }
        )
