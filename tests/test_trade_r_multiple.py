from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest

from bt.core.enums import Side
from bt.core.types import Trade
from bt.logging.trades import TradesCsvWriter
from bt.risk.r_multiple import compute_r_multiple


def test_compute_r_multiple() -> None:
    assert compute_r_multiple(200.0, 100.0) == 2.0
    assert compute_r_multiple(-50.0, 100.0) == -0.5
    assert compute_r_multiple(10.0, None) is None
    assert compute_r_multiple(10.0, 0.0) is None
    assert compute_r_multiple(10.0, -1.0) is None


def test_trades_csv_contains_risk_and_r_multiple_columns(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path)

    trade = Trade(
        symbol="BTC",
        side=Side.BUY,
        entry_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        exit_ts=pd.Timestamp("2024-01-01T01:00:00Z"),
        entry_price=100.0,
        exit_price=110.0,
        qty=2.0,
        pnl=20.0,
        fees=1.0,
        slippage=0.5,
        mae_price=95.0,
        mfe_price=112.0,
        metadata={"risk_amount": 100.0, "stop_distance": 5.0},
    )
    writer.write_trade(trade)
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    row = rows[0]

    assert "risk_amount" in row
    assert "stop_distance" in row
    assert "entry_qty" in row
    assert "exit_qty" in row
    assert "entry_stop_distance" in row
    assert "r_multiple_gross" in row
    assert "r_multiple_net" in row

    assert float(row["risk_amount"]) == 100.0
    assert float(row["stop_distance"]) == 5.0
    assert float(row["entry_stop_distance"]) == 5.0
    assert float(row["entry_qty"]) == 2.0
    assert float(row["exit_qty"]) == 2.0

    pnl_price = trade.pnl
    pnl_net = trade.pnl - trade.fees
    assert float(row["r_multiple_gross"]) == pnl_price / 100.0
    assert float(row["r_multiple_net"]) == pnl_net / 100.0


def test_trades_csv_legacy_trade_without_risk_metadata(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path)

    trade = Trade(
        symbol="ETH",
        side=Side.SELL,
        entry_ts=pd.Timestamp("2024-01-02T00:00:00Z"),
        exit_ts=pd.Timestamp("2024-01-02T01:00:00Z"),
        entry_price=200.0,
        exit_price=190.0,
        qty=1.0,
        pnl=10.0,
        fees=0.5,
        slippage=0.25,
        mae_price=205.0,
        mfe_price=180.0,
        metadata={},
    )
    writer.write_trade(trade)
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    row = rows[0]
    assert row["risk_amount"] == ""
    assert row["stop_distance"] == ""
    assert row["entry_stop_distance"] == ""
    assert float(row["entry_qty"]) == 1.0
    assert float(row["exit_qty"]) == 1.0
    assert row["r_multiple_gross"] == ""
    assert row["r_multiple_net"] == ""


def test_trades_csv_supports_distinct_entry_qty_and_exit_qty_for_partial_close(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path)

    trade = Trade(
        symbol="BTC",
        side=Side.BUY,
        entry_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        exit_ts=pd.Timestamp("2024-01-01T01:00:00Z"),
        entry_price=100.0,
        exit_price=105.0,
        qty=0.4,
        pnl=2.0,
        fees=0.1,
        slippage=0.0,
        mae_price=99.0,
        mfe_price=106.0,
        metadata={"entry_qty": 1.0, "risk_amount": 50.0, "stop_distance": 50.0},
    )
    writer.write_trade(trade)
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    row = rows[0]
    assert float(row["entry_qty"]) == 1.0
    assert float(row["exit_qty"]) == 0.4
    assert float(row["r_multiple_gross"]) == pytest.approx(2.0 / 50.0)
