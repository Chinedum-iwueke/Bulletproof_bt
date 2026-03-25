from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderType, Side
from bt.core.types import Fill, OrderIntent
from bt.exec.adapters.simulated import SimulatedBrokerAdapter
from bt.exec.events.broker_events import BrokerOrderAcknowledgedEvent, BrokerOrderFilledEvent, BrokerOrderPartiallyFilledEvent
from bt.exec.services.execution_router import ExecutionRouter
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.exec.state.sqlite_store import SQLiteExecutionStateStore
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.portfolio.portfolio import Portfolio


def _router(tmp_path):
    adapter = SimulatedBrokerAdapter(
        execution_model=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0, atr_pct_cap=0.0, impact_cap=0.0, fixed_bps=0.0),
            spread_mode="none",
            spread_bps=0.0,
            spread_pips=None,
            intrabar_mode="worst_case",
            delay_bars=0,
            instrument=None,
        )
    )
    adapter.start()
    portfolio_runner = PortfolioRunner(portfolio=Portfolio(initial_cash=10_000.0))
    store = SQLiteExecutionStateStore(path=str(tmp_path / "state.sqlite"))
    return adapter, portfolio_runner, ExecutionRouter(
        run_id="run-1",
        mode="paper_broker",
        adapter=adapter,
        portfolio_runner=portfolio_runner,
        store=store,
        save_processed_event_ids=True,
    )


def test_ack_not_fill_and_fill_dedupe(tmp_path) -> None:
    adapter, portfolio_runner, router = _router(tmp_path)
    intent = OrderIntent(
        ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        reason="test",
    )
    result = router.submit_order(order_seq=1, intent=intent, ts=intent.ts)
    assert result.order_id.startswith("sim-")
    assert router.local_fills() == []

    fill = Fill(
        order_id=result.order_id,
        ts=intent.ts,
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=1.0,
        price=100.0,
        fee=0.0,
        slippage=0.0,
        metadata={"exec_id": "e-1"},
    )
    adapter._events.extend(  # noqa: SLF001
        [
            BrokerOrderAcknowledgedEvent(ts=intent.ts, broker_event_id="ack-1", order=adapter.fetch_open_orders()[0]),
            BrokerOrderPartiallyFilledEvent(ts=intent.ts, broker_event_id="p-1", fill=fill, leaves_qty=0.4),
            BrokerOrderFilledEvent(ts=intent.ts, broker_event_id="f-1", fill=fill),
        ]
    )
    first = router.process_broker_events()
    second = router.process_broker_events()
    assert len(first) == 1
    assert second == []
    assert len(router.local_fills()) == 1
    assert portfolio_runner.portfolio.position_book.get("BTCUSDT").qty == 1.0
