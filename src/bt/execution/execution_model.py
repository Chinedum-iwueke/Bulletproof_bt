"""Execution model placeholder."""
from __future__ import annotations

from dataclasses import replace
from typing import Optional

import pandas as pd

from bt.core.enums import IntrabarMode, OrderState, OrderType, Side
from bt.core.types import Bar, Fill, Order
from bt.execution.fees import FeeModel
from bt.execution.intrabar import worst_case_market_fill_price
from bt.execution.slippage import SlippageModel


class ExecutionModel:
    def __init__(
        self,
        *,
        fee_model: FeeModel,
        slippage_model: SlippageModel,
        intrabar_mode: IntrabarMode = IntrabarMode.WORST_CASE,
        delay_bars: int = 1,
    ) -> None:
        if delay_bars < 0:
            raise ValueError("delay_bars must be >= 0")
        self._fee_model = fee_model
        self._slippage_model = slippage_model
        self._intrabar_mode = intrabar_mode
        self._delay_bars = delay_bars

    def process(
        self,
        *,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        open_orders: list[Order],
    ) -> tuple[list[Order], list[Fill]]:
        """
        Process open orders at timestamp ts.
        Returns: (updated_orders, fills_emitted)
        """
        updated_orders: list[Order] = []
        fills: list[Fill] = []
        for order in open_orders:
            if order.order_type != OrderType.MARKET:
                raise NotImplementedError("Only MARKET orders are supported in v1.")

            updated_order = order
            if updated_order.state == OrderState.NEW:
                updated_order = replace(updated_order, state=OrderState.SUBMITTED)

            metadata = dict(updated_order.metadata)
            if "delay_remaining" not in metadata:
                metadata["delay_remaining"] = self._delay_bars

            bar: Optional[Bar] = bars_by_symbol.get(updated_order.symbol)
            if bar is None:
                updated_orders.append(replace(updated_order, metadata=metadata))
                continue

            if metadata["delay_remaining"] > 0:
                metadata["delay_remaining"] = max(metadata["delay_remaining"] - 1, 0)
                updated_orders.append(replace(updated_order, metadata=metadata))
                continue

            fill_price = worst_case_market_fill_price(updated_order.side, bar)
            slippage_quote = self._slippage_model.estimate_slippage(qty=updated_order.qty, bar=bar)
            slip_px = slippage_quote / max(abs(updated_order.qty), 1e-12)
            if updated_order.side == Side.BUY:
                fill_price += slip_px
            elif updated_order.side == Side.SELL:
                fill_price -= slip_px
            else:
                raise ValueError(f"Unsupported side: {updated_order.side}")

            notional = abs(updated_order.qty) * fill_price
            fee = self._fee_model.fee_for_notional(notional=notional, is_maker=False)

            fills.append(
                Fill(
                    order_id=updated_order.id,
                    ts=ts,
                    symbol=updated_order.symbol,
                    side=updated_order.side,
                    qty=updated_order.qty,
                    price=fill_price,
                    fee=fee,
                    slippage=slippage_quote,
                    metadata={
                        "intrabar_mode": self._intrabar_mode.value,
                        "delay_bars": self._delay_bars,
                    },
                )
            )

            updated_orders.append(
                replace(
                    updated_order,
                    state=OrderState.FILLED,
                    metadata={**metadata, "delay_remaining": 0},
                )
            )

        return updated_orders, fills
