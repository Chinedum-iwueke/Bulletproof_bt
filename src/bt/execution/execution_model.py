"""Execution model placeholder."""
from __future__ import annotations

from dataclasses import replace
from typing import Optional

import pandas as pd

from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Fill, Order
from bt.execution.fees import FeeModel
from bt.execution.intrabar import IntrabarMode, IntrabarSpec, market_fill_price
from bt.execution.slippage import SlippageModel
from bt.execution.spread import SpreadMode, apply_spread


class ExecutionModel:
    def __init__(
        self,
        *,
        fee_model: FeeModel,
        slippage_model: SlippageModel,
        spread_mode: SpreadMode = "none",
        spread_bps: float = 0.0,
        intrabar_mode: IntrabarMode = "worst_case",
        delay_bars: int = 1,
    ) -> None:
        if delay_bars < 0:
            raise ValueError("delay_bars must be >= 0")
        if spread_bps < 0:
            raise ValueError("spread_bps must be >= 0")
        if spread_mode not in {"none", "fixed_bps", "bar_range_proxy"}:
            raise ValueError(f"Unsupported spread_mode: {spread_mode}")
        self._fee_model = fee_model
        self._slippage_model = slippage_model
        self._spread_mode = spread_mode
        self._spread_bps = spread_bps
        self._intrabar_spec = IntrabarSpec(mode=intrabar_mode)
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

            fill_price = market_fill_price(side=updated_order.side, bar=bar, intrabar_spec=self._intrabar_spec)
            spread_adjusted_fill_price = apply_spread(
                mode=self._spread_mode,
                spread_bps=self._spread_bps,
                price=fill_price,
                bar_high=bar.high,
                bar_low=bar.low,
                side=updated_order.side.value,
            )
            spread_cost = abs(updated_order.qty) * abs(spread_adjusted_fill_price - fill_price)
            fill_price = spread_adjusted_fill_price

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

            fill_metadata = dict(updated_order.metadata)
            fill_metadata.update(
                {
                    "intrabar_mode": self._intrabar_spec.mode,
                    "delay_bars": self._delay_bars,
                    "spread_mode": self._spread_mode,
                    "spread_bps": self._spread_bps,
                    "spread_cost": spread_cost,
                }
            )

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
                    metadata=fill_metadata,
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
