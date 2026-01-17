"""Position state management and MAE/MFE tracking."""
from __future__ import annotations

from dataclasses import replace
from typing import Optional

import pandas as pd

from bt.core.enums import PositionState, Side
from bt.core.types import Fill, Position, Trade


class PositionBook:
    def __init__(self) -> None:
        self._positions: dict[str, Position] = {}
        self._position_costs: dict[str, tuple[float, float]] = {}

    def get(self, symbol: str) -> Position:
        """Return current Position for symbol (create FLAT if missing)."""
        if symbol not in self._positions:
            self._positions[symbol] = Position(
                symbol=symbol,
                state=PositionState.FLAT,
                side=None,
                qty=0.0,
                avg_entry_price=0.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                mae_price=None,
                mfe_price=None,
                opened_ts=None,
                closed_ts=None,
            )
        return self._positions[symbol]

    def all_positions(self) -> dict[str, Position]:
        return dict(self._positions)

    def open_positions_count(self) -> int:
        """Count positions that are OPEN/OPENING/REDUCING (not FLAT/CLOSED)."""
        open_states = {PositionState.OPEN, PositionState.OPENING, PositionState.REDUCING}
        return sum(1 for position in self._positions.values() if position.state in open_states)

    def apply_fill(self, fill: Fill) -> tuple[Position, Optional[Trade]]:
        """
        Apply a Fill to the symbol position.
        Returns: (updated_position, trade_if_closed_else_None)
        """
        position = self.get(fill.symbol)
        trade: Optional[Trade] = None
        fees_paid, slippage_paid = self._position_costs.get(fill.symbol, (0.0, 0.0))

        if position.state in {PositionState.FLAT, PositionState.CLOSED} or position.qty == 0:
            position = self._open_new_position(fill)
            self._positions[fill.symbol] = position
            self._position_costs[fill.symbol] = (fill.fee, fill.slippage)
            return position, None

        if position.side == fill.side:
            new_qty = position.qty + fill.qty
            new_avg = (
                position.avg_entry_price * position.qty + fill.price * fill.qty
            ) / new_qty
            mae_price, mfe_price = self._update_mae_mfe(position, fill.price)
            position = replace(
                position,
                qty=new_qty,
                avg_entry_price=new_avg,
                state=PositionState.OPEN,
                mae_price=mae_price,
                mfe_price=mfe_price,
            )
            self._positions[fill.symbol] = position
            self._position_costs[fill.symbol] = (
                fees_paid + fill.fee,
                slippage_paid + fill.slippage,
            )
            return position, None

        reduce_qty = min(position.qty, fill.qty)
        realized_pnl = self._realized_pnl(position, fill.price, reduce_qty)
        mae_price, mfe_price = self._update_mae_mfe(position, fill.price)
        remaining_qty = position.qty - reduce_qty
        fill_qty = max(abs(fill.qty), 1e-12)
        close_ratio = reduce_qty / fill_qty
        closing_fee = fill.fee * close_ratio
        closing_slippage = fill.slippage * close_ratio
        total_fees = fees_paid + closing_fee
        total_slippage = slippage_paid + closing_slippage

        if fill.qty > position.qty:
            trade = self._build_trade(
                position=position,
                exit_price=fill.price,
                exit_ts=fill.ts,
                qty=reduce_qty,
                pnl=realized_pnl,
                fees=total_fees,
                slippage=total_slippage,
                mae_price=mae_price,
                mfe_price=mfe_price,
            )
            position = self._open_new_position(fill, qty=fill.qty - reduce_qty)
            self._position_costs[fill.symbol] = (
                fill.fee - closing_fee,
                fill.slippage - closing_slippage,
            )
        elif remaining_qty == 0:
            trade = self._build_trade(
                position=position,
                exit_price=fill.price,
                exit_ts=fill.ts,
                qty=reduce_qty,
                pnl=realized_pnl,
                fees=total_fees,
                slippage=total_slippage,
                mae_price=mae_price,
                mfe_price=mfe_price,
            )
            position = replace(
                position,
                state=PositionState.CLOSED,
                side=None,
                qty=0.0,
                avg_entry_price=0.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                mae_price=None,
                mfe_price=None,
                opened_ts=None,
                closed_ts=fill.ts,
            )
            self._position_costs.pop(fill.symbol, None)
        else:
            position = replace(
                position,
                qty=remaining_qty,
                realized_pnl=position.realized_pnl + realized_pnl,
                state=PositionState.REDUCING,
                mae_price=mae_price,
                mfe_price=mfe_price,
            )
            self._position_costs[fill.symbol] = (total_fees, total_slippage)

        self._positions[fill.symbol] = position
        return position, trade

    def _open_new_position(self, fill: Fill, qty: Optional[float] = None) -> Position:
        open_qty = fill.qty if qty is None else qty
        return Position(
            symbol=fill.symbol,
            state=PositionState.OPEN,
            side=fill.side,
            qty=open_qty,
            avg_entry_price=fill.price,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            mae_price=fill.price,
            mfe_price=fill.price,
            opened_ts=fill.ts,
            closed_ts=None,
        )

    @staticmethod
    def _realized_pnl(position: Position, price: float, qty: float) -> float:
        if position.side == Side.BUY:
            return (price - position.avg_entry_price) * qty
        return (position.avg_entry_price - price) * qty

    @staticmethod
    def _update_mae_mfe(position: Position, price: float) -> tuple[float, float]:
        if position.mae_price is None or position.mfe_price is None:
            return price, price
        mae_price = min(position.mae_price, price)
        mfe_price = max(position.mfe_price, price)
        return mae_price, mfe_price

    @staticmethod
    def _build_trade(
        *,
        position: Position,
        exit_price: float,
        exit_ts: pd.Timestamp,
        qty: float,
        pnl: float,
        fees: float,
        slippage: float,
        mae_price: float,
        mfe_price: float,
    ) -> Trade:
        return Trade(
            symbol=position.symbol,
            side=position.side or Side.BUY,
            entry_ts=position.opened_ts or exit_ts,
            exit_ts=exit_ts,
            entry_price=position.avg_entry_price,
            exit_price=exit_price,
            qty=qty,
            pnl=pnl,
            fees=fees,
            slippage=slippage,
            mae_price=mae_price,
            mfe_price=mfe_price,
            metadata={},
        )
