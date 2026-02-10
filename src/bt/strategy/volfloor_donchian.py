"""Hypothesis 1 Volatility-floor gated Donchian strategy."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.indicators.atr import ATR
from bt.indicators.dmi_adx import DMIADX
from bt.strategy.base import Strategy
from bt.strategy import register_strategy


@dataclass
class _SymbolState:
    highs: deque[float]
    lows: deque[float]
    natr_history: deque[float]
    atr: ATR
    adx: DMIADX
    position: Side | None = None
    last_htf_ts: pd.Timestamp | None = None


@register_strategy("volfloor_donchian")
class VolFloorDonchianStrategy(Strategy):
    def __init__(
        self,
        *,
        seed: int | None = None,
        timeframe: str = "15m",
        donchian_entry_lookback: int = 20,
        donchian_exit_lookback: int = 10,
        adx_min: float = 18.0,
        vol_floor_pct: float = 60.0,
        atr_period: int = 14,
        vol_lookback_bars: int = 2880,
        symbols: list[str] | None = None,
    ) -> None:
        self._seed = seed
        self._timeframe = timeframe
        self._entry_lookback = donchian_entry_lookback
        self._exit_lookback = donchian_exit_lookback
        self._adx_min = adx_min
        self._vol_floor_pct = vol_floor_pct
        self._atr_period = atr_period
        self._vol_lookback_bars = vol_lookback_bars
        self._symbols = set(symbols) if symbols is not None else None
        self._state: dict[str, _SymbolState] = {}

    def _state_for(self, symbol: str) -> _SymbolState:
        current = self._state.get(symbol)
        if current is None:
            current = _SymbolState(
                highs=deque(maxlen=max(self._entry_lookback, self._exit_lookback)),
                lows=deque(maxlen=max(self._entry_lookback, self._exit_lookback)),
                natr_history=deque(maxlen=self._vol_lookback_bars),
                atr=ATR(self._atr_period),
                adx=DMIADX(14),
            )
            self._state[symbol] = current
        return current

    @staticmethod
    def _percentile_rank(reference: list[float], value: float) -> float:
        if not reference:
            return 0.0
        count = sum(1 for item in reference if item <= value)
        return (count / len(reference)) * 100.0

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        signals: list[Signal] = []
        htf_ctx = ctx.get("htf", {})
        tf_bars = htf_ctx.get(self._timeframe, {}) if isinstance(htf_ctx, Mapping) else {}

        target_symbols = self._symbols if self._symbols is not None else tradeable
        for symbol in sorted(target_symbols):
            htf_bar = tf_bars.get(symbol)
            if htf_bar is None:
                continue

            symbol_state = self._state_for(symbol)
            if symbol_state.last_htf_ts is not None and htf_bar.ts <= symbol_state.last_htf_ts:
                continue
            symbol_state.last_htf_ts = htf_bar.ts

            prev_highs = list(symbol_state.highs)
            prev_lows = list(symbol_state.lows)

            symbol_state.atr.update(htf_bar)
            symbol_state.adx.update(htf_bar)

            atr_value = symbol_state.atr.value
            adx_values = symbol_state.adx.values
            adx_value = adx_values.get("adx")
            plus_di = adx_values.get("plus_di")
            minus_di = adx_values.get("minus_di")

            natr_value: float | None = None
            vol_rank: float | None = None
            if atr_value is not None and htf_bar.close > 0:
                natr_value = atr_value / htf_bar.close
                vol_rank = self._percentile_rank(list(symbol_state.natr_history), natr_value)

            entry_high = max(prev_highs[-self._entry_lookback :]) if len(prev_highs) >= self._entry_lookback else None
            entry_low = min(prev_lows[-self._entry_lookback :]) if len(prev_lows) >= self._entry_lookback else None
            exit_high = max(prev_highs[-self._exit_lookback :]) if len(prev_highs) >= self._exit_lookback else None
            exit_low = min(prev_lows[-self._exit_lookback :]) if len(prev_lows) >= self._exit_lookback else None

            if symbol_state.position == Side.BUY and exit_low is not None and htf_bar.close < exit_low:
                signals.append(
                    Signal(
                        ts=ts,
                        symbol=symbol,
                        side=Side.SELL,
                        signal_type="h1_volfloor_donchian_exit",
                        confidence=1.0,
                        metadata={
                            "strategy": "volfloor_donchian",
                            "tf": self._timeframe,
                            "vol_pct_rank": vol_rank,
                            "vol_floor_pct": self._vol_floor_pct,
                            "adx": adx_value,
                            "donchian_entry": {"high": entry_high, "low": entry_low},
                            "donchian_exit": {"high": exit_high, "low": exit_low},
                        },
                    )
                )
                symbol_state.position = None
            elif symbol_state.position == Side.SELL and exit_high is not None and htf_bar.close > exit_high:
                signals.append(
                    Signal(
                        ts=ts,
                        symbol=symbol,
                        side=Side.BUY,
                        signal_type="h1_volfloor_donchian_exit",
                        confidence=1.0,
                        metadata={
                            "strategy": "volfloor_donchian",
                            "tf": self._timeframe,
                            "vol_pct_rank": vol_rank,
                            "vol_floor_pct": self._vol_floor_pct,
                            "adx": adx_value,
                            "donchian_entry": {"high": entry_high, "low": entry_low},
                            "donchian_exit": {"high": exit_high, "low": exit_low},
                        },
                    )
                )
                symbol_state.position = None

            adx_ok = adx_value is not None and adx_value >= self._adx_min
            vol_ok = (
                vol_rank is not None
                and len(symbol_state.natr_history) >= self._vol_lookback_bars
                and vol_rank >= self._vol_floor_pct
            )
            if symbol_state.position is None and adx_ok and vol_ok:
                long_bias_ok = plus_di is None or minus_di is None or plus_di > minus_di
                short_bias_ok = plus_di is None or minus_di is None or minus_di > plus_di

                if entry_high is not None and htf_bar.close > entry_high and long_bias_ok:
                    signals.append(
                        Signal(
                            ts=ts,
                            symbol=symbol,
                            side=Side.BUY,
                            signal_type="h1_volfloor_donchian_entry",
                            confidence=1.0,
                            metadata={
                                "strategy": "volfloor_donchian",
                                "tf": self._timeframe,
                                "vol_pct_rank": vol_rank,
                                "vol_floor_pct": self._vol_floor_pct,
                                "adx": adx_value,
                                "donchian_entry": {"high": entry_high, "low": entry_low},
                                "donchian_exit": {"high": exit_high, "low": exit_low},
                            },
                        )
                    )
                    symbol_state.position = Side.BUY
                elif entry_low is not None and htf_bar.close < entry_low and short_bias_ok:
                    signals.append(
                        Signal(
                            ts=ts,
                            symbol=symbol,
                            side=Side.SELL,
                            signal_type="h1_volfloor_donchian_entry",
                            confidence=1.0,
                            metadata={
                                "strategy": "volfloor_donchian",
                                "tf": self._timeframe,
                                "vol_pct_rank": vol_rank,
                                "vol_floor_pct": self._vol_floor_pct,
                                "adx": adx_value,
                                "donchian_entry": {"high": entry_high, "low": entry_low},
                                "donchian_exit": {"high": exit_high, "low": exit_low},
                            },
                        )
                    )
                    symbol_state.position = Side.SELL

            symbol_state.highs.append(htf_bar.high)
            symbol_state.lows.append(htf_bar.low)
            if natr_value is not None:
                symbol_state.natr_history.append(natr_value)

        return signals
