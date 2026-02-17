from __future__ import annotations

from bt.core.enums import Side
from bt.strategy.volfloor_donchian import VolFloorDonchianStrategy


def test_compute_structural_stop_by_side() -> None:
    assert VolFloorDonchianStrategy._compute_structural_stop(Side.BUY, exit_high=120.0, exit_low=95.0) == 95.0
    assert VolFloorDonchianStrategy._compute_structural_stop(Side.SELL, exit_high=120.0, exit_low=95.0) == 120.0


def test_compute_atr_stop_by_side() -> None:
    assert VolFloorDonchianStrategy._compute_atr_stop(Side.BUY, entry_price=100.0, atr_value=2.0, atr_stop_multiple=2.5) == 95.0
    assert VolFloorDonchianStrategy._compute_atr_stop(Side.SELL, entry_price=100.0, atr_value=2.0, atr_stop_multiple=2.5) == 105.0


def test_compute_final_stop_for_each_mode() -> None:
    assert VolFloorDonchianStrategy._compute_final_stop(
        Side.BUY,
        stop_mode="structural",
        structural_stop=93.0,
        atr_stop=95.0,
    ) == 93.0
    assert VolFloorDonchianStrategy._compute_final_stop(
        Side.SELL,
        stop_mode="atr",
        structural_stop=107.0,
        atr_stop=105.0,
    ) == 105.0

    assert VolFloorDonchianStrategy._compute_final_stop(
        Side.BUY,
        stop_mode="hybrid",
        structural_stop=93.0,
        atr_stop=95.0,
    ) == 93.0
    assert VolFloorDonchianStrategy._compute_final_stop(
        Side.SELL,
        stop_mode="hybrid",
        structural_stop=107.0,
        atr_stop=105.0,
    ) == 107.0


def test_invalid_combinations_return_none() -> None:
    assert VolFloorDonchianStrategy._compute_structural_stop(Side.BUY, exit_high=120.0, exit_low=None) is None
    assert VolFloorDonchianStrategy._compute_structural_stop(Side.SELL, exit_high=None, exit_low=95.0) is None

    assert VolFloorDonchianStrategy._compute_atr_stop(Side.BUY, entry_price=100.0, atr_value=None, atr_stop_multiple=2.5) is None
    assert VolFloorDonchianStrategy._compute_atr_stop(Side.SELL, entry_price=100.0, atr_value=2.0, atr_stop_multiple=0.0) is None

    assert VolFloorDonchianStrategy._compute_final_stop(
        Side.BUY,
        stop_mode="hybrid",
        structural_stop=None,
        atr_stop=95.0,
    ) is None
    assert VolFloorDonchianStrategy._compute_final_stop(
        Side.SELL,
        stop_mode="hybrid",
        structural_stop=107.0,
        atr_stop=None,
    ) is None
