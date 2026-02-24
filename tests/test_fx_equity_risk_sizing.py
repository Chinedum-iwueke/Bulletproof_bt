from __future__ import annotations

import pytest

from bt.core.config_resolver import resolve_config
from bt.instruments.spec import InstrumentSpec
from bt.risk.instrument_sizing import size_position_from_risk
from bt.risk.risk_engine import RiskEngine


def _engine(config: dict) -> RiskEngine:
    return RiskEngine(max_positions=1, config=resolve_config(config))


def test_fx_lot_sizing_rounds_down_deterministically() -> None:
    config = {
        "risk": {
            "mode": "r_fixed",
            "r_per_trade": 0.01,
            "fx": {"lot_step": 0.01},
            "margin": {"leverage": 20.0},
        },
        "instrument": {
            "type": "forex",
            "symbol": "EURUSD",
            "contract_size": 100000,
            "pip_size": 0.0001,
            "quote_currency": "USD",
            "account_currency": "USD",
        },
        "execution": {"spread_mode": "fixed_bps"},
    }
    engine = _engine(config)

    qty, meta = engine.compute_position_size_r(
        symbol="EURUSD",
        side="long",
        entry_price=1.2000,
        signal={"stop_price": 1.1980},
        bars_by_symbol={},
        ctx={},
        equity=10_000.0,
    )

    assert qty == pytest.approx(0.49)
    assert meta["qty_rounding_unit"] == pytest.approx(0.01)
    assert meta["margin_required"] is not None
    assert meta["notional"] > 0


def test_fx_too_small_risk_rejects_rounds_to_zero() -> None:
    with pytest.raises(ValueError, match=r"rounds to 0.*risk\.fx\.lot_step"):
        size_position_from_risk(
            instrument=InstrumentSpec(
                type="forex",
                symbol="EURUSD",
                contract_size=100000,
                pip_size=0.0001,
                quote_currency="USD",
                account_currency="USD",
            ),
            entry_price=1.2,
            stop_price=1.199,
            risk_amount=10.0,
            account_leverage=10.0,
            fx_lot_step=1.0,
        )


def test_equity_share_sizing_integer_rounding() -> None:
    config = {
        "risk": {
            "mode": "r_fixed",
            "r_per_trade": 0.01,
            "margin": {"leverage": 2.0},
        },
        "instrument": {"type": "equity", "symbol": "AAPL"},
    }
    engine = _engine(config)

    qty, meta = engine.compute_position_size_r(
        symbol="AAPL",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 98.0},
        bars_by_symbol={},
        ctx={},
        equity=10_000.0,
    )

    assert qty == 50.0
    assert meta["qty_rounding_unit"] == 1.0
    assert meta["margin_required"] == pytest.approx(2500.0)


def test_crypto_regression_sizing_unchanged() -> None:
    cfg_base = {"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "qty_rounding": "none"}}
    cfg_crypto = {
        "risk": {"mode": "r_fixed", "r_per_trade": 0.01, "qty_rounding": "none"},
        "instrument": {"type": "crypto", "symbol": "BTCUSDT"},
    }

    qty_a, meta_a = _engine(cfg_base).compute_position_size_r(
        symbol="BTCUSDT",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 95.0},
        bars_by_symbol={},
        ctx={},
        equity=10_000.0,
    )
    qty_b, meta_b = _engine(cfg_crypto).compute_position_size_r(
        symbol="BTCUSDT",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 95.0},
        bars_by_symbol={},
        ctx={},
        equity=10_000.0,
    )

    assert qty_a == pytest.approx(20.0)
    assert qty_b == pytest.approx(qty_a)
    assert meta_a["risk_amount"] == meta_b["risk_amount"]
    assert meta_a["stop_distance"] == meta_b["stop_distance"]


def test_missing_required_fx_fields_error_clarity() -> None:
    with pytest.raises(ValueError, match=r"instrument\.contract_size.*instrument\.type=forex"):
        resolve_config(
            {
                "risk": {"mode": "r_fixed", "r_per_trade": 0.01, "fx": {"lot_step": 0.01}},
                "instrument": {"type": "forex", "symbol": "EURUSD"},
                "execution": {"spread_mode": "fixed_bps"},
            }
        )

    with pytest.raises(ValueError, match=r"risk\.fx\.lot_step.*instrument\.type=forex"):
        resolve_config(
            {
                "risk": {"mode": "r_fixed", "r_per_trade": 0.01},
                "instrument": {"type": "forex", "symbol": "EURUSD", "contract_size": 100000},
                "execution": {"spread_mode": "fixed_bps"},
            }
        )
