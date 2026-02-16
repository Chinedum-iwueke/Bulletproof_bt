from __future__ import annotations

from bt.portfolio.portfolio import Portfolio


def test_negative_free_margin_is_recorded_for_engine_liquidation_flow(monkeypatch) -> None:
    portfolio = Portfolio(initial_cash=100.0, max_leverage=2.0)

    monkeypatch.setattr(portfolio, "_calculate_used_margin", lambda: 500.0)

    portfolio.mark_to_market({})

    assert portfolio.free_margin < 0
