from __future__ import annotations

import pytest

from bt.portfolio.portfolio import Portfolio


def test_negative_free_margin_raises_runtime_error_not_assertion(monkeypatch: pytest.MonkeyPatch) -> None:
    portfolio = Portfolio(initial_cash=100.0, max_leverage=2.0)

    monkeypatch.setattr(portfolio, "_calculate_used_margin", lambda: 500.0)

    with pytest.raises(RuntimeError, match="Negative free_margin"):
        portfolio.mark_to_market({})
