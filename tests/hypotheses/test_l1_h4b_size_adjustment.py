from bt.hypotheses.l1_h4b import capped_inverse_spread_ratio


def test_size_factor_bounded_and_ratio_driven() -> None:
    assert capped_inverse_spread_ratio(spread_proxy_t=0.01, spread_proxy_ref=0.02, cap_multiplier=0.5) == 1.0
    assert capped_inverse_spread_ratio(spread_proxy_t=0.02, spread_proxy_ref=0.01, cap_multiplier=0.5) == 0.5
    assert capped_inverse_spread_ratio(spread_proxy_t=0.015, spread_proxy_ref=0.01, cap_multiplier=0.5) == (0.01 / 0.015)


def test_size_factor_handles_non_positive_spread_proxy_safely() -> None:
    assert capped_inverse_spread_ratio(spread_proxy_t=0.0, spread_proxy_ref=0.01, cap_multiplier=0.75) == 1.0
    assert capped_inverse_spread_ratio(spread_proxy_t=None, spread_proxy_ref=0.01, cap_multiplier=0.75) is None
