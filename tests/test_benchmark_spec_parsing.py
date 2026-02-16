import pytest

from bt.benchmark import BenchmarkSpec, parse_benchmark_spec


def test_default_disabled_when_missing() -> None:
    spec = parse_benchmark_spec({})

    assert spec.enabled is False
    assert spec.symbol is None
    assert spec.price_field == "close"


def test_enabled_requires_symbol() -> None:
    with pytest.raises(ValueError, match="benchmark.symbol"):
        parse_benchmark_spec({"benchmark": {"enabled": True}})


def test_valid_enabled_minimal() -> None:
    spec = parse_benchmark_spec({"benchmark": {"enabled": True, "symbol": "BTCUSDT"}})

    assert spec == BenchmarkSpec(
        enabled=True,
        symbol="BTCUSDT",
        price_field="close",
        initial_equity=None,
        fee_model="none",
    )


def test_invalid_price_field_raises() -> None:
    with pytest.raises(ValueError, match="benchmark.price_field"):
        parse_benchmark_spec(
            {
                "benchmark": {
                    "enabled": True,
                    "symbol": "BTCUSDT",
                    "price_field": "hl2",
                }
            }
        )


@pytest.mark.parametrize("initial_equity", [0, -1])
def test_invalid_initial_equity_raises(initial_equity: float) -> None:
    with pytest.raises(ValueError, match="benchmark.initial_equity"):
        parse_benchmark_spec(
            {
                "benchmark": {
                    "enabled": True,
                    "symbol": "BTCUSDT",
                    "initial_equity": initial_equity,
                }
            }
        )


def test_fee_model_only_none_supported() -> None:
    with pytest.raises(ValueError, match="benchmark.fee_model"):
        parse_benchmark_spec(
            {
                "benchmark": {
                    "enabled": True,
                    "symbol": "BTCUSDT",
                    "fee_model": "taker",
                }
            }
        )
