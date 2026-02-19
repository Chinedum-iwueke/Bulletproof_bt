from bt.risk import reject_codes


def test_reject_codes_are_unique_and_non_empty() -> None:
    code_values = [
        value
        for name, value in vars(reject_codes).items()
        if name.startswith("RISK_")
    ]

    assert code_values
    assert all(isinstance(value, str) for value in code_values)
    assert all(value for value in code_values)
    assert len(code_values) == len(set(code_values))
    assert all(
        value.startswith("risk_reject:") or value.startswith("risk_fallback:")
        for value in code_values
    )
