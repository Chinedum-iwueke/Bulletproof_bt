from __future__ import annotations

import json
from pathlib import Path

import pytest

from bt.metrics.reconcile import reconcile_execution_costs


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def test_reconciliation_passes_on_known_costs(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    _write_jsonl(
        run_dir / "fills.jsonl",
        [
            {"order_id": "o1", "fee_cost": 1.25, "slippage_cost": 0.5, "spread_cost": 0.2},
            {"order_id": "o2", "fee_cost": 0.75, "slippage_cost": 0.25, "spread_cost": 0.3},
        ],
    )
    (run_dir / "performance.json").write_text(
        json.dumps(
            {
                "fee_total": 2.0,
                "slippage_total": 0.75,
                "spread_total": 0.5,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    reconcile_execution_costs(run_dir)


def test_reconciliation_fails_on_mismatch(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    _write_jsonl(
        run_dir / "fills.jsonl",
        [
            {"order_id": "o1", "fee_cost": 1.0, "slippage_cost": 0.5, "spread_cost": 0.25},
            {"order_id": "o2", "fee_cost": 0.5, "slippage_cost": 0.5, "spread_cost": 0.25},
        ],
    )
    (run_dir / "performance.json").write_text(
        json.dumps(
            {
                "fee_total": 1.5,
                "slippage_total": 1.00001,
                "spread_total": 0.5,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc_info:
        reconcile_execution_costs(run_dir)

    message = str(exc_info.value)
    assert str(run_dir) in message
    assert "slippage_total" in message
    assert "expected(performance)" in message
    assert "actual(sum_fills)" in message


def test_missing_spread_field_treated_as_zero(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    _write_jsonl(
        run_dir / "fills.jsonl",
        [
            {"order_id": "o1", "fee_cost": 0.1, "slippage_cost": 0.2},
            {"order_id": "o2", "fee_cost": 0.3, "slippage_cost": 0.4},
        ],
    )
    (run_dir / "performance.json").write_text(
        json.dumps(
            {
                "fee_total": 0.4,
                "slippage_total": 0.6,
                "spread_total": 0.0,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    reconcile_execution_costs(run_dir)


@pytest.mark.parametrize(
    "missing_name, create_performance, create_fills",
    [
        ("fills.jsonl", True, False),
        ("performance.json", False, True),
    ],
)
def test_missing_files_raise_actionable_valueerror(
    tmp_path: Path,
    missing_name: str,
    create_performance: bool,
    create_fills: bool,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    if create_fills:
        _write_jsonl(
            run_dir / "fills.jsonl",
            [{"order_id": "o1", "fee_cost": 0.1, "slippage_cost": 0.2, "spread_cost": 0.0}],
        )

    if create_performance:
        (run_dir / "performance.json").write_text(
            json.dumps(
                {
                    "fee_total": 0.1,
                    "slippage_total": 0.2,
                    "spread_total": 0.0,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    with pytest.raises(ValueError) as exc_info:
        reconcile_execution_costs(run_dir)

    message = str(exc_info.value)
    assert str(run_dir) in message
    assert missing_name in message
