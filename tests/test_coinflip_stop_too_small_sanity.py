from __future__ import annotations

import json
from pathlib import Path

import yaml

from bt.api import run_backtest


def test_coinflip_run_logs_stop_too_small_rejections_and_sanity_counter(tmp_path: Path) -> None:
    """Verify with: rg -n 'stop_too_small|sanity.json' -S <run_dir>."""
    override = {
        "strategy": {
            "name": "coinflip",
            "p_trade": 1.0,
            "cooldown_bars": 0,
        },
        "risk": {
            "min_stop_distance_pct": 0.02,
        },
    }
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump(override, sort_keys=False), encoding="utf-8")

    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            override_paths=[str(override_path)],
            run_name="stop-too-small-smoke",
        )
    )

    decisions_path = run_dir / "decisions.jsonl"
    decisions = [json.loads(line) for line in decisions_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    rejected_stop_small = [
        row
        for row in decisions
        if row.get("approved") is False and "stop_too_small" in str(row.get("reason"))
    ]
    assert rejected_stop_small

    sanity_path = run_dir / "sanity.json"
    assert sanity_path.exists()
    sanity = json.loads(sanity_path.read_text(encoding="utf-8"))
    assert int(sanity["rejected_by_reason"].get("stop_too_small", 0)) >= 1
    assert sanity["signals_emitted"] == sanity["signals_approved"] + sanity["signals_rejected"]
