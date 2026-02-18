"""CLI entrypoint for deterministic experiment grids."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.api import run_grid
from bt.logging.run_contract import validate_run_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic backtest experiment grid")
    parser.add_argument("--config", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--override", action="append", default=[])
    parser.add_argument("--local-config")
    args = parser.parse_args()

    override_paths = list(args.override)
    if args.local_config:
        override_paths.append(args.local_config)

    experiment_dir = run_grid(
        config_path=args.config,
        experiment_path=args.experiment,
        data_path=args.data,
        out_dir=args.out,
        override_paths=override_paths or None,
        experiment_name=None,
    )

    runs_dir = Path(experiment_dir) / "runs"
    summary_path = Path(experiment_dir) / "summary.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    runs = summary_payload.get("runs")
    if not isinstance(runs, list):
        raise ValueError(f"Invalid summary.json format at {summary_path}; expected list at 'runs'.")

    for row in runs:
        if not isinstance(row, dict):
            raise ValueError(f"Invalid run row in {summary_path}; expected object entries in 'runs'.")
        run_name = row.get("run_name")
        status = row.get("status")
        if not isinstance(run_name, str) or not run_name:
            raise ValueError(f"Invalid run_name in {summary_path}; expected non-empty string.")
        if status == "PASS":
            validate_run_artifacts(runs_dir / run_name)


if __name__ == "__main__":
    main()
