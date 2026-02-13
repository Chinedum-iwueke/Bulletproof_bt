"""CLI entrypoint for deterministic experiment grids."""
from __future__ import annotations

import argparse

from bt.api import run_grid


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

    run_grid(
        config_path=args.config,
        experiment_path=args.experiment,
        data_path=args.data,
        out_dir=args.out,
        override_paths=override_paths or None,
        experiment_name=None,
    )


if __name__ == "__main__":
    main()
