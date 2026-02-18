"""CLI entrypoint for v1 backtests."""
from __future__ import annotations

import argparse
from pathlib import Path

from bt.api import run_backtest
from bt.config import load_yaml
from bt.logging.cli_footer import print_run_footer
from bt.logging.run_contract import validate_run_artifacts
from bt.logging.run_manifest import write_run_manifest
from bt.logging.summary import write_summary_txt


def main() -> None:
    parser = argparse.ArgumentParser(description="Run backtest (v1).")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--data", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--override", action="append", default=[])
    parser.add_argument("--local-config")
    args = parser.parse_args()

    override_paths = list(args.override)
    if args.local_config:
        override_paths.append(args.local_config)

    run_dir = run_backtest(
        config_path=args.config,
        data_path=args.data,
        out_dir="outputs/runs",
        override_paths=override_paths or None,
        run_name=args.run_id,
    )

    validate_run_artifacts(Path(run_dir))

    config_path = Path(run_dir) / "config_used.yaml"
    try:
        config = load_yaml(config_path)
    except Exception as exc:  # pragma: no cover - defensive user-facing guard
        raise ValueError(f"Unable to read config_used.yaml from run_dir={run_dir}: {exc}") from exc
    if not isinstance(config, dict):
        raise ValueError(f"Invalid config_used.yaml format in run_dir={run_dir}; expected mapping.")

    write_run_manifest(Path(run_dir), config=config, data_path=args.data)
    write_summary_txt(Path(run_dir))
    print_run_footer(Path(run_dir))


if __name__ == "__main__":
    main()
