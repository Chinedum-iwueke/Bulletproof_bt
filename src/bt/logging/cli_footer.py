"""CLI footer helpers for concise run completion output."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable


def print_run_footer(run_dir: Path) -> None:
    """
    Print a short, user-friendly completion footer for a single run.
    """
    artifact_count = sum(1 for path in run_dir.iterdir() if path.is_file())
    print("Run completed successfully.")
    print(f"Run dir: {run_dir}")
    print(f"Open: {run_dir / 'summary.txt'}")
    print(f"Artifacts: {artifact_count} files")


def print_grid_footer(run_dirs: Iterable[Path], *, out_dir: Path) -> None:
    """
    Print a short summary footer for a grid run (N runs written).
    """
    run_count = sum(1 for _ in run_dirs)
    print("Experiment grid completed successfully.")
    print(f"Output root: {out_dir}")
    print(f"Runs written: {run_count}")
    print("Tip: open any run's summary.txt for the 1-page overview.")
