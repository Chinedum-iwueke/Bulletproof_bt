"""Process-level parallel runner for H1B manifests."""
from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from bt.experiments.parallel_grid import cli_run_parallel_grid


if __name__ == "__main__":
    raise SystemExit(cli_run_parallel_grid())
