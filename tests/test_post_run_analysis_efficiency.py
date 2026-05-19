from __future__ import annotations

import subprocess
import sys


def test_post_run_analysis_exposes_incremental_control_flags() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/post_run_analysis.py", "--help"],
        check=True,
        text=True,
        capture_output=True,
    )
    assert "--jobs" in result.stdout
    assert "--force" in result.stdout
