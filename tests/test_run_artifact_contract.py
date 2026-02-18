from __future__ import annotations

from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.logging.run_contract import REQUIRED_ARTIFACTS, validate_run_artifacts


def _touch_all(run_dir: Path, artifacts: list[str]) -> None:
    for artifact_name in artifacts:
        (run_dir / artifact_name).touch()


def test_validate_run_artifacts_passes_with_all_required_files(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _touch_all(run_dir, REQUIRED_ARTIFACTS)

    validate_run_artifacts(run_dir)


def test_validate_run_artifacts_raises_when_single_artifact_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    missing_artifact = "trades.csv"
    _touch_all(run_dir, [name for name in REQUIRED_ARTIFACTS if name != missing_artifact])

    with pytest.raises(ValueError) as excinfo:
        validate_run_artifacts(run_dir)

    message = str(excinfo.value)
    assert "Run artifact validation failed" in message
    assert str(run_dir) in message
    assert missing_artifact in message


def test_validate_run_artifacts_lists_multiple_missing_files_in_sorted_order(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    missing = {"trades.csv", "equity.csv"}
    _touch_all(run_dir, [name for name in REQUIRED_ARTIFACTS if name not in missing])

    with pytest.raises(ValueError) as excinfo:
        validate_run_artifacts(run_dir)

    message = str(excinfo.value)
    equity_index = message.index("equity.csv")
    trades_index = message.index("trades.csv")
    assert equity_index < trades_index
