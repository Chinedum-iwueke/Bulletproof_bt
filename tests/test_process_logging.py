from __future__ import annotations

import json
import logging
from pathlib import Path
import sys

import pytest

from orchestrator.process_logging import CommandExecutionError, detect_root_cause, run_logged_command


def _logger() -> logging.Logger:
    logger = logging.getLogger("test_process_logging")
    logger.handlers.clear()
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.setLevel(logging.INFO)
    return logger


def test_run_logged_command_captures_stdout_stderr_and_manifest(tmp_path: Path) -> None:
    script = tmp_path / "ok.py"
    script.write_text("print('out-line')\nimport sys\nprint('err-line', file=sys.stderr)\n", encoding="utf-8")
    log_dir = tmp_path / "logs"
    result = run_logged_command(stage="001_pipeline", cmd=[sys.executable, str(script)], log_dir=log_dir, logger=_logger(), queue_id="q1", job_name="job")
    assert "out-line" in Path(result.stdout_log).read_text(encoding="utf-8")
    assert "err-line" in Path(result.stderr_log).read_text(encoding="utf-8")
    manifest = json.loads((log_dir / "command_log_manifest.json").read_text(encoding="utf-8"))
    assert manifest["queue_id"] == "q1"
    assert manifest["commands"][0]["stage"] == "001_pipeline"


def test_failing_command_raises_with_tails(tmp_path: Path) -> None:
    script = tmp_path / "fail.py"
    script.write_text("print('before')\nraise KeyError('x')\n", encoding="utf-8")
    with pytest.raises(CommandExecutionError) as exc_info:
        run_logged_command(stage="002_state_discovery", cmd=[sys.executable, str(script)], log_dir=tmp_path / "logs", logger=_logger())
    assert "before" in exc_info.value.result.stdout_tail
    assert "KeyError" in (exc_info.value.result.stderr_tail + (exc_info.value.result.root_cause_hint or ""))


def test_root_cause_keyerror_from_traceback() -> None:
    stderr = "Traceback (most recent call last):\n  File 'x', line 1\nKeyError: 'foo'\n"
    assert detect_root_cause("", stderr) == "KeyError: 'foo'"
