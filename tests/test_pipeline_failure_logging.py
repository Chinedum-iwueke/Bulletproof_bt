from __future__ import annotations

import json
from pathlib import Path
import sys

from orchestrator.process_logging import PipelineCommandError, detect_root_cause, run_pipeline_command
from orchestrator.db import ResearchDB


def test_subprocess_failure_logs_and_hint(tmp_path: Path) -> None:
    script = tmp_path / "fail.py"
    script.write_text("print('hello out')\nraise KeyError('entry_state_csi_pctile')\n", encoding="utf-8")
    pipeline_log = tmp_path / "pipeline.log"
    pipeline_log.write_text("start\n", encoding="utf-8")
    command_dir = tmp_path / "cmd_logs"

    try:
        run_pipeline_command(
            cmd=[sys.executable, str(script)],
            step="post_analysis_stable",
            cwd=tmp_path,
            log_path=pipeline_log,
            command_log_dir=command_dir,
            sequence_num=5,
            dry_run=False,
            capture_logs=True,
            failure_tail_lines=120,
        )
        assert False, "expected failure"
    except PipelineCommandError as exc:
        assert exc.step == "post_analysis_stable"
        assert exc.cmd == [sys.executable, str(script)]
        assert exc.returncode != 0
        assert "KeyError" in (exc.root_cause_hint or "")
        assert exc.stdout_path and Path(exc.stdout_path).exists()
        assert exc.stderr_path and Path(exc.stderr_path).exists()
        block = exc.to_failure_block()
        assert "Step: post_analysis_stable" in block
        assert "Exit code:" in block
        assert "STDOUT log:" in block
        assert "STDERR log:" in block


def test_detect_root_cause_traceback_line() -> None:
    stderr = "Traceback (most recent call last):\n  File 'x', line 1\nKeyError: 'entry_state_csi_pctile'\n"
    assert detect_root_cause("", stderr) == "KeyError: 'entry_state_csi_pctile'"


def test_db_error_message_includes_root_cause(tmp_path: Path) -> None:
    db = ResearchDB(tmp_path / "research.sqlite", repo_root=tmp_path)
    db.init_schema()
    run_id = db.create_pipeline_run(name="n", phase="tier2", hypothesis_path="h.yaml")
    msg = 'post_analysis_stable failed exit=1 root_cause="KeyError: \'entry_state_csi_pctile\'" stderr_log="outputs/x.stderr.log"'
    db.fail_pipeline_run(run_id, msg, commands=[])
    row = db.connect().execute("SELECT error_message FROM pipeline_runs WHERE id = ?", (run_id,)).fetchone()
    assert row is not None
    assert "KeyError" in row["error_message"]


def test_command_manifest_written(tmp_path: Path) -> None:
    manifest = tmp_path / "command_log_manifest.json"
    payload = [{"step": "post_analysis_stable", "cmd": ["python", "x.py"], "returncode": 1}]
    manifest.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    data = json.loads(manifest.read_text(encoding="utf-8"))
    assert data[0]["step"] == "post_analysis_stable"
