from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from pathlib import Path
import shlex
import subprocess
import threading
from typing import TextIO


DEFAULT_FAILURE_TAIL_LINES = 120


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_step_name(step: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", step).strip("_") or "step"


def detect_root_cause(stdout_tail: str, stderr_tail: str) -> str | None:
    text = f"{stderr_tail}\n{stdout_tail}".strip()
    if not text:
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    joined = "\n".join(lines)

    if "Traceback (most recent call last)" in joined:
        for line in reversed(lines):
            if ":" in line and not line.startswith("File "):
                return line

    for marker, hint in (
        ("ModuleNotFoundError", "Missing Python dependency."),
        ("FileNotFoundError", "Missing file/path."),
        ("KeyError", "Missing expected field/column/key."),
        ("pandas.errors", "Pandas/dataframe error."),
        ("No such file or directory", "Missing path or command."),
        ("Permission denied", "Permission issue."),
        ("MemoryError", "Memory pressure / OOM possible."),
        ("Killed", "Process killed; memory pressure / OOM possible."),
    ):
        if marker in joined:
            if marker in {"KeyError", "FileNotFoundError", "ModuleNotFoundError", "MemoryError"}:
                for line in reversed(lines):
                    if marker in line:
                        return line
            return hint

    for marker in ("IndexError", "TypeError", "ValueError"):
        if marker in joined:
            for line in reversed(lines):
                if marker in line:
                    return line

    if "returned non-zero exit status" in joined:
        for line in reversed(lines):
            if "returned non-zero exit status" in line:
                return line

    return None


class PipelineCommandError(RuntimeError):
    def __init__(self, *, step: str, cmd: list[str], returncode: int, cwd: str | None, stdout_tail: str, stderr_tail: str, stdout_path: str | None, stderr_path: str | None, root_cause_hint: str | None = None) -> None:
        self.step = step
        self.cmd = cmd
        self.returncode = returncode
        self.cwd = cwd
        self.stdout_tail = stdout_tail
        self.stderr_tail = stderr_tail
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.root_cause_hint = root_cause_hint
        super().__init__(self.compact_message())

    def compact_message(self) -> str:
        hint = self.root_cause_hint or "No automatic hint detected."
        stderr_log = self.stderr_path or "n/a"
        return f'{self.step} failed exit={self.returncode} root_cause="{hint}" stderr_log="{stderr_log}"'

    def to_failure_block(self) -> str:
        cmd_rendered = shlex.join(self.cmd)
        hint = self.root_cause_hint or "No automatic hint detected. Inspect stderr log."
        return (
            "\n" + "=" * 80 + "\n"
            "PIPELINE COMMAND FAILED\n"
            f"Step: {self.step}\n"
            f"Exit code: {self.returncode}\n"
            "Command:\n"
            f"  {cmd_rendered}\n"
            "CWD:\n"
            f"  {self.cwd or ''}\n"
            "STDOUT log:\n"
            f"  {self.stdout_path or ''}\n"
            "STDERR log:\n"
            f"  {self.stderr_path or ''}\n\n"
            "STDOUT tail:\n"
            f"{self.stdout_tail}\n\n"
            "STDERR tail:\n"
            f"{self.stderr_tail}\n\n"
            "Root-cause hint:\n"
            f"  {hint}\n"
            + "=" * 80
        )


@dataclass
class CommandRunResult:
    step: str
    cmd: list[str]
    returncode: int
    cwd: str | None
    stdout_log: str | None
    stderr_log: str | None
    started_at: str
    completed_at: str
    root_cause_hint: str | None


def run_pipeline_command(*, cmd: list[str], step: str, cwd: Path | None, log_path: Path, command_log_dir: Path | None, sequence_num: int, dry_run: bool, capture_logs: bool, failure_tail_lines: int = DEFAULT_FAILURE_TAIL_LINES) -> CommandRunResult:
    started_at = utc_now_iso()
    if dry_run:
        completed_at = utc_now_iso()
        return CommandRunResult(step, cmd, 0, str(cwd) if cwd else None, None, None, started_at, completed_at, None)

    stdout_path = None
    stderr_path = None
    stdout_file: TextIO | None = None
    stderr_file: TextIO | None = None
    if capture_logs and command_log_dir is not None:
        safe = _safe_step_name(step)
        stdout_path = command_log_dir / f"{sequence_num:03d}_{safe}.stdout.log"
        stderr_path = command_log_dir / f"{sequence_num:03d}_{safe}.stderr.log"
        command_log_dir.mkdir(parents=True, exist_ok=True)
        stdout_file = stdout_path.open("w", encoding="utf-8")
        stderr_file = stderr_path.open("w", encoding="utf-8")

    stdout_tail: deque[str] = deque(maxlen=failure_tail_lines)
    stderr_tail: deque[str] = deque(maxlen=failure_tail_lines)

    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(f"[{started_at}] STEP={step}\n")
        fh.write(f"CMD: {shlex.join(cmd)}\n")

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, cwd=str(cwd) if cwd else None)

    def reader(stream: TextIO, tail: deque[str], label: str, sink: TextIO | None) -> None:
        for line in iter(stream.readline, ""):
            line = line.rstrip("\n")
            print(f"[{step}:{label}] {line}")
            tail.append(line)
            if sink is not None:
                sink.write(line + "\n")
        stream.close()

    assert process.stdout is not None and process.stderr is not None
    t_out = threading.Thread(target=reader, args=(process.stdout, stdout_tail, "stdout", stdout_file), daemon=True)
    t_err = threading.Thread(target=reader, args=(process.stderr, stderr_tail, "stderr", stderr_file), daemon=True)
    t_out.start(); t_err.start()
    returncode = process.wait()
    t_out.join(); t_err.join()

    if stdout_file is not None:
        stdout_file.close()
    if stderr_file is not None:
        stderr_file.close()

    completed_at = utc_now_iso()
    stdout_tail_text = "\n".join(stdout_tail)
    stderr_tail_text = "\n".join(stderr_tail)
    hint = detect_root_cause(stdout_tail_text, stderr_tail_text)

    if returncode != 0:
        raise PipelineCommandError(
            step=step,
            cmd=cmd,
            returncode=returncode,
            cwd=str(cwd) if cwd else None,
            stdout_tail=stdout_tail_text,
            stderr_tail=stderr_tail_text,
            stdout_path=str(stdout_path) if stdout_path else None,
            stderr_path=str(stderr_path) if stderr_path else None,
            root_cause_hint=hint,
        )

    with log_path.open("a", encoding="utf-8") as fh:
        fh.write("STATUS: SUCCESS\n\n")

    return CommandRunResult(step, cmd, returncode, str(cwd) if cwd else None, str(stdout_path) if stdout_path else None, str(stderr_path) if stderr_path else None, started_at, completed_at, hint)
