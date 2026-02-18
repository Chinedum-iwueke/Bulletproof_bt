from __future__ import annotations

from pathlib import Path

from bt.logging.cli_footer import print_grid_footer, print_run_footer


def test_print_run_footer_outputs_expected_lines(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "run_001"
    run_dir.mkdir()
    (run_dir / "summary.txt").write_text("summary", encoding="utf-8")
    (run_dir / "equity.csv").write_text("equity", encoding="utf-8")
    (run_dir / "trades.csv").write_text("trades", encoding="utf-8")

    print_run_footer(run_dir)

    out = capsys.readouterr().out
    assert "Run completed successfully." in out
    assert "Run dir:" in out
    assert "Open:" in out
    assert "Artifacts: 3 files" in out


def test_print_grid_footer_outputs_expected_lines(tmp_path: Path, capsys) -> None:
    out_dir = tmp_path / "grid_out"
    out_dir.mkdir()
    run1 = out_dir / "run_a"
    run2 = out_dir / "run_b"
    run1.mkdir()
    run2.mkdir()

    print_grid_footer([run1, run2], out_dir=out_dir)

    out = capsys.readouterr().out
    assert "Experiment grid completed successfully." in out
    assert "Runs written: 2" in out
