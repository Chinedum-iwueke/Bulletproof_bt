"""Tests for the stable and lightweight ``bt`` public surface."""

from __future__ import annotations

import importlib
import sys


HEAVY_MODULES = (
    "pandas",
    "pyarrow",
    "bt.core.engine",
    "bt.execution.execution_model",
)


def test_bt_public_api_exports():
    import bt

    assert hasattr(bt, "run_backtest")
    assert hasattr(bt, "run_grid")
    assert hasattr(bt, "__version__")


def test_import_bt_is_lightweight():
    for module_name in HEAVY_MODULES:
        sys.modules.pop(module_name, None)

    for module_name in list(sys.modules):
        if module_name == "bt" or module_name.startswith("bt."):
            sys.modules.pop(module_name, None)

    importlib.import_module("bt")

    for module_name in HEAVY_MODULES:
        assert module_name not in sys.modules


def test_run_backtest_still_works_via_bt_namespace(tmp_path):
    import bt

    run_dir = bt.run_backtest(
        config_path="configs/engine.yaml",
        data_path="data/curated/sample.csv",
        out_dir=str(tmp_path),
        override_paths=[],
        run_name="public_surface_smoke",
    )

    assert run_dir == str(tmp_path / "public_surface_smoke")
    assert (tmp_path / "public_surface_smoke").exists()
