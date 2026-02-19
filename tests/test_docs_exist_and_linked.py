from __future__ import annotations

from pathlib import Path


DOC_PATHS = [
    "docs/dataset_contract.md",
    "docs/execution_model_contract.md",
    "docs/strategy_contract.md",
    "docs/portfolio_risk_contract.md",
    "docs/error_and_run_status_contract.md",
    "docs/output_artifacts_contract.md",
    "docs/config_layering_contract.md",
    "docs/beginner_vs_pro_contract.md",
]


def test_docs_contract_pack_exists() -> None:
    for rel_path in DOC_PATHS:
        assert Path(rel_path).is_file(), f"Missing contract doc: {rel_path}"


def test_readme_links_to_all_contract_docs() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    for rel_path in DOC_PATHS:
        assert rel_path in readme, f"README is missing link/path: {rel_path}"
