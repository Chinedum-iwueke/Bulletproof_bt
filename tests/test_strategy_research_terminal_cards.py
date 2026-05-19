from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from orchestrator.db import ResearchDB
from orchestrator.research_terminal.cards import (
    CARD_SCHEMA_VERSION,
    CARD_TYPES,
    build_and_write_failure_cards,
    build_and_write_intelligence_cards,
)
from orchestrator.research_terminal.terminal import research_status, recommendation_summary
from bt.saas.models import STRATEGY_RESEARCH_TERMINAL_BUNDLE_SCHEMA_VERSION
from bt.saas.service import StrategyRobustnessLabService


def _write_minimal_artifacts(root: Path, *, rich: bool = False) -> None:
    (root / "summaries").mkdir(parents=True)
    (root / "research_data").mkdir(parents=True)
    pd.DataFrame(
        [
            {"run_id": "r1", "ev_r_net": 0.25, "n_trades": 12, "metrics_valid": True},
            {"run_id": "r2", "ev_r_net": -0.05, "n_trades": 8, "metrics_valid": True},
        ]
    ).to_csv(root / "summaries" / "run_summary.csv", index=False)
    pd.DataFrame([{"run_id": "r1", "ev_r_net": 0.25}, {"run_id": "r2", "ev_r_net": -0.05}]).to_parquet(
        root / "research_data" / "runs_dataset.parquet"
    )
    trades = pd.DataFrame(
        [
            {
                "run_id": "r1",
                "symbol": "BTCUSDT",
                "pnl": 10.0,
                "gross_pnl": 13.0,
                "fees": 2.0,
                "r_multiple_net": 0.5,
                "r_multiple_gross": 0.65,
            }
        ]
    )
    if rich:
        trades = trades.assign(
            entry_state_funding_pctile=0.9,
            entry_state_oi_accel_pctile=0.8,
            entry_state_basis_pctile=0.7,
            entry_state_csi_source="enriched",
        )
        pd.DataFrame([{"bucket": "funding_very_positive", "ev_r_net": 0.3}]).to_csv(
            root / "summaries" / "ev_by_bucket_funding.csv",
            index=False,
        )
        pd.DataFrame([{"bucket": "oi_accel_high", "ev_r_net": 0.2}]).to_csv(
            root / "summaries" / "ev_by_bucket_oi_accel.csv",
            index=False,
        )
        pd.DataFrame([{"bucket": "basis_premium", "ev_r_net": 0.1}]).to_csv(
            root / "summaries" / "ev_by_bucket_basis.csv",
            index=False,
        )
    trades.to_parquet(root / "research_data" / "trades_dataset.parquet")


def _db(tmp_path: Path) -> tuple[ResearchDB, str, str]:
    db = ResearchDB(tmp_path / "research.sqlite", repo_root=tmp_path)
    db.init_schema()
    hypothesis_id = db.upsert_hypothesis_by_name(name="h1", yaml_path=tmp_path / "h1.yaml", status="IMPLEMENTED")
    pipeline_id = db.create_pipeline_run(
        name="h1",
        phase="tier2",
        hypothesis_path=tmp_path / "h1.yaml",
        hypothesis_id=hypothesis_id,
    )
    return db, hypothesis_id, pipeline_id


def test_strategy_terminal_cards_generated_and_registered(tmp_path: Path) -> None:
    stable = tmp_path / "outputs" / "tier2" / "h1_parallel_stable"
    volatile = tmp_path / "outputs" / "tier2" / "h1_parallel_vol"
    _write_minimal_artifacts(stable, rich=True)
    _write_minimal_artifacts(volatile, rich=False)
    hypothesis = tmp_path / "h1.yaml"
    hypothesis.write_text("name: h1\nstrategy: sample\n", encoding="utf-8")
    bundle = tmp_path / "outputs" / "tier2" / "h1_verdict_bundle"
    bundle.mkdir(parents=True)
    (bundle / "manifest.json").write_text(json.dumps({"name": "h1"}), encoding="utf-8")
    db, hypothesis_id, pipeline_id = _db(tmp_path)

    result = build_and_write_intelligence_cards(
        name="h1",
        phase="tier2",
        hypothesis_path=hypothesis,
        stable_root=stable,
        volatile_root=volatile,
        output_dir=tmp_path / "outputs" / "tier2" / "h1_strategy_terminal_cards",
        project_root=tmp_path,
        pipeline_run_id=pipeline_id,
        verdict_bundle_dir=bundle,
        db=db,
        hypothesis_id=hypothesis_id,
    )

    assert result.bundle_json.exists()
    payload = json.loads(result.bundle_json.read_text(encoding="utf-8"))
    assert payload["schema_version"] == STRATEGY_RESEARCH_TERMINAL_BUNDLE_SCHEMA_VERSION
    assert {card["card_type"] for card in payload["cards"]} == set(CARD_TYPES)
    assert all(card["schema_version"] == CARD_SCHEMA_VERSION for card in payload["cards"])
    regime = next(card for card in payload["cards"] if card["card_type"] == "RegimeDependencyCard")
    assert "entry_state_funding_pctile" in regime["data"]["rich_state_columns_detected"]

    artifacts = db.list_artifacts(pipeline_run_id=pipeline_id)
    assert any(row["artifact_type"] == "strategy_terminal_cards_json" for row in artifacts)
    assert any(row["artifact_type"] == "strategy_terminal_card_VerdictCard" for row in artifacts)


def test_strategy_terminal_missing_artifacts_degrade_gracefully(tmp_path: Path) -> None:
    hypothesis = tmp_path / "h2.yaml"
    hypothesis.write_text("name: h2\n", encoding="utf-8")
    result = build_and_write_intelligence_cards(
        name="h2",
        phase="tier2",
        hypothesis_path=hypothesis,
        stable_root=tmp_path / "missing_stable",
        volatile_root=tmp_path / "missing_vol",
        output_dir=tmp_path / "cards",
        project_root=tmp_path,
    )
    payload = json.loads(result.bundle_json.read_text(encoding="utf-8"))
    run_quality = next(card for card in payload["cards"] if card["card_type"] == "RunQualityCard")
    assert run_quality["data"]["stable"]["available"] is False
    assert any("Missing" in warning for warning in run_quality["warnings"])


def test_strategy_terminal_failure_cards_capture_error(tmp_path: Path) -> None:
    log = tmp_path / "pipeline.log"
    log.write_text("Traceback\nFileNotFoundError: missing manifest\n", encoding="utf-8")
    result = build_and_write_failure_cards(
        name="h3",
        phase="tier2",
        hypothesis_path=None,
        stable_root=None,
        volatile_root=None,
        output_dir=tmp_path / "cards",
        project_root=tmp_path,
        pipeline_log_path=log,
        error_message="missing manifest",
    )
    payload = json.loads(result.bundle_json.read_text(encoding="utf-8"))
    failure = next(card for card in payload["cards"] if card["card_type"] == "FailureCauseCard")
    assert failure["data"]["failure_detected"] is True
    assert failure["data"]["root_cause_hint"] == "missing_artifact_or_path"


def test_strategy_terminal_saas_contract_loads_bundle(tmp_path: Path) -> None:
    hypothesis = tmp_path / "h4.yaml"
    hypothesis.write_text("name: h4\n", encoding="utf-8")
    result = build_and_write_intelligence_cards(
        name="h4",
        phase="tier2",
        hypothesis_path=hypothesis,
        stable_root=tmp_path / "stable",
        volatile_root=tmp_path / "vol",
        output_dir=tmp_path / "cards",
        project_root=tmp_path,
    )
    bundle = StrategyRobustnessLabService().load_strategy_terminal_bundle(result.bundle_json)
    assert bundle.schema_version == STRATEGY_RESEARCH_TERMINAL_BUNDLE_SCHEMA_VERSION
    assert len(bundle.cards) == len(CARD_TYPES)
    assert bundle.cards[0].schema_version == CARD_SCHEMA_VERSION


def test_strategy_terminal_command_helpers(tmp_path: Path) -> None:
    db, hypothesis_id, pipeline_id = _db(tmp_path)
    cards_dir = tmp_path / "cards"
    cards_dir.mkdir()
    card_path = cards_dir / "NextExperimentCard.json"
    card_path.write_text(
        json.dumps(
            {
                "schema_version": CARD_SCHEMA_VERSION,
                "card_type": "NextExperimentCard",
                "card_id": "x",
                "name": "h1",
                "phase": "tier2",
                "hypothesis_name": "h1",
                "pipeline_run_id": pipeline_id,
                "created_at": "2026-01-01T00:00:00+00:00",
                "source_artifacts": {},
                "data": {"recommended_action": "PROMOTE_TIER3", "promotion_or_scrap_summary": "candidate"},
                "warnings": [],
            }
        ),
        encoding="utf-8",
    )
    db.register_artifact(
        artifact_type="strategy_terminal_card_NextExperimentCard",
        path=card_path,
        hypothesis_id=hypothesis_id,
        pipeline_run_id=pipeline_id,
    )
    status = research_status(tmp_path / "research.sqlite")
    assert status["pipeline_runs"]["STARTED"] == 1
    recs = recommendation_summary(tmp_path / "research.sqlite")
    assert recs["recommendations"][0]["recommended_action"] == "PROMOTE_TIER3"
