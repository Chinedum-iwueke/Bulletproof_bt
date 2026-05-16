from __future__ import annotations

import json
from pathlib import Path

from bt.saas.models import NormalizedTradeRecord, ParsedArtifactInput
from bt.saas.service import StrategyRobustnessLabService


def _trade_csv_artifact() -> ParsedArtifactInput:
    return ParsedArtifactInput(
        artifact_kind="trade_csv",
        richness="trade_only",
        strategy_metadata={"strategy_name": "envelope_fixture"},
        trades=[
            NormalizedTradeRecord(
                trade_id="t1",
                symbol="BTCUSDT",
                side="LONG",
                entry_time="2024-01-01T00:00:00Z",
                exit_time="2024-01-01T00:30:00Z",
                entry_price=100.0,
                exit_price=101.0,
                quantity=1.0,
            ),
            NormalizedTradeRecord(
                trade_id="t2",
                symbol="BTCUSDT",
                side="SHORT",
                entry_time="2024-01-01T01:00:00Z",
                exit_time="2024-01-01T01:30:00Z",
                entry_price=102.0,
                exit_price=101.0,
                quantity=1.0,
            ),
        ],
    )


def test_run_analysis_from_parsed_artifact_emits_engine_envelope_v1() -> None:
    result = StrategyRobustnessLabService().run_analysis_from_parsed_artifact(_trade_csv_artifact())
    expected = json.loads(
        Path("tests/fixtures/engine-seam/trade_csv_limited_expected_envelope.json").read_text()
    )

    assert result.envelope.engine_name == expected["engine_name"]
    assert result.envelope.seam_name == expected["seam_name"]
    assert result.envelope.seam_version == expected["seam_version"]
    assert result.envelope.adapter_version == expected["adapter_version"]
    assert result.envelope.parser_version == expected["parser_version"]
    assert result.envelope.capability_profile_version == expected["capability_profile_version"]
    assert result.envelope.diagnostic_contract_version == expected["diagnostic_contract_version"]
    assert result.capability_profile.diagnostics["overview"].status in {"supported", "limited"}
