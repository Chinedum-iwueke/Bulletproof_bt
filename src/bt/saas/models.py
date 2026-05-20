"""Typed contracts for ingestion and engine-facing analysis seams."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


DiagnosticName = Literal[
    "overview",
    "distribution",
    "monte_carlo",
    "stability",
    "execution",
    "regimes",
    "ruin",
    "prop_evaluation_readiness",
    "report",
]

CapabilityStatus = Literal["supported", "limited", "unavailable"]
STRATEGY_TRUTH_ROOM_CONTRACT_VERSION = "1.0.0"

STRATEGY_TRUTH_ROOM_ARTIFACT_FAMILIES = (
    "trade_log_v1",
    "equity_curve_v1",
    "broker_export_v1",
    "backtest_report_v1",
    "strategy_config_v1",
    "prop_evaluation_rules_v1",
    "ohlcv_context_v1",
    "benchmark_series_v1",
    "parameter_sweep_v1",
    "declared_claims_v1",
    "strategy_truth_room_bundle_v1",
)

STRATEGY_TRUTH_ROOM_VERDICTS = (
    "structurally_credible",
    "promising_but_under_supported",
    "likely_overfit",
    "execution_fantasy",
    "data_insufficient",
    "regime_dependent",
    "untradeable_after_costs",
)

STRATEGY_TRUTH_ROOM_EVIDENCE_STATES = (
    "supported",
    "limited",
    "unsupported",
    "contradicted",
    "unavailable",
    "plan_locked",
    "pending_review",
)

ArtifactKind = Literal[
    "trade_csv",
    "bundle_v1",
    "artifact_bundle",
    "parameter_sweep",
    "trade_log_v1",
    "equity_curve_v1",
    "broker_export_v1",
    "backtest_report_v1",
    "strategy_config_v1",
    "prop_evaluation_rules_v1",
    "ohlcv_context_v1",
    "benchmark_series_v1",
    "parameter_sweep_v1",
    "declared_claims_v1",
    "strategy_truth_room_bundle_v1",
]
ArtifactRichness = Literal[
    "trade_only",
    "trade_plus_metadata",
    "trade_plus_context",
    "research_complete",
]


@dataclass(frozen=True)
class ScorePayload:
    overall: float
    sub_scores: dict[str, float]
    methodology: dict[str, Any]


@dataclass(frozen=True)
class IngestedRun:
    source: str
    trades: Any
    equity: Any
    performance: dict[str, Any]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class NormalizedTradeRecord:
    symbol: str
    side: str
    entry_time: str
    exit_time: str | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    quantity: float | None = None
    fees: float | None = None
    pnl: float | None = None
    gross_pnl: float | None = None
    pnl_pct: float | None = None
    slippage: float | None = None
    risk_amount: float | None = None
    stop_distance: float | None = None
    r_multiple_net: float | None = None
    r_multiple_gross: float | None = None
    mae: float | None = None
    mfe: float | None = None
    duration_seconds: float | None = None
    strategy_name: str | None = None
    timeframe: str | None = None
    market: str | None = None
    exchange: str | None = None
    trade_id: str | None = None

@dataclass(frozen=True)
class ParameterSweepRunInput:
    run_id: str
    params: dict[str, int | float | str | bool]
    trades: list[NormalizedTradeRecord]
    summary: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class ParameterSweepInput:
    parameter_names: list[str]
    runs: list[ParameterSweepRunInput]
    assumptions: dict[str, Any] | None = None
    execution_context: dict[str, Any] | None = None

@dataclass(frozen=True)
class ParsedArtifactInput:
    artifact_kind: ArtifactKind
    richness: ArtifactRichness
    trades: list[NormalizedTradeRecord]
    strategy_metadata: dict[str, Any] = field(default_factory=dict)
    equity_curve: list[dict[str, Any]] | None = None
    assumptions: dict[str, Any] | None = None
    params: dict[str, Any] | None = None
    parameter_sweep: ParameterSweepInput | None = None
    ohlcv: list[dict[str, Any]] | None = None
    benchmark_series: list[dict[str, Any]] | None = None
    broker_exports: list[dict[str, Any]] | None = None
    declared_claims: list[dict[str, Any]] | None = None
    source_files: list[dict[str, Any]] = field(default_factory=list)
    bundle_manifest: dict[str, Any] | None = None
    asset_class_capabilities: dict[str, Any] = field(default_factory=dict)
    ohlcv_present: bool = False
    benchmark_present: bool = False
    broker_export_present: bool = False
    parser_notes: list[str] = field(default_factory=list)
    diagnostic_eligibility: dict[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class AnalysisRunConfig:
    seed: int = 42
    simulations: int = 1_000
    ruin_drawdown_levels: tuple[float, ...] = (0.30, 0.50)
    account_size: float | None = None
    risk_per_trade_pct: float | None = None
    benchmark: dict[str, Any] | None = None
    prop_evaluation_rules: dict[str, Any] | None = None


@dataclass(frozen=True)
class DiagnosticCapability:
    status: CapabilityStatus
    reason: str
    required_inputs: list[str]
    optional_enrichments: list[str]


@dataclass(frozen=True)
class AnalysisCapabilityProfile:
    diagnostics: dict[DiagnosticName, DiagnosticCapability]
    artifact_capabilities: dict[str, bool] = field(default_factory=dict)


ENGINE_SEAM_NAME = "run_analysis_from_parsed_artifact"
ENGINE_SEAM_VERSION = "1.0.0"
ENGINE_ADAPTER_VERSION = "1.0.0"
ENGINE_PARSER_VERSION = "1.0.0"
CAPABILITY_PROFILE_VERSION = "1.0.0"
DIAGNOSTIC_CONTRACT_VERSION = "1.0.0"
STRATEGY_RESEARCH_TERMINAL_CARD_SCHEMA_VERSION = "strategy_research_terminal.card.v1"
STRATEGY_RESEARCH_TERMINAL_BUNDLE_SCHEMA_VERSION = "strategy_research_terminal.bundle.v1"


@dataclass(frozen=True)
class EngineEnvelopeV1:
    engine_name: str
    engine_version: str | None
    strategy_truth_room_contract_version: str
    seam_name: str
    seam_version: str
    adapter_version: str
    parser_version: str
    capability_profile_version: str
    diagnostic_contract_version: str


@dataclass(frozen=True)
class EngineRunContext:
    artifact_kind: ArtifactKind
    richness: ArtifactRichness
    trade_count: int
    ohlcv_present: bool
    benchmark_present: bool
    has_assumptions: bool
    has_params: bool
    has_parameter_sweep: bool
    has_broker_export: bool = False
    has_declared_claims: bool = False
    asset_class_capabilities: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EngineAnalysisResult:
    envelope: EngineEnvelopeV1
    run_context: EngineRunContext
    capability_profile: AnalysisCapabilityProfile
    warnings: list[str]
    diagnostics: dict[DiagnosticName, dict[str, Any]]
    raw_payload: dict[str, Any]
    evidence_facts: list[dict[str, Any]] = field(default_factory=list)
    assumption_ledger: list[dict[str, Any]] = field(default_factory=list)
    claim_inventory: list[dict[str, Any]] = field(default_factory=list)
    proof_report: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyTerminalCard:
    """Stable SaaS-facing contract for Strategy Research Terminal cards."""

    schema_version: str
    card_type: str
    card_id: str
    name: str
    phase: str
    hypothesis_name: str
    pipeline_run_id: str | None
    created_at: str
    source_artifacts: dict[str, Any]
    data: dict[str, Any]
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class StrategyTerminalBundle:
    """Bundle contract consumed by future robustness and invariance desks."""

    schema_version: str
    card_schema_version: str
    created_at: str
    cards: list[StrategyTerminalCard]
