# Web Integration Capability Audit (`bulletproof_bt` → `invariance_research`)

## Scope
This audit reviews whether `bulletproof_bt` can serve as an analysis engine behind a separate web/backend shell that supplies **normalized parsed artifacts**.

## What already supports the target architecture

- A service-layer analysis path already exists in `bt.saas.service.StrategyRobustnessLabService` with deterministic diagnostics for overview, distribution, Monte Carlo, stability proxy, execution sensitivity, regime proxy, ruin, score, and report blocks. `build_dashboard_payload` returns these as structured sections. 
- Trade-level ingestion already exists and normalizes multiple column aliases into a common frame (`entry_ts`, `side`, `pnl_net`, `fees_paid`, etc.) with hard validation (`IngestionError`) and PnL inference when prices/size are present.
- Artifact-directory ingestion (`ingest_run_artifacts`) can consume `trades.csv` and optionally `equity.csv` + `performance.json`, providing a bridge from run artifacts to diagnostics.
- Existing analytics utilities (`bt.analytics.run_summary`, `bt.analytics.postmortem`) already compute post-run grouped diagnostics, indicating mature internal metric tooling for trade-level outputs.

## Partial support / mismatches

- Existing service ingestion is file-path oriented (`ingest_trade_log`, `ingest_run_artifacts`) and not centered on a normalized in-memory parsed artifact contract from an external backend.
- Existing `IngestedRun` typing used broad `Any` for tables; this was not a clear package seam for external callers.
- Stability and regime logic are **proxy-style** from trade sequence statistics; they do not currently require/consume full OHLCV context and therefore should be represented as limited when richer context is absent.
- Output sections are structured but were returned as a broad dictionary without capability declarations/eligibility surface.

## Gap closure implemented

- Added canonical engine-facing normalized input models:
  - `NormalizedTradeRecord`
  - `ParsedArtifactInput`
  - `AnalysisRunConfig`
- Added capability/output seam models:
  - `DiagnosticCapability`
  - `AnalysisCapabilityProfile`
  - `EngineRunContext`
  - `EngineAnalysisResult`
- Added central integration seam:
  - `StrategyRobustnessLabService.run_analysis_from_parsed_artifact(parsed_artifact, config?)`
  - convenience function `run_analysis_from_parsed_artifact(parsed_artifact, config?)`
- Added capability gating profile so diagnostics are marked `supported` / `limited` / `unavailable` with reasons rather than overpromising.
- Seam now honors `parsed_artifact.diagnostic_eligibility` to explicitly skip requested blocks while still returning a structured response contract.
- Added deterministic seam tests proving trade-only operation + honest degradation behavior.

## Remaining gaps before full package integration

- No native ingest of parser-produced OHLCV/benchmark series into deeper regime semantics yet (current regime block is trade-sequence proxy).
- Stability diagnostics do not yet include true parameter-grid topology unless external grid data is provided via existing grid-specific path (`parameter_stability_from_grid`).
- Product-facing scoring/verdict vocabulary is still engine-native and will still need backend adapter mapping in `invariance_research`.
