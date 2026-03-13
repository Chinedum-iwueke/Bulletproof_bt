# Engine Input Contract for Parsed Artifacts

## Entry point
Use one of:

- `bt.saas.service.run_analysis_from_parsed_artifact(parsed_artifact, config=None)`
- `StrategyRobustnessLabService.run_analysis_from_parsed_artifact(parsed_artifact, config=None)`

## Required normalized input model

`ParsedArtifactInput`:

- `artifact_kind`: `trade_csv | artifact_bundle`
- `richness`: `trade_only | trade_plus_metadata | trade_plus_context | research_complete`
- `trades`: `list[NormalizedTradeRecord]` (**required; must be non-empty**)
- `strategy_metadata`: free-form metadata map
- optional enrichments:
  - `equity_curve`
  - `assumptions`
  - `params`
  - `ohlcv_present`
  - `benchmark_present`
  - `parser_notes`
  - `diagnostic_eligibility`

`NormalizedTradeRecord` includes canonical trade fields such as symbol/side/entry/exit/price/size/fees/pnl/mae/mfe plus optional context tags.

## Behavior

- Engine normalizes records into internal trade frame via existing normalization pipeline.
- If `pnl` is absent, the engine infers net PnL when entry/exit/quantity/side are present.
- If `equity_curve` is absent or incomplete, equity is reconstructed from trade PnL.
- Metadata from parsed artifact is merged into run metadata.

## Structured output model

`EngineAnalysisResult` returns:

- `run_context`: artifact/richness and available context flags
- `capability_profile`: per-diagnostic status + reason + input requirements
- `warnings`: low sample and parser notes
- `diagnostics`: normalized blocks (`overview`, `distribution`, `monte_carlo`, `stability`, `execution`, `regimes`, `ruin`, `report`)
- `raw_payload`: underlying service payload for compatibility

## Degradation policy

- `diagnostic_eligibility` can disable specific diagnostics at call-time; disabled blocks are returned as `{"status": "skipped"}` and marked unavailable in capability profile.

- No trades: diagnostics marked unavailable.
- Low sample (<30 trades): trade-derived diagnostics marked limited.
- Missing params: stability marked limited (single-run proxy).
- Missing OHLCV: regimes marked limited (trade-sequence proxy).
