# Error and Run Status Contract

## Stable contract (client-facing)

### Error taxonomy used by system
Domain error classes:
- `ConfigError`
- `DataError`
- `StrategyContractError`
- `RiskError`
- `ExecutionError`
- `PortfolioError`

Additionally, invalid config/data paths may raise `ValueError` where adapters normalize external input.

Repo Evidence: `src/bt/core/errors.py`, `src/bt/data/load_feed.py`, `src/bt/execution/profile.py`.

### Where errors are surfaced
- **CLI/API**: exception is raised to caller.
- **`run_status.json`**: `status=FAIL`, `error_type`, `error_message`, `traceback`.
- **`summary.txt`**: reads run artifacts and surfaces run outcome context when present.
- **sanity counters**: written in `sanity.json` (`decisions`, `fills`, `closed_trades`, `forced_liquidations`, `risk_rejections`).

Repo Evidence: `src/bt/api.py::run_backtest`, `src/bt/experiments/grid_runner.py::_write_run_status`, `src/bt/logging/summary.py::write_summary_txt`, `src/bt/logging/sanity.py::write_sanity_json`.

### run_status.json schema (v1)
Current writer guarantees these core fields per run:
- `schema_version` (currently `1`)
- `status` (`PASS|FAIL`)
- `error_type`
- `error_message`
- `traceback`
- `run_id`
- execution snapshot fields (when resolvable): `execution_profile`, `effective_execution`, `spread_mode`, `intrabar_mode`, optional `spread_bps`
- stop contract/reporting fields: `stop_resolution`, `used_legacy_stop_proxy`, `r_metrics_valid`, `notes`, `stop_resolution_counts`, optional `stop_contract`

Repo Evidence: `src/bt/contracts/schema_versions.py::RUN_STATUS_SCHEMA_VERSION`, `src/bt/experiments/grid_runner.py::_write_run_status`, `src/bt/execution/effective.py::build_effective_execution_snapshot`.

### Stable reason/decision codes
Canonical constants include:
- `risk_rejected:insufficient_margin`
- `risk_rejected:max_positions`
- `risk_rejected:notional_cap`
- `risk_rejected:stop_unresolvable`
- `risk_rejected:min_stop_distance`
- `liquidation:end_of_run`
- `liquidation:negative_free_margin`

Risk-module reject code constants also include stable suffix variants such as:
- `risk_rejected:stop_unresolvable:strict`
- `risk_rejected:stop_unresolvable:safe_no_proxy`

Repo Evidence: `src/bt/core/reason_codes.py`, `src/bt/risk/reject_codes.py`, `tests/test_risk_reject_codes_stability.py`.

## FAQ / common failure modes
- **FAIL status but run directory exists**  
  This is expected; diagnostics are persisted to `run_status.json` and `sanity.json` even on failure.

- **`error_type` is `ValueError` not a custom class**  
  Some adapters/validators raise `ValueError` directly; treat these as contract-level config/data issues and use `error_message` details.

- **Can I key automation on reason-code strings?**  
  Yes for documented constants above; prefer constants validated by stability tests.

Repo Evidence: `src/bt/api.py`, `src/bt/experiments/grid_runner.py`, `tests/test_error_taxonomy_and_schema_version.py`, `tests/test_risk_reject_codes_stability.py`.

## Repo Evidence index
- `src/bt/core/errors.py`
- `src/bt/experiments/grid_runner.py::_write_run_status`
- `src/bt/contracts/schema_versions.py`
- `src/bt/core/reason_codes.py`
- `src/bt/risk/reject_codes.py`
- `src/bt/logging/sanity.py`
