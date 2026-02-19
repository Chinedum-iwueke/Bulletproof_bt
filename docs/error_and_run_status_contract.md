# Error and Run Status Contract

## What this contract covers
This contract defines PASS/FAIL run status behavior and error reporting fields for client consumption.

Implementation: src/bt/api.py, src/bt/experiments/grid_runner.py

## V1 support
- `run_status.json` is written for both successful and failed runs.
- Core fields include:
  - `status` (`PASS` or `FAIL`)
  - `error_type`
  - `error_message`
  - `traceback`
  - `run_id`
- Additional execution and stop-resolution metadata is included when available.

Implementation: src/bt/api.py, src/bt/experiments/grid_runner.py, src/bt/execution/effective.py

## Inputs and guarantees
- On success, `status=PASS` and error fields are empty strings.
- On failure, `status=FAIL` and error fields are populated from the exception.
- Stop-resolution summary keys are present with deterministic structure.

Implementation: src/bt/api.py, src/bt/experiments/grid_runner.py

## Rejections and failure modes
- Any runtime exception bubbles after run status is written.
- If execution snapshot cannot be derived on failure, snapshot fields may be partial.

Implementation: src/bt/api.py

## Artifacts and where to look
- Primary: `run_status.json`.
- Related diagnostics: `sanity.json`, `decisions.jsonl`, `fills.jsonl`.

Implementation: src/bt/api.py, src/bt/logging/sanity.py, src/bt/logging/jsonl.py

## Examples
PASS shape (abbreviated):

```yaml
status: PASS
error_type: ""
error_message: ""
traceback: ""
```

FAIL shape (abbreviated):

```yaml
status: FAIL
error_type: ValueError
error_message: "..."
traceback: "Traceback (most recent call last): ..."
```

## Versioning
- Contract version: v1.
- Run-status fields have expanded additively in tests.
- Schema versioning: not yet exposed as `run_status.schema_version`; treat docs/tests as source of truth.

Observation points: tests/test_run_status_stop_resolution.py, tests/test_run_status_execution_metadata.py
