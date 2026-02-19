# Beginner vs Pro Contract

## Stable contract (client-facing)

### What “safe” vs “strict” means
- **safe** (`risk.stop_resolution: safe`): unresolved stops may fall back only if `risk.allow_legacy_proxy: true`.
- **strict** (`risk.stop_resolution: strict`): unresolved stop contract is rejected; legacy proxy fallback is disallowed.

Repo Evidence: `src/bt/core/config_resolver.py::resolve_config`, `src/bt/risk/reject_codes.py`, `tests/test_stop_resolution_enforcement_modes.py`.

### Execution profile posture by audience
- Beginner default: preset `tier2` (no execution field overrides)
- Pro/research: choose `tier1|tier2|tier3` presets or explicit `custom`

Repo Evidence: `src/bt/execution/profile.py::_BUILTIN_PROFILES`, `src/bt/execution/profile.py::resolve_execution_profile`.

### Reference config packs
- `configs/examples/safe_client.yaml`
- `configs/examples/strict_research.yaml`

Repo Evidence: `configs/examples/safe_client.yaml`, `configs/examples/strict_research.yaml`, `tests/test_config_packs_stop_contract.py`.

## Safe → strict migration checklist
1. Move to `risk.stop_resolution: strict`.
2. Set `risk.allow_legacy_proxy: false`.
3. Ensure strategy emits resolvable stop inputs (explicit stop price or stop spec).
4. Run and inspect `run_status.json.stop_resolution` and decision reasons for rejects.
5. Keep execution profile preset unless you intentionally need `custom` values.

Repo Evidence: `src/bt/core/config_resolver.py`, `src/bt/experiments/grid_runner.py::_write_run_status`, `tests/test_stop_unresolvable_rejected.py`.

## FAQ / common failure modes
- **Strict config rejected because `allow_legacy_proxy=true`**  
  That combination is invalid by contract; set it to `false` for strict mode.

- **Strategy works in safe mode but fails in strict mode**  
  Safe mode can allow fallback; strict mode requires explicit stop-contract compliance.

- **Can I stay safe mode but use tier3 execution?**  
  Yes; risk stop mode and execution profile are independent controls.

Repo Evidence: `src/bt/core/config_resolver.py`, `src/bt/execution/profile.py`, `tests/test_config_packs_stop_contract.py`.
