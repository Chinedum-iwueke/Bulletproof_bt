# Execution Model Contract

## Stable contract (client-facing)

### Order support matrix (v1)
| Order type | Support |
| --- | --- |
| `MARKET` | ✅ supported |
| `LIMIT` | ❌ defined in enums, but execution rejects non-market orders in v1 |

Repo Evidence: `src/bt/core/enums.py::OrderType`, `src/bt/execution/execution_model.py::ExecutionModel.process`.

### Fill timing semantics
- `delay_bars` means an order waits that many bars before it becomes fill-eligible.
- Delay countdown is tracked in order metadata (`delay_remaining`).

Repo Evidence: `src/bt/execution/execution_model.py::ExecutionModel.process`.

### Intrabar mode options
- `worst_case`:
  - BUY fills at bar high
  - SELL fills at bar low
- `best_case`:
  - BUY fills at bar low
  - SELL fills at bar high
- `midpoint`: fills at `(high + low)/2`

Default intrabar mode is `worst_case` when unset.

Repo Evidence: `src/bt/execution/intrabar.py::parse_intrabar_spec`, `src/bt/execution/intrabar.py::market_fill_price`.

### Cost model pipeline (execution order)
1. Intrabar raw market fill price
2. Spread application (`apply_spread`)
3. Slippage application (`estimate_slippage`)
4. Fee application on final notional (`fee_for_notional`)

Repo Evidence: `src/bt/execution/execution_model.py::ExecutionModel.process`, `src/bt/execution/spread.py::apply_spread`, `src/bt/execution/slippage.py::SlippageModel.estimate_slippage`, `src/bt/execution/fees.py::FeeModel.fee_for_notional`.

## Execution tiers and override policy

### Tier reference table (exact preset values)
| Profile name | Intended use case / realism | maker_fee | taker_fee | slippage_bps | spread_bps | delay_bars | intrabar default influenced by tier? | Override policy |
| --- | --- | ---:| ---:| ---:| ---:| ---:| --- | --- |
| tier1 | optimistic / low-friction | 0.0 | 0.0004 | 0.5 | 0.0 | 0 | No (intrabar remains independent, default worst_case) | Preset fields locked |
| tier2 (default) | baseline realistic default | 0.0 | 0.0006 | 2.0 | 1.0 | 1 | No | Preset fields locked |
| tier3 | conservative / higher friction | 0.0 | 0.0008 | 5.0 | 3.0 | 1 | No | Preset fields locked |
| custom | explicit user assumptions | required explicit | required explicit | required explicit | required explicit | required explicit | No | Only profile that allows these fields |

Repo Evidence: `src/bt/execution/profile.py::_BUILTIN_PROFILES`, `src/bt/execution/profile.py::resolve_execution_profile`, `tests/test_execution_profile_resolution.py::test_builtin_profile_values_exact`.

### Preset override rule
- `tier1|tier2|tier3` reject explicit overrides for:
  - `maker_fee`, `taker_fee`, `slippage_bps`, `delay_bars`, `spread_bps`
- `custom` requires **all** those fields.

Typical client error:
- `execution.profile=tier2 forbids overrides. Remove override keys or set execution.profile=custom...`

Repo Evidence: `src/bt/execution/profile.py::_PROFILE_OVERRIDE_FIELDS`, `src/bt/execution/profile.py::resolve_execution_profile`, `tests/test_execution_profile_override_policy.py`.

### “What’s in my tier?” verification
- Check `run_status.json`:
  - `execution_profile`
  - `effective_execution` snapshot (`maker_fee`, `taker_fee`, `slippage_bps`, `delay_bars`, `spread_bps`)
- Check `config_used.yaml` for resolved config inputs.

Repo Evidence: `src/bt/execution/effective.py::build_effective_execution_snapshot`, `src/bt/api.py::run_backtest`, `src/bt/experiments/grid_runner.py` status payload creation.

## Copy/paste config examples

### Preset tier2 (default spread disabled)
```yaml
execution:
  profile: tier2
  spread_mode: none
  intrabar_mode: worst_case
```

### Preset tier with fixed spread enabled
```yaml
execution:
  profile: tier3
  spread_mode: fixed_bps
  # spread_bps optional; defaults to tier spread_bps (tier3 => 3.0)
```

### Custom execution assumptions
```yaml
execution:
  profile: custom
  maker_fee: 0.0
  taker_fee: 0.0010
  slippage_bps: 3.0
  spread_bps: 1.5
  delay_bars: 1
  spread_mode: fixed_bps
  intrabar_mode: midpoint
```

## FAQ / common failure modes
- **Why am I getting “tier forbids overrides”?**  
  Because preset tiers lock execution fields. Move to `execution.profile: custom` if you need explicit values.

- **Why does `spread_bps` still error even if `spread_mode: none`?**  
  The profile resolver checks override fields independent of spread mode for preset tiers; preset tier + explicit `spread_bps` is still forbidden.

- **What happens if I set `spread_mode: fixed_bps` on a tier preset without `spread_bps`?**  
  Resolver uses the tier preset value (`tier1=0.0`, `tier2=1.0`, `tier3=3.0`).

- **How do I intentionally override?**  
  Set `execution.profile: custom` and provide all required override fields.

Repo Evidence: `src/bt/execution/profile.py::resolve_execution_profile`, `src/bt/core/config_resolver.py::resolve_config`.

## Repo Evidence index
- `src/bt/execution/profile.py::resolve_execution_profile`
- `src/bt/execution/profile.py::_BUILTIN_PROFILES`
- `src/bt/execution/execution_model.py::ExecutionModel.process`
- `src/bt/execution/intrabar.py`
- `src/bt/execution/spread.py`
- `src/bt/execution/slippage.py`
- `src/bt/execution/fees.py`
- `src/bt/execution/effective.py::build_effective_execution_snapshot`
