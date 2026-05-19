# Strategy Research Terminal

The Strategy Research Terminal is a read-only command and artifact layer over the
research daemon. It turns completed pipeline outputs into compact intelligence
cards that can be queried from the CLI today and consumed by Strategy Robustness
Lab or invariance research desks later.

It does not participate in backtest execution. The terminal reads existing
artifacts after the event-driven engine has finished, so the core guarantees
remain unchanged:

- no lookahead
- no interpolation
- missing bars remain missing
- execution tiers, fees, slippage, spread, delay, and intrabar assumptions stay
  explicit in the original run artifacts
- deterministic strategy behavior is not altered

## Card Contract

Every card uses schema `strategy_research_terminal.card.v1`. A pipeline emits:

- `HypothesisCard`
- `RunQualityCard`
- `RegimeDependencyCard`
- `ExecutionDragCard`
- `FailureCauseCard`
- `VerdictCard`
- `NextExperimentCard`

Cards are written under:

```text
outputs/<phase>/<hypothesis>_strategy_terminal_cards/
  cards.json
  cards.md
  HypothesisCard.json
  RunQualityCard.json
  RegimeDependencyCard.json
  ExecutionDragCard.json
  FailureCauseCard.json
  VerdictCard.json
  NextExperimentCard.json
```

When a research DB is configured, those files are registered in the `artifacts`
table as `strategy_terminal_*` artifacts. The bundle schema is
`strategy_research_terminal.bundle.v1`.

## Inputs

Cards consume existing research outputs only:

- `summaries/run_summary.csv`
- `research_data/runs_dataset.parquet`
- `research_data/trades_dataset.parquet`
- rich structural bucket CSVs where present
- state discovery outputs
- verdict bundle manifests
- pipeline command logs
- hypothesis YAML

Richer derivatives state is represented when present in extracted trades, for
example funding, open-interest acceleration, basis, premium, crowding, constraint
stress, and enriched CSI fields. If those fields are absent, the cards degrade to
OHLCV-era summaries and include warnings instead of inventing data.

## CLI

Use the read-only terminal CLI:

```bash
PYTHONPATH=src:. python orchestrator/research_terminal_cli.py research-status \
  --db research_db/research.sqlite

PYTHONPATH=src:. python orchestrator/research_terminal_cli.py queue-status \
  --db research_db/research.sqlite

PYTHONPATH=src:. python orchestrator/research_terminal_cli.py latest-verdict \
  --db research_db/research.sqlite \
  --hypothesis l1_h7c_high_selectivity_regime

PYTHONPATH=src:. python orchestrator/research_terminal_cli.py compare \
  --db research_db/research.sqlite \
  --hypothesis l1_h7c_high_selectivity_regime \
  --phase tier2

PYTHONPATH=src:. python orchestrator/research_terminal_cli.py explain-failure \
  --db research_db/research.sqlite \
  --hypothesis l1_h7c_high_selectivity_regime

PYTHONPATH=src:. python orchestrator/research_terminal_cli.py state-summary \
  --cards-json outputs/tier2/l1_h7c_high_selectivity_regime_strategy_terminal_cards/cards.json

PYTHONPATH=src:. python orchestrator/research_terminal_cli.py similar-runs \
  --db research_db/research.sqlite \
  --card-type RegimeDependencyCard

PYTHONPATH=src:. python orchestrator/research_terminal_cli.py similar-runs \
  --memory-db research_memory/research_memory.sqlite \
  --state-json /tmp/current_state.json

PYTHONPATH=src:. python orchestrator/research_terminal_cli.py recommendations \
  --db research_db/research.sqlite
```

## SaaS Seam

`StrategyRobustnessLabService.load_strategy_terminal_bundle()` loads
`cards.json` into a stable dataclass contract:

- `StrategyTerminalBundle`
- `StrategyTerminalCard`

This gives future SaaS surfaces the same post-run evidence packet used by the
daemon without coupling the UI to internal pipeline paths.

## Failure Handling

Successful pipelines emit the full card bundle after the verdict bundle is
created. Failed pipelines still emit a `FailureCauseCard` with:

- pipeline status
- error message
- root-cause hint
- pipeline log tail
- command log paths

Card generation is best-effort. A card generation issue is logged as a terminal
warning and does not change the verdict of an otherwise valid backtest pipeline.
