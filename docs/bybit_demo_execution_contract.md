# Bybit Demo Execution Contract (Phase 5)

## Scope
- Enables `paper_broker` execution mode against Bybit **demo** environment only.
- Activates adapter mutation methods (`submit_order`, `cancel_order`, `amend_order`) when `broker.environment=demo`.
- Keeps live/mainnet mutation blocked for this phase.

## Ack vs final-state
- REST create/amend/cancel responses are treated as **acceptance acks**, not final lifecycle truth.
- Order terminal states and fill confirmations must come from private stream `order`/`execution` events and reconciliation.

## Source of truth
- Real-time lifecycle uses Bybit private topics:
  - `order`
  - `execution`
  - `position`
  - `wallet`
- Runtime consumes canonical broker events and applies fills only from confirmed execution events.

## Restart / recovery semantics
- Existing Phase 2/3 durable state and reconciliation cadence remain active.
- `paper_broker` mode is guarded to demo-only, with conservative startup behavior left to reconciliation policy.
- Exactly-once across all failure modes is not claimed; tested dedupe prevents duplicate fill application for repeated broker events.

## Remaining limitations before live phase
- Mainnet/live mutations remain intentionally disabled.
- Startup reconciliation gating knobs are config-visible; strict block-until-reconciled orchestration remains incremental.
- External ops alerting/kill-switch integrations are still out of scope for Phase 5.
