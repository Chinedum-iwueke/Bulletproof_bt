# Product Strategy Validation Platform Design

Generated from office-hours review on 2026-05-15.

## Executive Summary

This document corrects the earlier greenfield assumption. There is already a real web app repo:

- `invariance_research`: the existing Next.js + TypeScript SaaS application.
- `bulletproof_bt`: the existing Python strategy research, diagnostics, backtesting, artifact, and orchestration engine.

The product should not be rebuilt from scratch. The existing Strategy Robustness Lab in `invariance_research` should be treated as the incomplete product surface, and `bulletproof_bt` should be treated as the engine and research substrate that powers it.

The first commercial wedge remains narrow:

> Upload strategy evidence. Get a hostile, auditable validation report.

The revised implementation thesis is:

> Make the current Strategy Robustness Lab real, trustworthy, and saleable before expanding into the full claim-first research operating system.

Approach A should be first:

> Harden and deepen the existing upload-to-diagnostics Lab using the current `invariance_research` product shell and the current `bulletproof_bt` engine seam.

The full ambition remains larger:

> A claim-first strategy validation and research intelligence platform that remembers every hypothesis, every failure, every regime dependency, and every unsupported conclusion.

The sequencing matters. The platform should grow from the existing app, not around it.

## Office-Hours Position

The product should be judged by demand, not architecture.

The sharpest early customer is not "anyone who backtests." The sharpest early customer is someone who needs credible strategy evidence:

- a serious independent trader deciding whether to keep allocating time or capital
- a strategy seller or educator who needs a hostile third-party report
- a systematic trader who needs artifact-backed diagnostics
- an emerging manager or prop-style operator screening a strategy
- a crypto-native researcher whose edge depends on execution and regime context

The status quo is not just another dashboard. The status quo is:

- hand-curated spreadsheets
- screenshots of equity curves
- broker exports with missing assumptions
- loosely written strategy claims
- private notebooks that cannot be audited later
- manual PDF writeups that overstate confidence
- credibility rituals in private communities instead of reproducible validation

The immediate product should not ask users to adopt a whole research OS. It should ask them for an artifact they already have and return a report they would actually share.

## Current Repo Reality

### `invariance_research` Exists And Is The Product App

Observed from repo docs and code:

- public authority site exists
- `/robustness-lab` marketing surface exists
- authenticated `/app` workspace exists
- `/app/new-analysis` upload flow exists
- `/api/uploads/inspect` exists
- `/api/analyses` exists
- queue-backed analysis jobs exist
- export jobs exist
- JSON, Markdown, and PDF export paths exist
- plan matrix exists: `explorer`, `professional`, `research_lab`, `advisory`
- Stripe checkout, portal, and webhook processing exist
- admin/ops console exists
- health checks exist
- persistence abstraction exists with SQLite now and Postgres-ready modules present
- local object storage abstraction exists
- benchmark manifest/data plumbing exists
- Python bridge exists through `scripts/run_bulletproof_engine.py`
- current engine seam calls `bt.run_analysis_from_parsed_artifact`

Key files in the web app:

- `src/app/robustness-lab/page.tsx`
- `src/app/app/new-analysis/page.tsx`
- `src/components/forms/new-analysis-intake.tsx`
- `src/app/api/uploads/inspect/route.ts`
- `src/app/api/analyses/route.ts`
- `src/lib/server/services/upload-intake-service.ts`
- `src/lib/server/services/analysis-service.ts`
- `src/lib/server/engine/bulletproof-runner.ts`
- `src/lib/server/adapters/bulletproof/map-engine-analysis-record.ts`
- `src/lib/contracts/analysis.ts`
- `src/lib/server/entitlements/plans.ts`
- `src/lib/server/persistence/postgres-schema.ts`
- `src/lib/server/workers/analysis-worker.ts`
- `src/lib/server/workers/export-worker.ts`

Implication:

The product is not pre-product. It is an incomplete product with real surfaces, contracts, workflows, billing, and ops. The next plan should be a hardening and expansion plan, not a first-build plan.

### `bulletproof_bt` Is The Engine And Research Substrate

Observed:

- deterministic event-driven backtest core
- public `run_backtest` and `run_grid` APIs
- `bt.run_analysis_from_parsed_artifact` seam for the web app
- SaaS service under `src/bt/saas/service.py`
- typed SaaS models under `src/bt/saas/models.py`
- artifact discipline with trades, manifests, benchmark artifacts, performance, robustness, and R metrics
- execution modeling with fees, slippage, spreads, profiles, intrabar assumptions, and cost attribution
- hypothesis contract system under `src/bt/hypotheses`
- research data layer for OHLCV, mark/index, funding, open interest, liquidations, and panels
- orchestration layer with SQLite research database, daemon, queues, experiment pipelines, verdict artifacts, and research memory
- research memory modules for trades, state buckets, candidates, recommendations, and reports
- internal FastAPI/Jinja dashboard that is useful for local research but should not become the public web app

Key files:

- `src/bt/saas/service.py`
- `src/bt/saas/models.py`
- `src/bt/api.py`
- `src/bt/core/engine.py`
- `src/bt/execution/*`
- `src/bt/metrics/*`
- `src/bt/benchmarks/*`
- `src/bt/hypotheses/*`
- `src/bt/research_data/*`
- `orchestrator/research_memory/*`
- `orchestrator/research_daemon.py`

Implication:

The Python repo should not become a web app. It should become the increasingly stable engine, artifact, validation, and research-memory substrate called by `invariance_research`.

## Product Thesis

Most trading validation tools help users produce a more attractive result. This product should help users find the point where their strategy stops deserving belief.

The brand should be comfortable saying:

> We cannot support that conclusion from the data provided.

That sentence is not a failure state. It is the product.

The user promise:

> Find out whether your trading edge is real before the market does.

The wedge:

> Upload strategy evidence. Get a hostile validation report.

The long-term ambition:

> A strategy research operating system where every market claim becomes a falsifiable hypothesis, every run becomes auditable evidence, and every failure improves future research.

## Demand Hypothesis

This plan is still a hypothesis until real users pay, share reports, or build workflow around it.

Strong demand evidence would look like:

- a trader pays for a report even when the verdict is negative
- a strategy seller shares the report with buyers
- an allocator asks for reports on multiple strategies
- a user uploads a better artifact to unlock a missing diagnostic
- a user asks for an advisory follow-up after a limitation is surfaced
- a team wants org-level retention and audit history
- a user returns after a rejected strategy with a revised hypothesis

Weak evidence:

- users say the product is interesting
- users like the public site
- users upload one toy CSV and leave
- users only want the product to confirm their existing belief
- users ask for live trading before they trust validation

The first release should be instrumented to learn which of these is true.

## Premises

1. `invariance_research` is the product app and should remain the primary user-facing repo.
2. `bulletproof_bt` is the engine and research substrate and should expose stable seams to the web app.
3. The existing Strategy Robustness Lab is real but incomplete.
4. The fastest credible path is to harden existing upload, analysis, diagnostics, report, entitlement, and ops flows.
5. Claim-first research OS features should be progressively introduced after users trust artifact-backed validation.
6. Research memory is the eventual moat, but tenant safety and auditability must come before cross-user intelligence.
7. Product honesty matters more than feature breadth.

## Strategic Approaches Considered

### Approach A: Harden The Existing Strategy Robustness Lab First

Summary:

Use the existing `invariance_research` app as the product surface. Deepen the upload-to-analysis-to-report path, tighten the Node/Python seam, improve diagnostic truthfulness, and convert the current Lab into a saleable validation product.

Effort: Medium

Risk: Medium

Reuses:

- `invariance_research` public site and `/robustness-lab`
- authenticated `/app` workspace
- upload inspection and eligibility system
- analysis queue and worker model
- `bt.run_analysis_from_parsed_artifact` bridge
- `map-engine-analysis-record.ts`
- report and export pipeline
- entitlements, billing, and admin
- `bulletproof_bt` SaaS service and artifact diagnostics

Pros:

- ships fastest because the app already exists
- reduces product risk before expanding architecture
- lets real users validate the wedge
- preserves existing billing, admin, auth, upload, and worker work
- creates a clean path for advisory/report revenue
- keeps the product concrete: upload evidence, get verdicts

Cons:

- current architecture carries transitional SQLite/local-storage assumptions
- current report/PDF output is not yet premium enough for high-trust sale
- current diagnostics depend heavily on artifact richness
- claim-first hypothesis flows are deferred
- research memory remains mostly internal for now

Recommendation:

Choose Approach A first. It is the best path from current repo reality to real product evidence.

### Approach B: Claim-First Research OS

Summary:

Make the core product object a market claim or hypothesis rather than an uploaded artifact. Users define a falsifiable claim, attach evidence, run experiments, and track lineage.

Effort: Large

Risk: High

Reuses:

- `bulletproof_bt` hypothesis contracts
- orchestrator experiment pipeline
- research memory schema
- research data panels
- existing diagnostic/report pages after adaptation

Pros:

- strongest long-term differentiation
- aligns with the deepest research culture in `bulletproof_bt`
- creates a durable data model for lineage, failures, and follow-on research
- better supports systematic traders and research teams

Cons:

- too abstract as the first user experience
- requires new product objects in the web app: claims, hypotheses, experiments, validations, forks
- needs careful UX to avoid feeling like homework
- pushes demand validation farther away
- risks building a system before knowing which report customers will pay for

Recommendation:

Do not lead with this. Build toward it through artifact-backed validation and add claim capture when a user already has a reason to care.

### Approach C: Advisory / Research Desk First

Summary:

Use the app as intake for paid human or expert-assisted validation. The internal team reviews artifacts, runs deeper engine workflows, annotates findings, and delivers polished reports.

Effort: Medium

Risk: Medium

Reuses:

- existing contact and research-desk funnel
- admin console
- report/export pipeline
- failed-job and rerun controls
- `bulletproof_bt` orchestrator and internal dashboard concepts

Pros:

- can generate revenue before full automation
- reveals what serious users actually ask for
- creates high-quality report templates and review rubrics
- gives a path for incomplete diagnostics to become consultative upsell

Cons:

- does not scale without operations discipline
- can distract from product automation
- requires clear boundaries around financial advice
- report quality must be high immediately

Recommendation:

Use this as a companion to Approach A, not a replacement. The Lab should produce automated first-pass validation, then route serious cases to Research Desk.

## Recommended Path

Choose Approach A first.

The product should advance in this order:

1. Make the existing Lab trustworthy.
2. Make the first report worth sharing.
3. Make the data eligibility model impossible to misunderstand.
4. Add Research Desk handoff for serious users.
5. Add claim capture inside the analysis flow.
6. Add tenant-scoped research memory.
7. Expand into the full claim-first Research OS.

## CEO Review Scope Additions

Generated by `/plan-ceo-review` in selective-expansion mode on 2026-05-15.

Approach A remains the baseline. The following additions are now in scope because they strengthen the Lab-first wedge without turning the first implementation into the full research OS.

### 1. Evidence Sufficiency Ledger

The product needs a first-class Evidence Sufficiency Ledger. Diagnostic availability alone is not enough; the report should track what claims the evidence can and cannot support.

The ledger should store, for each user-facing conclusion or implied claim:

- claim ID
- plain-language claim
- evidence status: `supported`, `limited`, `unsupported`, or `contradicted`
- required evidence
- evidence actually received
- source diagnostic or source report section
- missing fields or missing artifacts
- limitation text suitable for UI/report use
- deterministic reason code

It should appear in:

- upload inspection: what this artifact can support
- overview: strongest supported and unsupported conclusions
- report appendix: full evidence map
- share room: report-safe evidence state without exposing raw uploads

The ledger is the product's trust spine. It makes the statement "we cannot support that conclusion from the data provided" operational instead of rhetorical.

### 2. Single Source Of Evidence Truth

The Evidence Sufficiency Ledger must not be recomputed separately by upload UI, overview UI, report rendering, share rooms, and LLM insight generation.

Implementation rule:

```text
artifact eligibility
  + engine capability profile
  + diagnostic outputs
  + report/share policy
  -> Evidence Sufficiency Ledger service
  -> persisted ledger snapshot
  -> entitlement projection overlays locked/unlocked state
  -> UI/report/share/LLM projections
```

The ledger is canonical for evidence status. Entitlements are deliberately projected after ledger derivation so the product never confuses "the evidence does not support this conclusion" with "your plan does not include this diagnostic."

Requirements:

- one shared product contract for evidence status
- one derivation service for the ledger
- implement ledger derivation outside the engine adapter mapper, for example in `src/lib/server/evidence/evidence-ledger-service.ts`
- keep `map-engine-analysis-record.ts` as a mapper that consumes ledger output/projections rather than deriving evidence policy
- deterministic ledger snapshots saved with the analysis/report
- upload inspection, diagnostic access, overview, reports, share rooms, and LLM inputs consume ledger projections rather than recomputing rules
- plan locks are overlays on top of ledger status, not alternate evidence states
- exports and share rooms use the same saved ledger snapshot
- LLM-generated insights may consume the ledger but cannot override it
- ledger derivation must be fixture-tested and snapshot-tested

### 3. Cross-Repo Rollout Contract

Approach A spans two repos. The plan must explicitly define release order and compatibility rules.

Required rollout sequence:

1. Add or update shared engine seam fixtures.
2. Update `bulletproof_bt` seam behavior and payload version.
3. Verify `bulletproof_bt` emits the fixture shape.
4. Update `invariance_research` adapter tests to consume the fixture shape.
5. Update web adapter mappings.
6. Update UI/report/share projections.
7. Release behind feature flags.

Compatibility rules:

- engine payload changes are additive unless the seam version is bumped
- unknown diagnostics fail closed into `unsupported`, not omitted
- fixture tests block merge when adapter behavior drifts
- every engine result carries a versioned `EngineEnvelopeV1`
- `EngineEnvelopeV1` includes `engine_name`, `engine_version`, `seam_name`, `seam_version`, `adapter_version`, `parser_version`, `capability_profile_version`, and `diagnostic_contract_version`
- `bulletproof_bt` owns the emitted engine/seam fields; `invariance_research` owns adapter/parser/report projection fields
- one owner is assigned for the compatibility matrix

The failure mode this prevents:

```text
bulletproof_bt changes EngineAnalysisResult
  -> invariance_research adapter expects old shape
  -> worker persists degraded or mis-mapped diagnostics
  -> report displays an evidence state that should have failed closed
```

### 4. Failure And Rescue Matrix

Every failure class needs a user-visible rescue path and an admin-visible debug path.

| Failure class | User sees | Admin sees | Rescue |
|---|---|---|---|
| Parser failure | File rejected with exact issue and accepted template link | Parser version, file envelope metadata, validation issue codes | Download/fill matching validation packet template |
| Eligibility limited | Available/limited/unavailable matrix plus missing evidence | Diagnostic evidence state and missing fields | Upload richer artifact or continue with limitations |
| Queue failure | Delayed or retrying status with product-safe explanation | Job lease, attempts, worker heartbeat, backoff state | Automatic retry, admin retry, or support handoff |
| Engine failure | Product-safe failure reason and retry/support option | Bridge exit code, timeout class, stderr pointer, engine version | Retry if safe, otherwise Research Desk/support handoff |
| Adapter failure | Report blocked because engine result could not be trusted | Schema mismatch details and fixture failure | Fail closed, add fixture, update adapter mapping |
| Export failure | Export retryable/failed status | Export job trace and renderer error class | Retry/regenerate export |
| Share-room failure | Report unavailable, expired, revoked, or superseded | Access audit event and share state | Regenerate share link or request access |
| Entitlement failure | Locked state clearly separated from unsupported evidence | Plan policy reason and diagnostic entitlement | Upgrade path or richer artifact guidance |

No failure should silently degrade into a confident report.

### 5. Report Share Room

The report should become a measurable demand object, not only a downloaded PDF.

Scope:

- shareable report view backed by `report_snapshot_id`
- dedicated `/share/[token]` route/API, not reused owner analysis detail routes
- `SharedReportViewModel` projection with allowlisted fields only
- raw uploads hidden by default and no artifact download route in share context
- expiration and revocation controls
- visible report version and generated-at timestamp
- stale/superseded warning when a newer report snapshot exists
- evidence ledger summary
- limitation appendix
- report engagement events that do not log report content
- Research Desk request CTA
- redaction tests for filenames, file paths, user IDs, account IDs, internal job IDs, bridge logs, raw upload metadata, and admin-only addenda

Not in scope:

- comments
- collaboration
- public profiles
- marketplace features
- raw artifact sharing by default

### 6. Report Sharing Threat Model

A shareable report creates a new trust boundary. It needs privacy rules from the first implementation.

Requirements:

- raw uploads are never exposed through shared reports by default
- share links are scoped, expiring, revocable, and audit-logged
- report views use least-privilege `SharedReportViewModel` payloads separate from owner analysis detail payloads
- share routes never call owner analysis detail APIs or artifact download APIs
- report appendix redacts sensitive file paths, raw filenames where needed, user IDs, account IDs, internal job IDs, and bridge logs
- share-room access events are tracked without logging report content
- admin/research-desk addenda are marked as human-reviewed and versioned
- expired, revoked, or superseded share links fail closed or show an explicit stale warning, never a silent stale report

### 7. Analysis, Report, And Share State Machine

The plan needs explicit states so retries, regeneration, and sharing cannot produce stale trust artifacts.

Analysis states:

```text
uploaded -> queued -> processing -> completed
       \-> failed
completed -> queued      # retry/rerun creates new processing attempt
completed -> superseded  # newer accepted result replaces it as current
```

Report states:

```text
not_generated -> queued -> rendering -> ready
                            \-> failed
ready -> expired
ready -> superseded
ready -> queued            # regenerate creates a new report snapshot
```

Share states:

```text
inactive -> active -> expired
                 \-> revoked
                 \-> superseded
```

Rules:

- all analysis/report/share state mutations go through explicit transition guards, not ad hoc repository updates
- implement either one scoped `state-transitions.ts` module or three small modules: `analysis-state-machine.ts`, `report-state-machine.ts`, and `share-state-machine.ts`
- transition guards define legal from/to states, required side effects, idempotency keys, and stale/superseded behavior
- retrying or rerunning an analysis after a report exists must either supersede the old report or leave it visible with a clear stale warning
- regenerating a report creates a new immutable report snapshot
- share rooms point to report snapshots, not mutable analysis objects
- overview/report pages must define how they select the current snapshot
- double-clicks on export/share must be idempotent or visibly deduplicated
- browser refresh/back-button flows must not create duplicate jobs or stale share links
- transition guards are fixture-tested for valid transitions, invalid transitions, duplicate requests, retry/rerun, report regeneration, and share revocation

### 8. Validation Packet Templates

The Lab should provide validation packet templates that make artifact sufficiency concrete.

Templates:

- trade CSV template
- structured bundle template
- research bundle template
- intentionally incomplete example that demonstrates unsupported diagnostics
- example that unlocks parameter stability
- example that unlocks benchmark/regime context

Each template should include:

- expected files and columns
- diagnostics it can support
- diagnostics it cannot support
- common mistakes
- sample accepted upload
- matching parser/validator fixture

This turns "your artifact is insufficient" into "here is exactly how to make it sufficient."

### 9. Wedge Acceptance Test Matrix

The first implementation should pass end-to-end tests that prove the wedge, not just units that prove functions.

| Scenario | Expected result |
|---|---|
| Trade CSV only | Overview, distribution, and Monte Carlo available; execution limited; stability/regime unavailable |
| Structured bundle full | Full diagnostics available, report ready, share room can be created |
| Incomplete bundle | Ledger marks unsupported claims and points to missing files |
| Plan-locked diagnostic | Locked is distinct from unavailable |
| Engine skipped diagnostic | Ledger preserves skipped reason and report cannot overclaim |
| Adapter unknown diagnostic | Fail closed; fixture test fails until mapped |
| Report regeneration after retry | Old share is superseded or visibly stale |
| Expired share link | Fails closed with no report payload |
| Raw upload privacy | Shared report cannot access raw artifact URL |
| Validation template upload | Each template unlocks the documented diagnostics |
| LLM insight contradiction | Deterministic ledger wins over generated prose |

Cross-repo fixture requirement:

- `bulletproof_bt` emits canonical fixture payloads
- `invariance_research` maps those same fixtures into product contracts

### 10. Wedge Learning Event Model

Analytics are part of the demand test. Events should answer whether the hostile validation report is working.

Minimum events:

- `upload_started`
- `upload_rejected`
- `eligibility_viewed`
- `missing_evidence_cta_clicked`
- `analysis_started`
- `analysis_completed`
- `ledger_viewed`
- `report_generated`
- `report_exported`
- `share_room_created`
- `share_room_opened`
- `share_room_expired`
- `research_desk_requested`
- `upgrade_clicked_from_locked_diagnostic`
- `template_downloaded`
- `template_uploaded_successfully`

Event payload rules:

- include account/plan bucket, not raw identity in analytics exports
- include artifact class
- include diagnostic availability counts
- include ledger counts by evidence status
- include report version where relevant
- never include raw strategy names, uploaded filenames, report content, or bridge logs

### 11. Feature Flag And Rollout Plan

Ship the wedge in slices so a half-built ledger or share room does not reach users as a confusing trust artifact.

Feature flags:

- `evidence_ledger_internal`: ledger computed and visible only in admin/debug
- `evidence_ledger_user`: ledger visible in upload, overview, and report
- `validation_templates`: packet templates visible in docs/intake
- `report_share_room_internal`: share rooms creatable by admins/test accounts only
- `report_share_room_public`: share rooms available to eligible users
- `research_desk_handoff`: CTA creates internal queue items

Rollout order:

1. Cross-repo fixtures and seam versioning.
2. Internal ledger computation.
3. User-visible eligibility and overview ledger.
4. Report appendix ledger.
5. Validation templates.
6. Report share room internal.
7. Report share room public.
8. Research Desk queue/handoff.

### 12. Memory Promotion Gate

Do not start tenant research memory until the Lab wedge shows demand.

Promotion criteria:

- at least 10 real artifact bundles reviewed
- at least 3 users submit a second or richer artifact after seeing limitations
- at least 3 reports are shared externally or used in a Research Desk request
- evidence ledger contract and tests are stable
- report snapshot/version model is live
- report sharing privacy model is implemented
- there is a specific user workflow that asks "where have I seen this before?"

When promoted, tenant memory starts with:

- per-account strategy history
- repeated unsupported claims
- recurring missing evidence
- hostile regimes per user/account
- report/version lineage

Not included at first:

- cross-user recommendations
- anonymized benchmark learning
- strategy generation
- automatic deployment advice

## Target Product Shape

### First Screen Promise

The public `/robustness-lab` page should sell one concrete action:

> Upload strategy evidence and receive a validation report that tells you what is supported, what is fragile, and what cannot be concluded.

Avoid leading with "platform" language. Lead with a job the user already understands.

### Authenticated Workflow

The current `/app/new-analysis` workflow should become:

1. Upload artifact.
2. Inspect artifact.
3. Explain what is available, limited, and unavailable.
4. Let the user name the strategy and choose benchmark/runtime assumptions.
5. Run analysis.
6. Show progress with meaningful stages.
7. Land on verdict-first overview.
8. Allow diagnostic deep dives.
9. Generate a shareable validation report.
10. Offer deeper Research Desk review when unsupported conclusions matter.

### Core Objects

Current objects to keep:

- account
- user
- artifact
- analysis
- analysis job
- export
- export job
- entitlement
- usage snapshot
- benchmark config

Objects to add later:

- strategy
- project
- market claim
- hypothesis
- validation plan
- experiment group
- research finding
- limitation
- user annotation
- reviewer annotation
- research memory entry

Do not add all of these in the first pass. Add `strategy`, `project`, and `market_claim` only when the current Lab can already produce a credible validation report.

## Engineering Rollout Slices For Approach A

Generated by `/plan-eng-review` on 2026-05-15 after the complexity gate selected phased scope.

Approach A remains the baseline and the CEO scope remains valid, but implementation should not land as one large cross-repo change. The first slice must prove the trust contract before adding share rooms, Research Desk handoff, or learning/memory features.

### Slice 1: Evidence And Seam Foundation

Goal: make the existing Lab's evidence status deterministic and cross-repo-safe.

Owns:

- shared engine seam fixtures in both repos
- `EngineEnvelopeV1` and compatibility matrix
- canonical `EvidenceLedgerService` in a dedicated evidence module, not inside the bulletproof adapter mapper
- shared evidence ledger contract schemas and fixtures
- upload inspection projections from the ledger
- diagnostic access projections with entitlement overlays
- fail-closed adapter behavior for unknown diagnostics
- fixture and snapshot tests for trade CSV, limited bundle, full bundle, and malformed payload

Must not include:

- share rooms
- public report links
- Research Desk workflow changes
- tenant memory
- LLM-driven claim rewriting

### Slice 2: Report Snapshot Foundation

Goal: make reports immutable, reproducible trust artifacts.

Owns:

- `report_snapshots` persistence
- report payload generation from analysis + ledger snapshot
- owner exports rendering from report snapshots
- report regeneration idempotency
- stale/superseded warnings
- transition guard tests for analysis/report state

Must not include:

- externally shared report rooms
- comments or collaboration
- marketplace/profile behavior

### Slice 3: Share Room Trust Boundary

Goal: make the report a safe, measurable demand object.

Owns:

- dedicated share token model
- `/share/[token]` route/API
- `SharedReportViewModel` allowlisted projection
- expiry, revocation, superseded behavior
- share access audit events without report-content logging
- indexes and cleanup rules for share token lookup, access events, expired shares, revoked shares, and superseded reports
- redaction tests for all sensitive fields

### Slice 4: Research Desk And Learning Loop

Goal: route high-value gaps to human review and product learning.

Owns:

- Research Desk request from limitations
- reviewer addenda tied to report snapshots
- wedge learning events
- validation packet template engagement
- memory promotion gates only after repeated evidence supports promotion

### Slice 5: Full Ambition Expansion

Goal: graduate from Lab-first product to claim-first research operating system.

Owns:

- explicit market claim capture
- strategy entity lifecycle
- tenant-scoped research memory
- experiment planning and promotion workflow
- cross-strategy research recall

## Approach A Detailed Implementation Plan

### Phase A0: Repo Contract Map

Goal:

Create a written contract between `invariance_research` and `bulletproof_bt` so both repos can evolve without accidental breakage.

Actions in `bulletproof_bt`:

- document `run_analysis_from_parsed_artifact`
- document `ParsedArtifactInput`
- document `AnalysisRunConfig`
- document `EngineAnalysisResult`
- version the SaaS seam payload
- add golden JSON fixtures for trade CSV, structured bundle, parameter sweep, and incomplete artifact
- add a fixture that intentionally lacks OHLCV/benchmark/context so unsupported diagnostics are locked

Actions in `invariance_research`:

- document expected engine response envelope
- add fixture tests around `map-engine-analysis-record.ts`
- add contract tests for `UploadInspectionResponse`, `CreateAnalysisRequest`, `AnalysisRecord`, and export payloads
- add a compatibility matrix mapping app diagnostic pages to engine diagnostic names

Acceptance criteria:

- a developer can change `bulletproof_bt` diagnostics and know which web tests must pass
- unsupported diagnostics are represented as first-class outputs, not missing data
- every engine payload carries version, engine name, adapter version, and capability profile

### Phase A1: Make Upload Eligibility The Trust Moment

Goal:

Turn upload inspection into a product asset, not a preflight form.

Current app already has:

- file type checks
- 10 MB limit
- `.csv` and `.zip`
- parser/validator pipeline
- eligibility summary
- plan upload policy checks

Needed changes:

- introduce the canonical `EvidenceLedgerService` before expanding report/share surfaces
- show a clearer "what we can conclude" and "what we cannot conclude" panel from ledger projections
- separate data insufficiency from plan locks by deriving evidence first and applying entitlement overlays second
- show missing fields that would unlock each diagnostic
- show parser confidence and assumptions
- show detected time range, symbols, trade count, costs, benchmark presence, OHLCV presence, parameter sweep presence
- preserve rejected uploads as diagnostic events only if privacy policy allows
- add sample downloadable artifact templates

Diagnostic eligibility language:

- "Available" means the diagnostic is supported by the artifact and engine.
- "Limited" means the product can compute a bounded proxy but cannot support a strong conclusion.
- "Unavailable" means the data does not support the diagnostic.
- "Locked" means the plan does not include the diagnostic even though the artifact may support it.

Acceptance criteria:

- a user knows why a diagnostic is unavailable before paying
- a user knows exactly what to upload next
- a plan restriction is never confused with missing evidence
- the app never silently infers execution realism from trade CSV alone

### Phase A2: Harden The Engine Seam

Goal:

Make the Node-to-Python bridge boring and auditable.

Current app already calls:

```text
analysis worker
  -> buildAnalysisEngineDispatchPayload
  -> runBulletproofAnalysisFromParsedArtifact
  -> runBulletproofEngine
  -> scripts/run_bulletproof_engine.py
  -> bt.run_analysis_from_parsed_artifact
  -> map-engine-analysis-record.ts
```

Needed changes:

- introduce a versioned `EngineEnvelopeV1` before ledger/report/share expansion
- add seam version negotiation
- add `seam_version`, `adapter_version`, `parser_version`, `capability_profile_version`, and `diagnostic_contract_version` to persisted engine context
- add engine timeout and failure classification
- persist bridge stdout/stderr safely for admins without exposing user strategy content
- require engine result schema validation before persistence
- persist engine context alongside analysis result
- distinguish engine failure, parser failure, queue failure, entitlement failure, and report failure
- add fixture-based regression tests for every mapped diagnostic
- add a compatibility test that imports `bt` and probes the seam in CI

Acceptance criteria:

- failed analyses show product-safe user errors
- admins can debug failures without touching raw strategy content unnecessarily
- changes to `bt.saas.models` fail fast in the web app
- engine payloads cannot accidentally bypass adapter normalization

### Phase A3: Verdict-First Results Overview

Goal:

The results page should read like a hostile research memo, not a dashboard.

Current app already has diagnostic pages:

- overview
- distribution
- monte carlo
- execution
- regimes
- ruin
- stability
- report

Needed top-level overview:

- headline verdict: robust, conditional, fragile, unsupported, or failed validation
- one-sentence reason
- three reasons to trust
- three reasons to doubt
- most important unsupported conclusion
- next experiment to run
- export/report CTA
- Research Desk CTA when a limitation blocks a high-value decision

Verdict rules:

- "Robust" requires multi-diagnostic support, not a high score alone.
- "Conditional" means the strategy is promising under stated assumptions but missing key validation.
- "Fragile" means one or more diagnostics show material dependence on costs, regime, parameter, or outlier concentration.
- "Unsupported" means the artifact cannot prove the user's likely claim.
- "Failed validation" means the evidence actively contradicts the claim.

Acceptance criteria:

- charts support the verdict instead of replacing it
- a user can explain the result to someone else in under one minute
- unsupported diagnostics are visible on the overview, not hidden in tabs

### Phase A4: Make The Report Worth Sharing

Goal:

The report must feel like an artifact someone would send to a buyer, allocator, partner, or internal committee.

Current app already has:

- report page
- deterministic JSON, Markdown, PDF exports
- export queue
- download endpoint

Engineering review decision: immutable report snapshots come before share rooms. Today exports are generated from mutable analysis records. Approach A must first create a snapshot foundation so retries, reruns, regenerations, and shared links cannot silently point at stale or changed evidence.

Snapshot foundation requirements:

- add `report_snapshots` before adding share rooms
- store immutable `report_payload_json`, `source_analysis_id`, `source_hash`, `ledger_snapshot_id`, `report_version`, `generated_at`, and `superseded_by_report_id`
- render owner exports and shared reports from a report snapshot, not directly from a mutable `AnalysisRecord`
- make regeneration create a new snapshot instead of mutating the previous report
- make share rooms point to `report_snapshot_id` only
- show stale/superseded warnings when a newer accepted analysis or report exists
- add idempotency keys for report generation/export/share creation

Required sequence:

```text
analysis completed
  -> evidence ledger snapshot created
  -> report snapshot generated from analysis + ledger snapshot
  -> owner export renders from report snapshot
  -> share room points to report snapshot
  -> rerun/regenerate creates new immutable snapshot
```

Needed report sections:

- executive verdict
- strategy and artifact identity
- evidence received
- diagnostic availability matrix
- performance summary
- cost/execution assumptions
- trade distribution and concentration
- benchmark context
- Monte Carlo and ruin assumptions
- parameter stability if available
- regime sensitivity if available
- limitations and unsupported claims
- reproducibility appendix
- engine, adapter, parser, dataset, and report versions
- content hashes for inputs and derived artifacts

PDF improvement path:

- keep deterministic renderer first
- add stronger layout only after content contract stabilizes
- no investment advice language
- no unsupported confidence language

Acceptance criteria:

- every chart or metric in the report has a source diagnostic
- every conclusion points to evidence or limitation
- a report generated today can be reproduced later from stored artifacts and versions
- reports can be shared without leaking raw uploaded files by default

### Phase A5: Research Desk Handoff

Goal:

Convert incomplete automated validation into a revenue and learning path.

Current app already has:

- public `/research-desk`
- contact funnel
- admin console
- account and analysis records

Needed changes:

- add "Request deeper validation" CTA from report and limited diagnostics
- capture which limitation triggered the request
- create admin queue item tied to analysis ID
- allow internal reviewer notes
- allow reviewer-approved report addendum
- track requested services: execution audit, data QA, benchmark suite, claim formalization, strategy rewrite as hypothesis, full advisory validation

Acceptance criteria:

- a serious user never hits a dead end
- every Research Desk request is linked to the exact artifact, analysis, limitations, and report
- manual review teaches the product what to automate next

### Phase A6: Production Persistence And Storage

Goal:

Remove assumptions that block real deployment.

Current app has:

- SQLite persistence
- Postgres schema/repository work in progress
- local object storage abstraction
- benchmark provider abstraction
- health checks

Needed path:

- make Postgres the production default
- keep SQLite for local dev only
- move uploads, exports, reports, and benchmark manifest/data to S3/R2-compatible object storage
- enforce object keys by account, artifact, analysis, and export ID
- add retention policy controls
- add deletion/tombstone paths
- add backup/restore runbook
- add rate limits to upload, analysis create, auth, waitlist, and contact endpoints
- add worker concurrency controls by plan and system load

Acceptance criteria:

- production does not depend on workstation paths
- workers can run outside the web process
- storage survives redeploys
- user deletion and retention policies are documented
- admin health shows DB, storage, engine, queue, Stripe, email, benchmark, and worker state

## Strategy Truth Room Sellable Approach A Definition

Approach A should not ship as "a dashboard for backtests." It should ship as a falsification room for strategy evidence.

Product name:

> Strategy Truth Room

Core promise:

> Upload evidence. Get hostile validation.

Best positioning:

> Institutional-grade strategy due diligence for traders who cannot afford to fool themselves.

Primary actions:

- Validate My Strategy
- Audit This Backtest
- Generate Proof Report
- Research Desk Review

The product should optimize for falsification:

- what assumptions produced this result?
- what happens when fills get worse?
- what happens when fees change?
- what happens outside the cherry-picked regime?
- how much of the edge comes from rare trades?
- how quickly does the thesis die under perturbation?
- what evidence is missing?
- what does this result not prove?

### Office Hours Demand Reality

The user is not buying another way to admire an equity curve. The user is buying defensible doubt.

Current status quo:

- spreadsheet summaries
- screenshots from backtest tools
- broker exports without context
- strategy seller PDFs
- forum claims
- self-built notebooks
- basic trade journaling analytics
- informal code or data reviews

The wedge is strongest when the user already has evidence and a claim they need to defend or kill. Do not start by asking the user to build a strategy inside the app. Start by letting them upload the proof they already trust, then show where that proof fails.

First customer profiles:

- serious independent systematic trader validating a live or near-live system
- strategy seller or educator who needs a buyer-ready validation memo
- allocator, prop evaluator, or partner screening a claimed edge
- crypto, FX, index, or equity researcher stress-testing regime-dependent results
- backtest platform power user who needs a hostile external audit

Demand test:

The product has pull when users export or share conditional and negative reports, not only flattering reports.

### CEO Review Scope Decision

Scope mode:

> Selective expansion, with Lab-first hardening as the baseline.

Keep Approach A focused on artifact-first validation. Cherry-pick only the expansions that make the first product commercially credible:

- stronger artifact schema and bundle contract
- falsification-first analyst workbench
- explicit unsupported-claims inventory
- proof report snapshots and share controls
- Research Desk upgrade path
- tiering that turns missing evidence and advanced diagnostics into natural upgrades

Do not include in launch Approach A:

- natural-language strategy generation
- web-native strategy builder
- signal marketplace
- broker execution
- live trading
- cross-user intelligence
- full research OS memory
- portfolio allocator

Those are full-ambition products. Approach A wins by making uploaded evidence trustworthy or visibly untrustworthy.

### Sellable Readiness Bar

Approach A is sellable only when every completed analysis produces:

- a clear verdict classification
- an evidence coverage map
- an unsupported-claims inventory
- an assumptions ledger
- at least one concrete falsification result
- a diagnostic availability matrix
- a "next evidence to upload" path
- an exportable proof report
- a Research Desk path when automation cannot answer the user's decision

The verdict taxonomy:

- Structurally credible: the available evidence supports the claim across the required diagnostics.
- Promising but under-supported: the result may be real, but missing artifacts block stronger conclusions.
- Likely overfit: performance appears too dependent on narrow parameters, outliers, or selection.
- Execution fantasy: the edge degrades materially under realistic costs, fills, latency, or liquidity assumptions.
- Data-insufficient: the artifact cannot support the claim the user likely wants to make.
- Regime-dependent: edge exists mainly in a narrow market state.
- Untradeable after costs: expected edge is consumed by plausible costs, slippage, or sizing constraints.

The overview must always explain why the verdict exists. A score without a hostile reason is not enough.

### Analyst Workbench Information Architecture

The authenticated product should feel like an analyst workbench, not a pile of chart cards. Every page should answer a falsification question, show the evidence behind the answer, and end with a user decision.

Shared page anatomy:

- verdict strip: the page-level conclusion in one sentence
- evidence state: Available, Limited, Unavailable, or Locked
- what was tested
- what the result says
- what assumption matters most
- what evidence is missing
- next action: upload richer evidence, run stronger test, export report, or request Research Desk review

The sidebar should always include all workspaces, even when gated or artifact-limited. Missing diagnostics are product education, not invisible features.

Required workspaces for sellable Approach A:

- Evidence Intake
- Truth Room Overview
- Assumption Ledger
- Execution Reality
- Distribution And Edge Concentration
- Monte Carlo Survival
- Ruin And Capital Survival
- Regime Dependence
- Parameter Stability
- Proof Report
- Share Room
- Research Desk Review
- Analysis Library

### Evidence Intake

Falsification question:

> What can this artifact prove, and what can it not prove?

Must show:

- detected artifact type
- parser confidence
- time range
- asset universe
- symbol coverage
- trade count or observation count
- fields detected
- costs detected
- OHLCV/context detected
- benchmark detected
- parameter sweep detected
- config detected
- unsupported diagnostics and exact missing inputs
- plan locks separated from evidence limitations

Primary user decision:

- continue with limited validation
- upload a richer bundle
- use a template
- request Research Desk help

Premium behavior:

Even free users should see what stronger artifacts would unlock. They should not see enough output to replace the paid diagnostic.

### Truth Room Overview

Falsification question:

> Is this result credible enough to keep investigating?

Must show:

- headline verdict
- credibility score
- evidence coverage score
- diagnostic availability matrix
- three reasons to trust the result
- three reasons to doubt the result
- strongest positive evidence
- strongest negative evidence
- most important unsupported claim
- next kill test
- export/report CTA
- Research Desk CTA if a limitation blocks a high-value decision

Primary user decision:

- accept the verdict as sufficient
- upload richer evidence
- inspect a specific failure mode
- generate a proof report

Design note:

The overview should not be a metric grid. It should read like the first page of a hostile research memo with supporting instruments below it.

### Assumption Ledger

Falsification question:

> What assumptions produced this result?

This is a missing workspace and should become a first-class product object. It can also appear as a persistent panel across other workspaces.

Must show:

- declared assumptions from config, report, or user input
- inferred assumptions from uploaded data
- default assumptions the engine applied
- cost assumptions
- execution assumptions
- sizing assumptions
- benchmark assumptions
- timezone and currency assumptions
- missing assumptions
- assumptions that materially affect verdict
- assumptions contradicted by artifacts

Primary user decision:

- accept assumptions for report
- revise assumptions
- upload supporting evidence
- request human review where assumptions are ambiguous

Report rule:

No proof report should bury assumptions in an appendix only. The executive report must state the assumptions that make the verdict true.

### Execution Reality

Falsification question:

> Does the edge survive worse fills, fees, spreads, and slippage?

Must show:

- baseline expectancy versus stressed expectancy
- fee sensitivity
- slippage sensitivity
- spread sensitivity where data permits
- commission model detected or assumed
- breakeven cost threshold
- execution fantasy warnings
- cost-adjusted drawdown and profitability
- unsupported execution claims
- broker/fill evidence quality

Primary user decision:

- trust the execution assumptions
- upload broker fills
- revise cost model
- classify the strategy as execution-fragile

Evidence behavior:

Trade CSV alone can support limited cost sensitivity, but not a strong execution realism verdict. Broker fills or explicit cost assumptions are required for strong execution conclusions.

### Distribution And Edge Concentration

Falsification question:

> Is the edge broad, or does it come from rare trades and outliers?

Must show:

- return distribution
- win/loss distribution
- top trade contribution
- top 5 and top 10 trade contribution
- payoff asymmetry
- streak profile
- sample size warnings
- tail dependence
- median trade versus mean trade
- concentration score
- rare-trade reliance verdict

Primary user decision:

- trust the edge as repeatable
- treat the result as outlier-dependent
- collect more trades
- segment the strategy before drawing conclusions

Design note:

This page should make "one whale trade made the backtest" impossible to miss.

### Monte Carlo Survival

Falsification question:

> Does the thesis survive path perturbation?

Must show:

- equity fan chart
- ending equity distribution
- drawdown envelope
- survival probability
- probability of hitting user-selected drawdown levels
- losing streak distribution
- time-to-recovery distribution where possible
- path dependence warning
- assumptions behind resampling

Primary user decision:

- accept path risk
- reduce size
- gather more trades
- reject the strategy as path-fragile

Evidence behavior:

Monte Carlo from trade logs is useful but bounded. It does not prove market stationarity. The page must say when it is resampling the past rather than simulating a richer future distribution.

### Ruin And Capital Survival

Falsification question:

> Can the account survive this edge under realistic sizing?

Must show:

- capital survival score
- ruin probability under baseline assumptions
- ruin probability under stressed costs
- drawdown breach probabilities
- risk-per-trade sensitivity
- max adverse run
- account size and sizing assumption
- recommended risk guardrails as non-advisory diagnostics
- limits of the ruin model

Primary user decision:

- reduce size
- reject the strategy
- upload sizing/config evidence
- request deeper review

Language rule:

The app must not give investment advice. It can say "this sizing assumption creates a high probability of breaching a 30% drawdown threshold under this model."

### Regime Dependence

Falsification question:

> What happens outside the cherry-picked regime?

Must show:

- regime availability state
- regime definitions used
- best and worst regime
- performance by volatility/trend/liquidity state where supported
- regime heatmap
- regime dispersion score
- missing OHLCV/context warning
- comparison between full sample and favorable regimes
- unsupported regime claims

Primary user decision:

- upload OHLCV/context data
- restrict the claim to specific regimes
- treat the result as regime-dependent
- request Research Desk benchmark/context construction

Evidence behavior:

This tab must always exist. If OHLCV or context is missing, it should show the locked evidence state and explain exactly what bundle unlocks the analysis.

### Parameter Stability

Falsification question:

> How quickly does the thesis die under parameter perturbation?

Must show:

- parameter sweep availability state
- parameter surface
- robustness plateau
- cliff zones
- best parameter versus neighboring parameters
- sensitivity by objective metric
- overfit warning
- missing sweep requirements
- required format for parameter grid uploads

Primary user decision:

- accept robustness
- classify as overfit-prone
- upload a parameter sweep
- request Research Desk sweep design

Evidence behavior:

This page requires a parameter sweep or structured engine run bundle. It must not fake stability from a single backtest run.

### Proof Report

Falsification question:

> Can this evidence be shared as defensible proof without overclaiming?

Must show:

- executive verdict
- strategy and artifact identity
- evidence received
- evidence coverage matrix
- assumptions ledger summary
- unsupported claims
- diagnostic summaries
- key charts only where they support the verdict
- limitations
- reproducibility appendix
- engine, parser, adapter, report, and artifact schema versions
- content hashes
- export controls
- share controls

Primary user decision:

- export private PDF
- create controlled share link
- regenerate after richer evidence
- request Research Desk addendum

Report standard:

The report should feel buyer-ready, allocator-ready, and committee-readable. It should not read like a marketing brochure. Negative and conditional reports must still feel valuable.

### Share Room

Falsification question:

> Can this verdict be shared without leaking strategy IP or overstating evidence?

Must show:

- current report snapshot
- share status
- expiry
- access log summary
- fields included and excluded
- raw artifact privacy status
- revocation control
- superseded report warning
- recipient-safe report preview

Primary user decision:

- create link
- revoke link
- regenerate report before sharing
- upgrade for private diligence room

Threat model:

Shared reports must render from immutable report snapshots and allowlisted projections. Raw uploads, internal engine payloads, account data, private notes, and Research Desk reviewer drafts must never be available in the public share context.

### Research Desk Review

Falsification question:

> What requires human or agent-assisted review beyond automated validation?

Must show:

- limitation that triggered the handoff
- recommended review type
- evidence packet preview
- estimated scope
- optional addendum path
- reviewer-safe artifact access rules
- prior report snapshot

Primary user decision:

- request execution audit
- request data QA
- request benchmark/context construction
- request parameter sweep design
- request buyer/allocator memo upgrade

Research Desk should be the pressure-release valve for serious users. A paid user should never reach "the product cannot answer this" without a next step.

### Analysis Library

Falsification question:

> How has this strategy's evidence changed over time?

Must show:

- analyses grouped by strategy or artifact family
- verdict history
- evidence coverage history
- report snapshots
- exports
- richer-upload prompts
- stale or superseded reports
- comparison-ready runs

Primary user decision:

- rerun with richer evidence
- compare two analyses
- regenerate report
- move into later full-ambition strategy workspace

This library is the bridge to Stage 2 strategy workspaces. It should be useful before full research OS memory exists.

## Artifact Schema And Diagnostic Unlock Contract

Approach A needs a public artifact contract. Users should understand which evidence unlocks which conclusions before they upload.

Artifact states:

- Available: the artifact supports the diagnostic.
- Limited: the product can compute a bounded proxy but cannot make a strong conclusion.
- Unavailable: the artifact cannot support the diagnostic.
- Locked: the user's plan does not include the diagnostic even though the artifact may support it.

Evidence must be evaluated before entitlement. A diagnostic can be artifact-supported and plan-locked, but a plan must never turn unsupported evidence into a supported conclusion.

### `trade_log_csv_v1`

Purpose:

- validate realized or simulated trade outcomes
- compute distribution, concentration, Monte Carlo, drawdown, and limited execution sensitivity

Required fields:

- `entry_time`
- `exit_time` or close timestamp
- `symbol` or instrument
- `side`
- `entry_price`
- `exit_price`
- `quantity`, `size`, or normalized exposure
- `pnl`, `return`, or enough fields to compute it

Recommended fields:

- `trade_id`
- `fees`
- `slippage`
- `strategy_tag`
- `timeframe`
- `entry_reason`
- `exit_reason`
- `account_currency`
- `venue`

Unlocks:

- Overview: Full
- Assumption Ledger: Limited
- Distribution: Full
- Monte Carlo: Full
- Ruin: Limited to Full depending on sizing fields
- Execution: Limited unless costs and fills are included
- Regime: Limited unless OHLCV/context is included
- Parameter Stability: No
- Proof Report: Full with limitations

### `equity_curve_v1`

Purpose:

- validate path behavior when trade-level data is unavailable

Required fields:

- `timestamp`
- `equity`, `nav`, or cumulative return

Recommended fields:

- deposits/withdrawals
- benchmark value
- account currency
- strategy tag

Unlocks:

- Overview: Limited
- Monte Carlo: Limited path analysis
- Ruin: Limited drawdown breach analysis
- Distribution: No trade-level concentration
- Execution: No
- Regime: Limited only with aligned OHLCV/context
- Parameter Stability: No
- Proof Report: Limited

### `broker_export_v1`

Purpose:

- audit execution realism from fills, orders, commissions, and venue data

Required fields:

- fill timestamp
- symbol
- side
- fill price
- quantity
- commission or fee where available

Recommended fields:

- order type
- order timestamp
- fill venue
- spread
- liquidity flag
- account currency
- order id
- trade id mapping

Unlocks:

- Execution: Full when matched to strategy trades
- Assumption Ledger: Full for execution assumptions
- Distribution: Limited unless complete trade lifecycle exists
- Proof Report: Full execution appendix

### `backtest_report_v1`

Purpose:

- audit claims and stated metrics from existing reports, PDFs, HTML exports, or JSON outputs

Accepted formats:

- PDF
- HTML
- JSON
- CSV summary
- Markdown

Extracted objects:

- declared performance metrics
- stated assumptions
- strategy claims
- cost claims
- benchmark claims
- data period
- asset universe
- tool/source metadata

Unlocks:

- Assumption Ledger: Limited to Full depending on parse quality
- Unsupported Claims Inventory: Full
- Overview: Limited unless raw trades/equity are included
- Proof Report: Limited claim-audit report

Rule:

A report alone can be audited for unsupported claims. It cannot validate the underlying strategy without raw evidence.

### `ohlcv_context_v1`

Purpose:

- support regime dependence, benchmark context, and market-state conditioning

Required fields:

- timestamp
- open
- high
- low
- close
- volume where available
- symbol
- timeframe

Recommended fields:

- adjusted close
- session/calendar metadata
- asset class
- exchange
- liquidity proxy

Unlocks:

- Regime: Full when aligned to trades or equity curve
- Benchmark Context: Limited to Full
- Execution: Limited spread/liquidity proxies where available
- Proof Report: regime appendix

### `parameter_sweep_v1`

Purpose:

- validate whether results survive parameter perturbation

Required fields:

- run id
- parameter names
- parameter values
- objective metric
- sample period

Recommended fields:

- train/test split
- costs used
- drawdown
- Sharpe or risk-adjusted metric
- trade count
- seed
- config hash

Unlocks:

- Parameter Stability: Full
- Overfit Warnings: Full
- Proof Report: parameter appendix

Rule:

Single-run configs do not unlock parameter stability. They only populate the assumption ledger.

### `strategy_config_v1`

Purpose:

- capture assumptions, sizing, costs, filters, and risk model

Accepted formats:

- JSON
- YAML
- TOML
- INI
- plain text with structured extraction

Recommended fields:

- strategy name
- asset universe
- timeframe
- entry/exit rules
- filters
- sizing rules
- risk limits
- cost model
- data source
- benchmark
- excluded periods

Unlocks:

- Assumption Ledger: Full
- Unsupported Claims Inventory: Full when paired with declared claims
- Ruin: Full when sizing fields exist
- Proof Report: assumptions appendix

### `benchmark_series_v1`

Purpose:

- compare results against a relevant market or strategy benchmark

Required fields:

- timestamp
- benchmark value or return
- benchmark identifier

Unlocks:

- Overview: benchmark-relative context
- Regime: benchmark-aware context where aligned
- Proof Report: benchmark appendix

### Full Validation Bundle

A full bundle is the gold-standard upload for Approach A.

Required top-level file:

```json
{
  "schema_version": "strategy_truth_room_bundle_v1",
  "bundle_id": "uuid",
  "strategy_identity": {
    "name": "string",
    "asset_universe": ["string"],
    "base_currency": "USD",
    "timezone": "UTC"
  },
  "declared_claims": [
    {
      "claim": "string",
      "claimed_metric": "string",
      "invalidation_condition": "string"
    }
  ],
  "source_tool": "string",
  "export_timestamp": "iso-8601",
  "privacy_flags": {
    "allow_report_share": false,
    "allow_research_desk_access": false
  },
  "files": [
    {
      "path": "trades.csv",
      "artifact_type": "trade_log_csv_v1",
      "sha256": "string"
    }
  ]
}
```

Recommended bundle files:

- `manifest.json`
- `trades.csv`
- `equity_curve.csv`
- `broker_fills.csv`
- `ohlcv/*.csv`
- `parameter_sweep.csv`
- `strategy_config.json`
- `benchmark.csv`
- `source_report.pdf`, `source_report.html`, or `source_report.json`

Full bundle unlocks:

- Truth Room Overview: Full
- Assumption Ledger: Full
- Execution Reality: Full
- Distribution And Edge Concentration: Full
- Monte Carlo Survival: Full
- Ruin And Capital Survival: Full
- Regime Dependence: Full
- Parameter Stability: Full
- Proof Report: Full
- Share Room: Full subject to plan
- Research Desk Review: Full packet

### Diagnostic Unlock Matrix

| Workspace | Trade log | Equity curve | Broker export | Backtest report | OHLCV/context | Parameter sweep | Strategy config | Full bundle |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Evidence Intake | Full | Full | Full | Full | Full | Full | Full | Full |
| Truth Room Overview | Full | Limited | Limited | Limited | Limited | Limited | Limited | Full |
| Assumption Ledger | Limited | Limited | Full for execution | Limited to Full | Limited | Limited | Full | Full |
| Execution Reality | Limited | No | Full | Limited claims audit | Limited proxy | No | Limited to Full | Full |
| Distribution | Full | No | Limited | No | No | Limited by run metrics | No | Full |
| Monte Carlo | Full | Limited | Limited | No | No | No | Limited sizing context | Full |
| Ruin | Limited to Full | Limited | Limited | No | No | No | Full if sizing exists | Full |
| Regime Dependence | Limited | Limited | No | No | Full when aligned | Limited by run metrics | Limited | Full |
| Parameter Stability | No | No | No | No | No | Full | Limited assumptions only | Full |
| Proof Report | Full with limitations | Limited | Full execution appendix | Limited claim audit | Regime appendix | Parameter appendix | Assumptions appendix | Full |
| Share Room | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Plan-dependent | Full subject to plan |
| Research Desk Review | Full packet component | Packet component | Packet component | Packet component | Packet component | Packet component | Packet component | Full |

## Launch Subscription Model For Approach A

Pricing should reflect the value of avoiding false confidence, not the commodity value of charting. The first launch should be simple enough to explain and strong enough to support a premium report product.

Principles:

- Free should demonstrate the falsification experience, not give away a full proof report.
- Paid individual should be attractive to serious solo traders.
- Pro should unlock the full automated due diligence loop.
- Team should support commercial evaluation, education businesses, and small research groups.
- Research Desk should monetize high-stakes ambiguity without turning the automated product into consulting.

### Recommended Launch Tiers

| Tier | Price | Best for | Core limit |
| --- | ---: | --- | --- |
| Explorer | $0/month | curious traders testing one artifact | 2 analyses/month |
| Truth Room Individual | $49/month | serious individual traders | 25 analyses/month |
| Truth Room Pro | $149/month | strategy sellers, educators, advanced researchers | 100 analyses/month |
| Research Lab Team | $399/month | small teams, prop evaluators, research desks | 500 analyses/month, 5 seats |
| Research Desk Review | $750-$2,500/review | high-stakes diligence or ambiguous evidence | scoped manual/agent-assisted review |

### Permission Matrix

| Capability | Explorer | Individual | Pro | Team | Research Desk add-on |
| --- | --- | --- | --- | --- | --- |
| Trade CSV upload | Yes | Yes | Yes | Yes | Yes |
| Equity curve upload | Yes | Yes | Yes | Yes | Yes |
| Broker export upload | Preview only | Yes | Yes | Yes | Yes |
| Backtest report upload | Preview only | Yes | Yes | Yes | Yes |
| Full validation bundle | No | Limited | Yes | Yes | Yes |
| Truth Room Overview | Limited | Full | Full | Full | Full |
| Assumption Ledger | Limited | Full | Full | Full | Reviewer-enhanced |
| Execution Reality | Preview | Full | Full | Full | Reviewer-enhanced |
| Distribution | Limited | Full | Full | Full | Full |
| Monte Carlo | Limited | Full | Full | Full | Full |
| Ruin | Preview | Full | Full | Full | Reviewer-enhanced |
| Regime Dependence | Locked preview | Locked or add-on | Full | Full | Reviewer-enhanced |
| Parameter Stability | Locked preview | Locked or add-on | Full | Full | Reviewer-enhanced |
| Proof Report export | Watermarked preview | PDF/Markdown | PDF/Markdown/JSON | PDF/Markdown/JSON | Addendum included |
| Controlled share links | No | 5 active links | 25 active links | 100 active links | Diligence packet |
| Report snapshots | Latest only | 30-day history | 1-year history | 2-year history | Included in packet |
| Analysis library | Latest 5 | Full personal | Full | Team-wide | Included |
| Research Desk request | Waitlist/contact | Paid add-on | Discounted add-on | Priority add-on | Included |
| API/webhook access | No | No | Limited beta | Limited beta | No |
| Support | Community/docs | Email | Priority | Priority/team admin | Scoped review channel |

### Tier Notes

Explorer:

- must make the user feel the product is hostile and useful
- should show unsupported claims and missing evidence
- should not allow polished proof export
- should show locked Regime and Parameter pages with upload requirements

Individual:

- should be enough for a serious trader validating personal systems
- includes export because the report is the product
- can gate Regime and Parameter Stability if pricing pressure requires it, but the pages must remain visible

Pro:

- should be the default recommended paid tier for strategy sellers, educators, and advanced researchers
- unlocks the full automated suite when artifacts support it
- includes share links because external trust is a major buying reason

Team:

- should support multiple analysts and repeat due diligence
- requires admin history, seat control, and stronger retention controls before being aggressively sold

Research Desk:

- should be sold as deep validation review, not investment advice
- should produce reviewer-approved report addenda tied to immutable report snapshots
- should focus on execution audit, data QA, benchmark construction, claim formalization, and parameter/regime review

### Commercial Packaging Rules

Locked pages should not disappear. They should show:

- what the diagnostic would answer
- what artifact is missing
- what the user's current plan includes
- what upgrade or upload unlocks it
- why the product refuses to make a stronger claim

The upgrade path should be tied to trust, not artificial scarcity:

- "Your artifact does not support parameter stability" is an evidence limitation.
- "Your plan does not include parameter stability exports" is an entitlement limitation.
- The UI must never confuse the two.

### Implementation Implications

The current product is not sellable until the core workbench surfaces are rebuilt around falsification rather than chart presentation.

Highest-priority implementation gaps:

- create the Assumption Ledger workspace
- make Regime and Parameter Stability visible in the sidebar even when gated or evidence-limited
- replace old card language with verdict-led evidence instruments
- add unsupported-claims inventory across Overview and Report
- make export/report generation a primary CTA on completed analyses
- make Share Room render from immutable report snapshots
- add upload templates for each artifact class
- add bundle manifest parsing
- separate evidence limitations from subscription locks in every workspace
- add Research Desk CTA from limitations, not only from marketing pages

The first product is ready to sell only when a user can upload a messy artifact, understand what it proves, see what it fails to prove, export a defensible memo, and know the next evidence needed to strengthen the verdict.

## First-Client Readiness Roadmap

This roadmap is the implementation bridge from the current two-repo state to a sellable Approach A product: Strategy Truth Room as an artifact-first validation product. The goal is not to build the full research operating system yet. The goal is to create a world-class first product that can take a real client's strategy evidence, falsify it honestly, produce a defensible report, and expose the next evidence required.

The repos already contain important foundations:

- `bulletproof_bt` has the deterministic analysis seam, diagnostic envelopes, capability profiles, trade-based diagnostics, Monte Carlo, execution sensitivity, ruin, regime analysis, parameter stability, and report payloads.
- `invariance_research` has auth, local and Postgres persistence, upload inspection, bundle intake, evidence ledger projection, analysis workers, diagnostic workspaces, chart adapters, report snapshots, export jobs, share tokens, Research Desk records, admin operations, and public product pages.

The product is still not first-client ready because the foundations are not yet unified into a strict Strategy Truth Room contract. The current product can run useful analyses, but it does not yet consistently behave like a hostile validation room that tracks evidence, claims, assumptions, artifact sufficiency, report immutability, share safety, and paid-tier boundaries as first-class product objects.

### Current Product Audit

| Area | Current State | Sellable Gap | Primary Repo |
| --- | --- | --- | --- |
| Product framing | Public pages now describe validation, execution realism, report output, and Research Desk handoff. | App workbench still needs to fully embody "upload evidence, get hostile validation" on every completed-analysis surface. | `invariance_research` |
| Design system | Research-red visual system, artifact surfaces, evidence panels, metric instruments, public-page refinements, and report snapshot UI exist. | Some analysis pages still mix old card grammar with new evidence instruments; workbench IA is not yet cohesive page-to-page. | `invariance_research` |
| Artifact intake | CSV and ZIP uploads exist; generic trade CSV and generic bundle parser exist; upload inspection returns eligibility and an evidence ledger. | Needs canonical Strategy Truth Room bundle manifest, richer artifact families, templates, claim intake, stronger bundle validation, and file-level provenance. | Both |
| Evidence ledger | Upload and engine statuses are reconciled into a diagnostic evidence ledger. | Needs to become a persistent evidence object, not only a diagnostic availability projection. It must include artifact facts, diagnostic unlock reasons, claims, assumptions, contradictions, and report-safe summaries. | `invariance_research` |
| Assumption handling | Diagnostics emit assumptions, limitations, and recommendations; UI has context panels. | No normalized Assumption Ledger workspace yet. Assumptions are not source-linked, materiality-scored, contradicted, or tied to verdict movement. | Both |
| Unsupported claims | Some copy and limitations warn about missing evidence. | No engine-native or app-native claim inventory that says which claims are supported, unsupported, contradicted, or outside scope. | Both |
| Diagnostic coverage | Overview, Execution, Distribution, Monte Carlo, Ruin, Regime, Parameter Stability, and Report routes exist. | The routes need a shared analyst-workbench grammar and stronger falsification content. Gated/unavailable states must remain visible and useful. | `invariance_research` |
| Engine diagnostics | `bulletproof_bt` supports core Approach A diagnostics with degradation behavior. | Diagnostics need stronger artifact-aware realism: broker fills, richer cost/slippage schedules, regime-aware Monte Carlo, stronger parameter topology, asset-class capability statements, and proof-report payloads. | `bulletproof_bt` |
| Report export | Report page has export actions; export queue can render JSON/Markdown/PDF from snapshots. | Export must become a polished validation memo contract with evidence coverage, assumption ledger, unsupported claims, hashes, schema versions, redaction state, and share-room projection. | Both |
| Share Room | Report snapshots, share tokens, share routes, and access events exist. | Needs productized Share Room: privacy controls, revoked/expired states, recipient-safe memo view, access log, redaction policy, and threat-model enforcement. | `invariance_research` |
| Subscription model | Entitlements exist for `explorer`, `professional`, `research_lab`, and `advisory`; diagnostic locks exist. | Needs launch tiers aligned to the new Free/Individual/Pro/Team/Research Desk matrix, pricing, copy, Stripe mapping, and per-diagnostic unlock states. | `invariance_research` |
| Research Desk | Request records, admin page, addenda, and report-page CTA exist. | Needs full handoff packet, human/agent review workflow, addendum approval policy, pricing path, and first-client operating procedure. | `invariance_research` |
| Reliability | Workers, retries, export queue, admin ops, and Postgres schema initialization exist. | Needs end-to-end first-client readiness harness across local SQLite, production Postgres, worker startup, artifact storage, email, exports, and share links. | Both |

### Engine Audit: `bulletproof_bt`

The engine is strong enough to be the foundation of Approach A, but it is not yet the final Strategy Truth Room engine contract.

Strengths already present:

- deterministic SaaS seam: `StrategyRobustnessLabService.run_analysis_from_parsed_artifact`
- structured models for normalized trades, parsed artifacts, analysis run config, engine envelope, run context, diagnostic status, and capability profiles
- diagnostic names covering overview, distribution, Monte Carlo, stability, execution, regimes, ruin, and report
- trade-only degradation and capability-profile signaling
- execution stress, distribution diagnostics, Monte Carlo simulation, ruin diagnostics, OHLCV-gated regime analysis, parameter sweep stability, and report output
- tests covering parsed-artifact engine seam behavior, trade-only degradation, skipped diagnostics, OHLCV regime metrics, parameter stability shape, and richer trade artifacts

Engine gaps to close:

| Requirement | Current Engine State | Required Change | Phase |
| --- | --- | --- | --- |
| Canonical artifact families | Engine artifact kinds are still coarse: trade CSV, artifact bundle, parameter sweep. | Add typed Strategy Truth Room artifacts: trade log, equity curve, broker export, backtest report, strategy config, OHLCV context, benchmark series, parameter sweep, declared claims, and full validation bundle. | Phase 1 |
| Full bundle manifest | Engine can consume parsed artifact input but does not own the full Strategy Truth Room bundle manifest contract. | Add manifest validation or shared contract tests so app and engine agree on file roles, hashes, schema versions, required/optional files, and unlock rules. | Phase 1 |
| Asset-class validation | Normalized trades are broadly asset-class agnostic. | Add explicit asset-class capability profile: crypto, FX, equity, index, futures/CFD where supported; mark unsupported execution details when broker/venue fields are absent. | Phase 1 |
| Assumption Ledger | Assumptions exist as diagnostic strings. | Emit normalized assumptions with source, diagnostic, materiality, confidence, testability, verdict impact, rescue evidence, and share-safe wording. | Phase 2 |
| Unsupported claims | No declared-claim support model. | Add claim objects and evidence mapping: supported, partially supported, unsupported, contradicted, outside scope. | Phase 2 |
| Execution realism | Execution stress exists but is largely deterministic and scenario-based. | Add broker/fill-aware execution audit, fee model provenance, spread/slippage sensitivity schedules, venue/timeframe caveats, and "execution fantasy" verdict triggers. | Phase 3 |
| Cost sensitivity | Fee/slippage stress exists. | Make stress ladders configurable from artifact, broker profile, asset class, and user assumptions; emit break-even friction and cost-kill threshold. | Phase 3 |
| Distribution falsification | Distribution metrics exist. | Add rare-trade dependence, winner concentration, tail asymmetry, liquidity-sensitive outlier handling, and edge-source classification. | Phase 3 |
| Monte Carlo | IID bootstrap Monte Carlo exists. | Add block bootstrap, regime-conditioned resampling when context exists, serial-dependence warning, and simulation model disclosure. | Phase 3 |
| Ruin | Ruin can run with account/risk config. | Make account and sizing assumptions explicit in the Assumption Ledger, avoid advice-like deployment language, and emit capital survival verdicts as validation statements. | Phase 3 |
| Regime | OHLCV-gated technical regimes exist. | Add regime definition metadata, regime coverage sufficiency, thin-sample warnings, optional benchmark-relative regimes, and future extension hooks for macro/event/liquidity regimes. | Phase 3 |
| Parameter stability | Parameter sweep support exists. | Add plateau/cliff topology, sensitivity by parameter, robust-region scoring, optimization target disclosure, and unsupported-sweep reasons. | Phase 3 |
| Proof report payload | Report payload exists. | Emit a report contract with verdict taxonomy, evidence coverage, assumptions, limitations, unsupported claims, diagnostic confidence, artifact hashes, and Research Desk packet hooks. | Phase 5 |
| Verdict taxonomy | Engine verdicts are closer to robust/conditional/fragile/not-ready. | Map to Strategy Truth Room verdicts: structurally credible, promising but under-supported, likely overfit, execution-fantasy, data-insufficient, regime-dependent, untradeable after costs. | Phase 5 |

### App Audit: `invariance_research`

The web app has enough infrastructure to become the product surface, but the UX and data model must now be tightened around falsification.

Strengths already present:

- auth and account provisioning with local SQLite and production Postgres paths
- upload inspection API with CSV and ZIP handling
- generic trade CSV parser, generic bundle parser, semantic validation, and artifact storage
- evidence ledger projection that reconciles artifact eligibility and engine capability profile
- analysis queue, retry path, status polling, and worker runtime
- diagnostic routes for the required core workspaces
- chart adapters and dashboard components
- report page with export actions
- export queue and PDF/Markdown/JSON renderers
- report snapshots, share tokens, share access events, Research Desk requests, reviewer addenda, and admin pages
- entitlement policy and diagnostic lock models

App gaps to close:

| Requirement | Current App State | Required Change | Phase |
| --- | --- | --- | --- |
| Strategy Truth Room intake | New Analysis accepts CSV/ZIP and shows eligibility. | Rebuild as Evidence Intake: artifact classification, file manifest review, unlock matrix, missing evidence prompts, templates, and bundle repair guidance. | Phase 1 |
| Canonical contracts | App has `bundle_v1`, `trade_csv`, `trade_history_bundle`, `backtest_result_bundle`, `research_bundle`. | Align contracts with the artifact schemas in this design doc and the engine models; version all file-level schemas. | Phase 1 |
| Evidence ledger persistence | Upload response returns an evidence ledger projection. | Persist evidence ledger snapshots per artifact and per analysis; display them consistently across workspaces and exports. | Phase 2 |
| Assumption Ledger route | No dedicated route. | Add `/app/analyses/[id]/assumptions` and make it a first-class sidebar item. | Phase 2 |
| Claim inventory | No declared-claim workspace. | Add claim intake, claim extraction from reports/config where possible, unsupported-claim inventory, and report integration. | Phase 2 |
| Workbench IA | Workspaces exist but are uneven in hierarchy, copy, and evidence density. | Redesign all analysis pages into a coherent analyst workbench with shared page anatomy, state blocks, diagnostics, figures, attack panels, and next-evidence prompts. | Phase 4 |
| Sidebar visibility | Core routes exist. | Ensure all diagnostics are always visible; show evidence-limited and plan-locked states without hiding the page. | Phase 4 |
| Analysis Library | Exists as run list. | Upgrade to case library with artifact richness, verdict, report status, share status, Research Desk status, and next-evidence filter. | Phase 4 |
| Report artifact | Export exists but is still mostly renderer-driven. | Build proof report model and visual report surface around verdict, coverage, assumptions, unsupported claims, diagnostic confidence, limitations, appendices, and share policy. | Phase 5 |
| Share Room | Share routes and tokens exist. | Build recipient-facing Share Room with report summary, redaction boundary, token status, expiration, access events, and revoke controls. | Phase 5 |
| Subscription tiers | Entitlements exist but names and gates do not yet match launch packaging. | Update tier model, pricing copy, Stripe mapping, upgrade page, pricing page, billing page, and diagnostic lock messaging. | Phase 6 |
| Research Desk handoff | Requests and addenda exist. | Add request wizard, packet generation, admin triage states, reviewer checklist, client-facing addenda, and status timeline. | Phase 7 |
| Production readiness | Admin ops and schema auto-init exist. | Add migration discipline, startup checks, email deliverability, worker launch docs, storage checks, share security checks, and first-client smoke tests. | Phase 8 |

### Implementation Phases

Each phase below should be implemented as a Codex slice with tests and a short verification note. Phases are ordered to avoid building polished pages on unstable contracts.

### Phase 0: Freeze The Strategy Truth Room Contract

Repo ownership:

- `bulletproof_bt`: diagnostic output contract, engine fixture outputs, version constants
- `invariance_research`: app contract types, intake contracts, report/share contracts
- Both repos: cross-repo fixture pack and contract tests

Purpose:

Create a single contract spine so the app and engine stop drifting. This phase should not chase visual polish. It should define the artifacts, statuses, verdicts, evidence objects, and report payloads that every later phase uses.

Implementation tasks:

- define `strategy_truth_room_contract_version`
- define canonical diagnostic names and page slugs:
  - overview
  - execution
  - distribution
  - monte_carlo
  - ruin
  - regimes
  - stability
  - assumptions
  - report
  - share_room
  - research_desk
  - library
- define canonical artifact families:
  - `trade_log_v1`
  - `equity_curve_v1`
  - `broker_export_v1`
  - `backtest_report_v1`
  - `strategy_config_v1`
  - `ohlcv_context_v1`
  - `benchmark_series_v1`
  - `parameter_sweep_v1`
  - `declared_claims_v1`
  - `strategy_truth_room_bundle_v1`
- define canonical verdict taxonomy:
  - structurally credible
  - promising but under-supported
  - likely overfit
  - execution-fantasy
  - data-insufficient
  - regime-dependent
  - untradeable after costs
- define canonical evidence states:
  - supported
  - limited
  - unsupported
  - contradicted
  - unavailable
  - plan_locked
  - pending_review
- define immutable IDs and hashes required for artifacts, analyses, reports, snapshots, shares, and Research Desk packets
- add fixture files representing:
  - trade-only CSV
  - trade CSV with fees and R-multiples
  - full validation bundle
  - bundle missing OHLCV
  - bundle missing parameter sweep
  - broker export with fills
  - equity-curve-only artifact
  - malformed bundle
- add cross-repo contract tests:
  - app fixture parses into engine-accepted payload
  - engine emits app-accepted diagnostic envelope
  - missing artifact inputs produce evidence-limited, not silent failure
  - unsupported diagnostic remains visible in app navigation
  - report payload includes evidence coverage and limitations

Definition of done:

- both repos expose matching contract version constants
- a fixture from `invariance_research` can be sent through the `bulletproof_bt` bridge and validated against app contracts
- existing tests continue to pass
- contract drift fails tests before reaching UI work

### Phase 1: Artifact Intake And Bundle Manifest

Repo ownership:

- `bulletproof_bt`: expand parsed artifact models, capability profiles, and artifact-aware diagnostic requirements
- `invariance_research`: upload UI, parser routing, manifest validation, storage metadata, artifact templates
- Both repos: fixture and manifest contract tests

Purpose:

Make the product artifact-first in a way a real client can understand. The user should know exactly what was accepted, what each file unlocked, what is missing, and what the system refuses to infer.

Implementation tasks in `bulletproof_bt`:

- expand artifact kind/type models to match the canonical artifact families
- accept optional declared claims, strategy config, broker fills, benchmark series, OHLCV context, equity curve, and parameter sweep metadata
- emit artifact-family-aware capability profiles
- emit file-level required inputs and optional enrichments per diagnostic
- emit asset-class capability statements:
  - crypto supported when symbol, timestamp, quantity, prices, fees, exchange are present
  - FX supported when pair, timestamp, quantity/lot, entry/exit, costs/spread assumptions are present
  - equities/indexes supported when symbol, timestamp, shares/contracts, prices, fees, and market calendar assumptions are present
  - futures/CFDs marked limited unless contract multiplier, tick value, margin, and session data are supplied
- ensure engine does not imply broker-grade execution realism when only trade-level rows are provided

Implementation tasks in `invariance_research`:

- replace generic upload copy with Evidence Intake language
- add artifact-type selector only as a fallback; prefer automatic classification
- add manifest review UI:
  - file list
  - schema version
  - checksum
  - recognized/ignored/rejected
  - diagnostic unlocks
  - missing evidence
  - parser warnings
- add downloadable templates:
  - trade log CSV
  - equity curve CSV
  - broker export mapping guide
  - OHLCV context CSV
  - parameter sweep CSV
  - declared claims JSON
  - full validation bundle ZIP example
- persist file-level artifact metadata and parser provenance
- distinguish artifact rejection, artifact-limited, and plan-limited states
- support equity-curve-only inspection as limited, not as an invalid strategy when possible
- show full-bundle readiness score before run creation

Definition of done:

- a user can upload a trade CSV and see exactly which diagnostics are available, limited, or unavailable
- a user can upload a full bundle and see the full-suite unlock matrix
- a malformed bundle produces repair instructions
- a bundle with extra files does not silently discard them without showing ignored/unsupported status
- engine and app agree on diagnostic unlock status

### Phase 2: Evidence Ledger, Assumption Ledger, And Claim Inventory

Repo ownership:

- `bulletproof_bt`: emit normalized assumptions, evidence facts, and claim-support objects
- `invariance_research`: persist/display ledgers, add Assumption Ledger workspace, add claim inventory UI

Purpose:

Turn the product from "analysis pages" into a truth room. The user should see what assumptions produced the result, what claims the artifact cannot support, and what evidence would change the verdict.

Implementation tasks in `bulletproof_bt`:

- emit `assumption_ledger` entries with:
  - assumption id
  - source: user, parser, engine default, inferred, missing
  - diagnostic
  - statement
  - materiality: low, medium, high, critical
  - confidence
  - falsification test
  - affected metrics
  - verdict impact
  - rescue evidence
  - share-safe wording
- emit `evidence_facts` with:
  - file/source id
  - field/value summary
  - diagnostic relevance
  - provenance
  - confidence
- support `declared_claims_v1`
- emit `claim_inventory` with:
  - claim id
  - claim text
  - source
  - support status
  - supporting diagnostics
  - contradicting diagnostics
  - missing evidence
  - report wording
- add tests for assumptions and claims in trade-only, full-bundle, and missing-context scenarios

Implementation tasks in `invariance_research`:

- persist evidence ledger snapshots on upload and analysis completion
- add `Assumption Ledger` sidebar item
- build Assumption Ledger workspace with:
  - critical assumptions
  - engine defaults
  - user-declared assumptions
  - inferred assumptions
  - contradictions
  - missing evidence
  - "what would rescue this" actions
- add unsupported claims panel to Overview and Report
- add declared-claims intake field/file support
- make assumptions and claims exportable in JSON/PDF
- make Research Desk request prefill with critical assumptions and unsupported claims

Definition of done:

- every completed run has an evidence ledger and assumption ledger
- every report has an unsupported-claims section, even if empty
- Overview answers "what assumptions produced this result?"
- Report answers "what this result does not prove"
- Research Desk packet can be generated from the ledger state

### Phase 3: Engine Diagnostic Hardening

Repo ownership:

- `bulletproof_bt`: primary owner
- `invariance_research`: update mappers, charts, copy, fixtures, and workbench states as engine output expands

Purpose:

Make the diagnostics hostile enough that a serious trader, seller, educator, or allocator would trust the output as due diligence rather than a prettified backtest summary.

Implementation tasks in `bulletproof_bt`:

- Execution:
  - add broker/fill-aware audit when broker export is supplied
  - classify missing fees, suspicious zero-cost fills, impossible fill timestamps, duplicate fills, partial fills, and venue/timezone ambiguity
  - compute break-even cost and break-even slippage
  - emit cost-kill threshold
  - emit execution-fantasy verdict triggers
- Distribution:
  - compute rare-trade dependence
  - compute top-N trade contribution
  - compute winner concentration and loser concentration
  - flag strategies where edge comes from too few trades
  - separate gross vs net distribution when fees exist
- Monte Carlo:
  - add IID bootstrap disclosure
  - add block bootstrap mode
  - add regime-conditioned bootstrap when OHLCV/regimes exist
  - emit path survival, drawdown envelope, time-under-water, and model limitations
- Ruin:
  - require explicit capital and risk assumptions for full confidence
  - separate validation language from investment advice
  - emit sizing fragility and survivability status
- Regime:
  - emit regime definition metadata
  - warn on thin samples per regime
  - emit regime dominance and adverse-regime decay
  - add benchmark-relative regime hooks
- Parameter Stability:
  - parse parameter sweep files into parameter surfaces
  - compute robust region, cliff risk, plateau width, local optimum dependence, and optimization target risk
  - emit stability confidence only when sweep coverage is sufficient
- Report:
  - emit diagnostic confidence per section
  - emit proof-report payload with assumptions, unsupported claims, evidence coverage, and artifact provenance

Implementation tasks in `invariance_research`:

- update bridge types and mappers for new diagnostic fields
- add charts/instruments for:
  - cost-kill threshold
  - rare-trade dependence
  - Monte Carlo survival envelope
  - regime dominance
  - parameter plateau/cliff map
  - assumption materiality
- add regression tests so missing engine fields degrade cleanly

Definition of done:

- engine can falsify a strategy under worse fills, higher fees, rare-trade removal, adverse regimes, Monte Carlo path stress, ruin assumptions, and parameter perturbation
- app displays every expanded diagnostic without raw JSON leakage
- each diagnostic produces a verdict-driving statement, not only charts

### Phase 4: Analyst Workbench IA And Page Redesign

Repo ownership:

- `invariance_research`: primary owner
- `bulletproof_bt`: minor support for output naming and payload consistency

Purpose:

Make the dashboard the core sellable product. The user should feel they are inside a rigorous analyst workbench, not a generic metrics dashboard.

Shared page anatomy:

- verdict strip:
  - current diagnostic posture
  - evidence state
  - artifact dependency
  - plan state if locked
- attack question:
  - one direct falsification question the page answers
- evidence instruments:
  - metrics, charts, scenarios, and tables presented as instruments
- assumption and limitation rail:
  - source-linked assumptions
  - material limitations
  - unsupported claims
- next evidence:
  - what to upload or request next
- report impact:
  - how this page changes the final memo

Page-specific implementation:

- Overview:
  - truth-room verdict
  - credibility score
  - evidence coverage
  - strongest support
  - strongest doubt
  - unsupported claims
  - next experiment
- Execution:
  - execution realism audit
  - cost/slippage sensitivity
  - broker/fill anomalies
  - cost-kill threshold
  - execution-fantasy triggers
- Distribution:
  - payoff anatomy
  - rare-trade dependence
  - outlier removal sensitivity
  - winner concentration
  - gross vs net behavior
- Monte Carlo:
  - simulation model disclosure
  - survival envelope
  - drawdown burden
  - path dependence
  - regime-aware mode when available
- Ruin:
  - capital/risk assumptions
  - ruin probability
  - sizing fragility
  - survival under reduced/increased risk
  - non-advisory language
- Regime:
  - regime definition
  - coverage sufficiency
  - favorable/adverse regime split
  - regime concentration
  - missing context state
- Parameter Stability:
  - sweep coverage
  - robust plateau
  - cliff zones
  - local optimum dependence
  - missing sweep state
- Assumption Ledger:
  - critical assumptions
  - engine defaults
  - inferred assumptions
  - user-declared assumptions
  - contradictions
  - rescue evidence
- Report:
  - proof memo preview
  - evidence coverage
  - limitations
  - unsupported claims
  - assumptions
  - export/share CTA
- Share Room:
  - recipient-safe report view
  - redaction notice
  - access state
  - snapshot identity
- Research Desk:
  - request scope
  - packet contents
  - status timeline
  - reviewer addenda
- Library:
  - case list
  - artifact richness
  - verdict
  - evidence gaps
  - report/share status
  - Research Desk status

Definition of done:

- all required pages exist and are reachable from navigation
- locked or evidence-limited pages are visible and useful
- no completed-analysis page uses generic "card dump" structure as its dominant pattern
- all pages answer a falsification question
- all pages show what evidence is missing

### Phase 5: Proof Report, Export, And Share Room

Repo ownership:

- `bulletproof_bt`: proof-report payload and verdict inputs
- `invariance_research`: report UI, snapshot service, export renderer, Share Room, redaction/access controls

Purpose:

Make the report the demand object. A user should pay because the memo is defensible, shareable, and precise about its limits.

Implementation tasks in `bulletproof_bt`:

- emit proof-report sections:
  - executive verdict
  - artifact identity
  - evidence coverage
  - assumptions
  - unsupported claims
  - diagnostic confidence
  - falsification results
  - limitations
  - next evidence
  - Research Desk recommended scope
- emit report-safe language that avoids investment advice
- map engine verdicts to Strategy Truth Room taxonomy

Implementation tasks in `invariance_research`:

- version report snapshots with:
  - analysis id
  - artifact ids and hashes
  - report schema version
  - generated timestamp
  - redaction policy
  - included diagnostics
  - excluded diagnostics and reasons
- rebuild PDF export around proof-report structure
- add Markdown and JSON export parity
- add report preview state before export
- add snapshot regeneration and superseded-state behavior
- build Share Room:
  - token creation
  - expiration
  - revoke
  - recipient view
  - report download policy
  - access-event log
  - private fields redaction
- implement Report Sharing Threat Model from the design doc:
  - no raw trade files in public share by default
  - no PII in share payload
  - no owner account ids exposed
  - snapshot cannot mutate after share creation
  - revoked tokens fail closed
  - expired tokens fail closed

Definition of done:

- a completed run can generate a polished PDF
- the PDF contains evidence coverage, assumptions, unsupported claims, limitations, and next evidence
- a share link renders a recipient-safe report
- revoked/expired shares cannot be accessed
- report snapshots are immutable enough for client trust

### Phase 6: Launch Subscription And Entitlement Model

Repo ownership:

- `invariance_research`: primary owner
- `bulletproof_bt`: no major work unless diagnostics require plan-aware output labels

Purpose:

Make pricing match value without corrupting evidence truth. Evidence limitations and subscription limitations must remain separate.

Implementation tasks:

- update account plan ids or add mapping layer for launch packaging:
  - Free
  - Individual
  - Pro
  - Team
  - Research Desk
- align pricing page, upgrade page, billing page, Stripe checkout, webhook mapping, account state, and diagnostic locks
- update entitlement matrix:
  - uploads per month
  - file size
  - bundle support
  - full diagnostic access
  - export access
  - share links
  - retention
  - Research Desk request eligibility
  - seats
  - priority processing
- preserve visible locked pages
- add quota and plan-copy tests
- add Stripe test-mode path for each paid tier
- add admin override for first-client pilots

Launch-tier target:

| Tier | Target Buyer | Price | Core Rights |
| --- | --- | --- | --- |
| Free | Curious trader or first-time evaluator | $0 | limited trade CSV runs, Overview/Distribution/Monte Carlo/Ruin preview, no export, short retention |
| Individual | Serious self-directed trader | $49-$79/month | trade CSV and basic bundles, full report export, core diagnostics, limited shares |
| Pro | strategy seller, educator, researcher | $149-$249/month | full automated suite when artifacts support it, parameter/regime access, share links, longer retention |
| Team | prop desk, small fund, research group | $499-$999/month | seats, team library, shared reports, admin controls, higher limits, priority processing |
| Research Desk | deep validation buyer | project-based, from $1,500 | human/agent-assisted review, addenda, execution/data QA, benchmark construction, claim formalization |

Definition of done:

- every locked diagnostic explains whether the lock is evidence-based, plan-based, or both
- checkout works for all paid tiers in test mode
- first-client accounts can be provisioned without manual database edits
- pricing matches the product promises in this doc

### Phase 7: Research Desk Handoff And First-Client Ops

Repo ownership:

- `invariance_research`: request workflow, admin operations, addenda, client state
- `bulletproof_bt`: Research Desk packet payload and diagnostic artifacts where needed

Purpose:

Create the upgrade path from automated validation to deeper review. Research Desk should not be generic consulting. It should be a structured continuation of the evidence gaps the product already found.

Implementation tasks:

- add Research Desk packet generator:
  - report snapshot
  - artifact manifest
  - evidence ledger
  - assumption ledger
  - unsupported claims
  - diagnostic outputs
  - requested questions
  - client notes
- build request wizard from Report and Assumption Ledger pages
- add request scopes:
  - execution audit
  - data quality audit
  - benchmark construction
  - parameter stability review
  - regime/context review
  - claim validation
  - investor/buyer memo review
- add admin workflow:
  - received
  - scoped
  - quoted
  - in_review
  - addendum_draft
  - approved
  - delivered
  - closed
- add reviewer addendum approval policy
- add client-facing timeline
- add email notifications
- add manual first-client playbook

Definition of done:

- a user can request Research Desk from a specific report limitation
- admin can see the full packet and move the request through states
- approved addendum attaches to a report snapshot
- the product can sell a high-touch review without pretending the automated run proved more than it did

### Phase 8: Reliability, Security, And Deployment Hardening

Repo ownership:

- `invariance_research`: primary owner
- `bulletproof_bt`: engine runtime reliability, deterministic outputs, packaging
- Both repos: smoke fixtures and release gates

Purpose:

Make the system safe enough for a first external client using real strategy evidence.

Implementation tasks in `invariance_research`:

- formalize migration path instead of relying only on startup schema initialization
- document local SQLite setup and production Postgres setup
- add worker startup script and health checks
- verify object storage for artifacts, exports, and snapshots
- verify email flows:
  - signup
  - verification if enabled
  - password reset
  - Research Desk request
  - share notification if added
- add retention policy and cleanup jobs
- add audit logs for:
  - uploads
  - report exports
  - share creation/revoke/access
  - Research Desk packet access
  - admin addenda
- add rate limits for upload, export, share access, and Research Desk request
- add share-token brute-force protection
- add production smoke route or admin health panel for:
  - database
  - queue
  - worker heartbeat
  - engine bridge
  - storage
  - email
  - Stripe

Implementation tasks in `bulletproof_bt`:

- make engine package/runtime path explicit for the web app
- add deterministic seed tests for all major diagnostics
- add runtime failure modes with structured error envelopes
- add performance benchmarks for large trade files and full bundles
- ensure warnings and limitations are emitted rather than swallowed

Definition of done:

- local SQLite test path works
- production Postgres path works
- analysis worker and export worker can be launched with documented commands
- first-client smoke test can run from upload through share link
- failure states are visible, retryable, or intentionally terminal

### Phase 9: First-Client Beta Protocol

Repo ownership:

- Both repos

Purpose:

Prove the product can handle a real client before calling Approach A sellable.

Beta scope:

- 3 to 5 serious users
- 10 to 20 artifact uploads
- at least 3 full validation bundles
- at least 2 broker/export files
- at least 2 parameter sweeps
- at least 2 Research Desk request simulations
- at least 2 external share recipients

First-client acceptance checklist:

- user can create account and sign in
- user can upload trade CSV
- user can upload structured bundle
- user can inspect artifact sufficiency before running analysis
- all workbench pages are visible
- unavailable pages explain missing evidence
- plan-locked pages explain subscription lock separately from evidence lock
- Overview gives a clear credibility verdict
- Execution explains what happens when fills/costs worsen
- Distribution explains rare-trade dependence
- Monte Carlo explains survival under path stress
- Ruin explains capital/risk assumptions
- Regime explains context dependence or missing context
- Parameter Stability explains sweep support or missing sweep
- Assumption Ledger shows assumptions and rescue evidence
- Report states what the artifact does not prove
- Export produces a polished PDF
- Share Room renders a recipient-safe snapshot
- Research Desk request can be submitted from a limitation
- admin can see requests, exports, shares, and worker status
- production deployment can run without missing tables
- local test deployment can run without production Postgres

Readiness metrics:

- 95% of valid trade CSV uploads inspect successfully
- 90% of full bundles produce the expected unlock matrix
- 0 hidden failed diagnostics on completed analyses
- 0 share links expose raw private artifacts by default
- 0 report exports omit limitations when diagnostics are limited
- median analysis runtime acceptable for first-client use
- all first-client failures produce a rescue path or admin-visible error

Exit criteria:

- at least one real user exports and shares a report without assistance
- at least one real user changes or abandons a strategy because of the output
- at least one real user asks for the next experiment or Research Desk review from an evidence limitation
- support burden is low enough that the next 10 users can be onboarded without custom database or code intervention

### Recommended Codex Implementation Order

The next implementation slices should follow this order:

1. Phase 0 contract freeze and fixture harness.
2. Phase 1 artifact intake and full bundle manifest.
3. Phase 2 evidence ledger, assumption ledger, and unsupported claims.
4. Phase 3 engine diagnostic hardening, starting with execution realism and distribution concentration.
5. Phase 4 workbench redesign page-by-page, starting with Overview and Execution.
6. Phase 5 proof report, export, and Share Room.
7. Phase 6 subscription and entitlement alignment.
8. Phase 7 Research Desk handoff.
9. Phase 8 reliability hardening.
10. Phase 9 first-client beta protocol.

This order matters. The app should not spend another pass polishing workbench pages before the artifact contract, evidence ledger, assumption ledger, and claim inventory are stable enough to power the product truthfully.

## Moving From Approach A To Full Ambition

Approach A should produce an excellent validation product. The full ambition emerges by adding research operating system concepts only when users need them.

### Stage 1: Artifact Validation

User action:

- upload evidence
- get validation
- export report

Product object:

- artifact
- analysis
- report

This is the current Strategy Robustness Lab path.

### Stage 2: Strategy Workspace

User action:

- group multiple analyses under a strategy
- compare versions
- see whether a revised version improved evidence quality

Product object:

- strategy
- project
- analysis lineage

Why this is next:

Users who return with multiple uploads need organization before they need a research OS.

### Stage 3: Market Claim Capture

User action:

- state the claim before running validation
- define what would disprove it
- attach assumptions and expected edge

Product object:

- market claim
- invalidation condition
- assumption

Important UX rule:

Do not make claim capture mandatory on first upload. Introduce it as "make this report stronger" after the user sees how unsupported claims weaken the report.

### Stage 4: Hypothesis And Experiment Planning

User action:

- convert claim into a formal hypothesis
- define required data
- define train/test or discovery/validation split
- run parameter or regime tests

Product object:

- hypothesis
- validation plan
- experiment group
- run manifest

Reuse from `bulletproof_bt`:

- `src/bt/hypotheses`
- `src/bt/experiments`
- `orchestrator/run_experiment_pipeline.py`
- `orchestrator/research_daemon.py`

### Stage 5: Tenant-Scoped Research Memory

User action:

- ask "where have I seen this before?"
- retrieve similar failures
- see hostile regimes across prior runs
- compare strategy variants

Product object:

- research finding
- state bucket
- candidate
- recommendation
- reviewer annotation

Reuse from `bulletproof_bt`:

- `orchestrator/research_memory/trade_memory.py`
- `orchestrator/research_memory/state_memory.py`
- `orchestrator/research_memory/candidate_memory.py`
- `orchestrator/research_memory/recommendation_engine.py`
- `orchestrator/research_memory/query_engine.py`

Required product changes:

- move research memory from local SQLite patterns to tenant-scoped Postgres tables
- add as-of timestamps
- add dataset version references
- prevent cross-user leakage
- keep recommendations evidence-only
- never let memory auto-approve deployment

### Stage 6: Full Claim-First Research OS

User action:

- design, validate, fork, reject, and remember strategies through a governed research process

Product object:

- claim graph
- hypothesis lineage
- experiment lineage
- evidence ledger
- research memory
- report library
- reviewer workflow

This is the 10-star product. It should be earned by the wedge, not built before the wedge works.

## Target Architecture

```text
User
  |
  v
invariance_research Next.js App
  |-- public authority site
  |-- Strategy Robustness Lab
  |-- authenticated workspace
  |-- upload inspection
  |-- diagnostic pages
  |-- report/export pages
  |-- billing/entitlements
  |-- admin/research desk
  |
  v
TypeScript API + Services
  |-- upload intake
  |-- analysis creation
  |-- entitlement policy
  |-- queue records
  |-- export records
  |-- report renderer
  |-- product contracts
  |
  v
Worker Runtime
  |-- analysis worker
  |-- export worker
  |-- health/heartbeat
  |
  v
Python Bridge
  |-- versioned request envelope
  |-- timeout/failure classification
  |-- probe command
  |
  v
bulletproof_bt
  |-- bt.run_analysis_from_parsed_artifact
  |-- StrategyRobustnessLabService
  |-- diagnostics
  |-- artifact contracts
  |-- benchmarks
  |-- hypothesis contracts
  |-- research memory
  |
  v
Storage
  |-- Postgres metadata
  |-- S3/R2 uploads and exports
  |-- benchmark manifests and datasets
  |-- immutable derived artifacts
```

## Contract Boundaries

### Web App Owns

- accounts and users
- auth and sessions
- plans and entitlements
- upload envelopes
- artifact ownership
- analysis/job lifecycle
- benchmark selection UI
- report/export lifecycle
- admin and research desk workflows
- product-safe errors
- public positioning
- user-facing contracts

### Engine Owns

- diagnostic computation
- strategy/backtest execution
- execution assumptions
- robustness metrics
- Monte Carlo assumptions
- benchmark alignment logic
- artifact bundle definitions
- hypothesis-contract execution
- research-memory algorithms

### Shared Contract

The shared boundary should be small and versioned:

```text
ParsedArtifactInput
AnalysisRunConfig
EngineAnalysisResult
DiagnosticCapabilityProfile
EngineRunContext
DiagnosticPayload
ReportSourcePayload
```

The web app should never import engine internals. The engine should never know about web app sessions, Stripe plans, or React routes.

## Diagnostic Honesty Model

The product should always distinguish four states.

### Available

The artifact and engine support the diagnostic.

Example:

- trade distribution from trade-level CSV
- Monte Carlo by trade resampling when enough trades exist
- benchmark comparison when benchmark data overlaps the strategy period

### Limited

The system can compute a bounded proxy but cannot support a strong conclusion.

Example:

- regime comments from timestamps but no OHLCV
- execution stress using generic costs when venue-specific fee/spread data is absent
- ruin estimate without position sizing context

### Unavailable

The data cannot support the diagnostic.

Example:

- parameter stability without parameter sweep data
- MFE/MAE without market path around trades
- execution realism without fills, costs, spread, latency, or order assumptions

### Locked

The plan does not include the diagnostic.

Locked is not the same as unavailable. This distinction is central to trust.

## Product Surface Redesign

### Public `/robustness-lab`

Current surface exists and should stay.

Redesign priorities:

- replace generic "research instrument" copy with artifact-to-report specificity
- make the first CTA "Upload strategy evidence"
- show three concrete artifact types: trade CSV, structured bundle, research bundle
- preview the validation report, not just diagnostic categories
- include the sentence "We may tell you the data does not support your claim"
- route serious users to Research Desk without making the product feel manual-only

### `/app/new-analysis`

Current surface exists and should be upgraded.

Needed layout:

1. Upload artifact.
2. Inspection result.
3. Diagnostic eligibility.
4. Benchmark/runtime settings.
5. Run analysis.

Critical details:

- show accepted file docs inline
- show sample file downloads
- show upload limitations before upload
- show plan restrictions only after artifact class detection
- preserve current benchmark selector but explain why benchmark overlap matters
- add strategy name, claim field, and "what decision are you trying to make?" later

### `/app/analyses/[id]/overview`

Needed layout:

- verdict banner
- evidence sufficiency score
- trust/doubt columns
- unsupported conclusions
- benchmark status
- report CTA
- Research Desk CTA when limits matter

### Diagnostic Pages

Each page should answer:

- what was tested
- what the result says
- what evidence supports it
- what assumptions matter
- what is missing
- what to do next

Pages should not be chart-first.

### Report Page

The report is the saleable artifact.

It should have:

- reading mode
- export buttons
- sharing rules
- limitation appendix
- reproducibility appendix
- "request human review" action

## Data Model Additions For Approach A

Minimum additions or hardening:

Persistence performance requirements:

- index `report_snapshots(analysis_id, created_at DESC)` for owner report history
- index the current report snapshot lookup, either with `report_snapshots(analysis_id, superseded_by_report_id)` or an explicit current pointer
- index `evidence_ledger_snapshots(analysis_id, created_at DESC)`
- index share token lookup by token hash, never plaintext token
- index share access events by `share_id, created_at DESC`
- index expired share/report cleanup by `expires_at` and status
- retention jobs delete or archive expired share access events without deleting immutable report snapshots needed for owner audit history
- Postgres and SQLite migrations stay schema-compatible until production provider is chosen

```text
analysis_engine_context
  analysis_id
  engine_name
  engine_version
  seam_version
  adapter_version
  parser_version
  benchmark_config_snapshot
  degraded
  degradation_reasons
  created_at

diagnostic_capabilities
  analysis_id
  diagnostic
  status
  reason
  required_inputs
  optional_enrichments
  source

report_snapshots
  report_id
  analysis_id
  report_version
  source_hash
  rendered_json_key
  rendered_md_key
  rendered_pdf_key
  created_at
```

Likely next additions:

```text
strategies
  strategy_id
  account_id
  name
  description
  created_at

market_claims
  claim_id
  strategy_id
  plain_english
  asset_universe
  timeframe
  expected_edge
  invalidation_conditions
  assumptions
  created_at
```

Do not add cross-user research memory tables until tenant-scoped memory has clear product behavior.

## Testing Strategy

### `bulletproof_bt`

Add or preserve:

- seam probe test for `run_analysis_from_parsed_artifact`
- SaaS model fixture tests
- diagnostic eligibility tests
- parameter sweep bundle tests
- incomplete artifact tests
- non-finite artifact payload tests
- benchmark availability tests
- deterministic report payload snapshots

### `invariance_research`

Add or preserve:

- upload inspect tests by artifact class
- plan-lock versus unavailable tests
- analysis creation tests
- worker success/failure tests
- engine bridge failure classification tests
- `map-engine-analysis-record.ts` fixture tests
- report/export snapshot tests
- admin retry tests
- Postgres repository parity tests where production depends on Postgres
- public page smoke tests for `/robustness-lab`, `/strategy-validation`, `/pricing`, and `/research-desk`

### Cross-Repo Contract Tests

Add a small shared fixture pack:

```text
fixtures/engine-seam/
  trade_csv_basic.json
  trade_csv_limited.json
  structured_bundle_full.json
  parameter_sweep_full.json
  malformed_engine_payload.json
```

Use it in both repos:

- engine repo proves it emits the fixture shape
- web repo proves it maps the fixture shape

### Engineering Test Coverage Map

The wedge acceptance matrix must be implemented as layered tests, not only product acceptance bullets. Slice 1 cannot be considered complete until the cross-repo contract and evidence ledger paths are covered by deterministic fixtures.

```text
CODE PATHS                                                     USER FLOWS
[+] bulletproof_bt seam emission                               [+] Upload inspection
  |-- [GAP][UNIT] EngineEnvelopeV1 fields                         |-- [GAP][INTEGRATION] Trade CSV only -> available/limited/unavailable
  |      tests/test_saas_engine_envelope.py                       |      src/__tests__/evidence/upload-ledger-flow.test.ts
  |-- [GAP][UNIT] malformed/non-finite payload fails closed        |-- [GAP][INTEGRATION] Plan lock overlays evidence state
  |      tests/test_saas_engine_envelope.py                       |      src/__tests__/evidence/entitlement-overlay.test.ts
  |-- [GAP][CONTRACT] canonical fixture pack emitted              |-- [GAP][E2E] Validation packet template unlocks expected diagnostics
         tests/test_engine_seam_fixtures.py                             tests/validation-packet-templates.test.ts

[+] invariance_research seam adapter                            [+] Analysis completion
  |-- [GAP][CONTRACT] EngineEnvelopeV1 accepted                    |-- [GAP][INTEGRATION] worker creates ledger snapshot before report snapshot
  |      tests/engine-envelope-contract.test.ts                    |      tests/analysis-ledger-report-flow.test.ts
  |-- [GAP][CONTRACT] unknown diagnostic fails closed              |-- [GAP][INTEGRATION] engine skipped diagnostic preserves reason
  |      tests/engine-envelope-contract.test.ts                    |      tests/analysis-ledger-report-flow.test.ts
  |-- [GAP][SNAPSHOT] fixture -> AnalysisRecord projection         |-- [GAP][INTEGRATION] LLM contradiction cannot override ledger
         src/__tests__/analysis/engine-fixture-mapping.test.ts           tests/llm-ledger-authority.test.ts

[+] EvidenceLedgerService                                       [+] Report generation
  |-- [GAP][UNIT] supported/limited/unsupported/contradicted       |-- [GAP][INTEGRATION] completed analysis -> immutable report snapshot
  |      src/__tests__/evidence/evidence-ledger-service.test.ts    |      tests/report-snapshot-state-machine.test.ts
  |-- [GAP][UNIT] entitlement overlay does not rewrite evidence    |-- [GAP][INTEGRATION] retry/rerun supersedes or marks stale
  |      src/__tests__/evidence/entitlement-overlay.test.ts        |      tests/report-snapshot-state-machine.test.ts
  |-- [GAP][SNAPSHOT] report/share/LLM projections                 |-- [GAP][INTEGRATION] double-click export/share is idempotent
         src/__tests__/evidence/evidence-projections.test.ts             tests/report-snapshot-state-machine.test.ts

[+] Share-safe projection                                       [+] Shared report access
  |-- [GAP][UNIT] SharedReportViewModel allowlist                  |-- [GAP][E2E] active token renders report-safe payload only
  |      tests/share-report-threat-model.test.ts                   |      tests/share-report-threat-model.test.ts
  |-- [GAP][UNIT] sensitive fields redacted                         |-- [GAP][E2E] expired/revoked/superseded token fails closed
  |      tests/share-report-threat-model.test.ts                   |      tests/share-report-threat-model.test.ts
  |-- [GAP][UNIT] share route cannot call artifact download API     |-- [GAP][E2E] raw upload URL inaccessible from share context
         tests/share-report-threat-model.test.ts                         tests/share-report-threat-model.test.ts
```

Coverage targets for Slice 1:

- `bulletproof_bt`: 100% branch coverage for `EngineEnvelopeV1` construction, malformed payload handling, unsupported diagnostics, and fixture serialization.
- `invariance_research`: 100% branch coverage for `EvidenceLedgerService`, entitlement overlays, engine envelope validation, and adapter fail-closed behavior.
- Cross-repo fixtures: every fixture in `fixtures/engine-seam/` must be consumed by tests in both repos.

Coverage targets for Slice 2:

- report snapshot generation, regeneration, stale/superseded behavior, idempotency, export rendering from snapshots, and invalid transitions.

Coverage targets for Slice 3:

- share token lifecycle, least-privilege projection, redaction, expiry, revocation, superseded report behavior, and no raw artifact access.

## Product Metrics

Track:

- upload attempt count
- upload rejection reasons
- accepted artifact classes
- diagnostics available per analysis
- diagnostics limited per analysis
- diagnostics unavailable per analysis
- analysis completion rate
- engine failure rate
- time to completed report
- report export rate
- Research Desk request rate
- upgrade CTA click from locked diagnostics
- return uploads per strategy/account
- percentage of reports with unsupported conclusions
- percentage of users who upload richer artifacts after seeing limitations

The most important early metric:

> How often does a user take action after the product says "your data does not support that conclusion"?

## Risks And Mitigations

### Risk: Users Want Confirmation, Not Truth

Mitigation:

- target credibility-seeking users first
- make shareable reports the value, not positive verdicts
- sell "defensible evidence" rather than "better backtests"

### Risk: Current Report Is Not Premium Enough

Mitigation:

- improve report content contract before visual polish
- make limitations and reproducibility appendix excellent
- use Research Desk as premium path while automation improves

### Risk: Artifact Messiness Overwhelms Intake

Mitigation:

- make parser failures useful
- publish templates
- show exact missing fields
- treat inspection as a product experience

### Risk: Engine/App Contracts Drift

Mitigation:

- version seam payloads
- add golden fixtures
- test adapters against real engine outputs
- fail closed on unknown diagnostics

### Risk: Research Memory Creates Privacy Problems

Mitigation:

- tenant-scoped memory first
- no cross-user learning by default
- explicit opt-in aggregation only after legal/product review
- keep recommendations evidence-only

### Risk: Execution Realism Claims Outrun Data

Mitigation:

- display execution assumptions clearly
- grade realism instead of claiming binary validity
- require venue/fill/cost context for strong execution conclusions

## Do Not Build First

- live trading
- broker execution
- public strategy marketplace
- social feed
- public leaderboards
- fully automated strategy generation
- natural-language compiler as first surface
- portfolio allocator
- cross-user intelligence
- complex visual market-structure explorer

These can exist later. They do not prove the wedge.

## The Assignment

Before broadening the platform, collect ten real artifact bundles from the exact first customers:

- two serious independent traders
- two strategy sellers or educators
- two systematic traders
- two crypto-native researchers
- two emerging fund, allocator, or prop-style operators

For each bundle, record:

- what they uploaded
- what claim they wanted supported
- what the product could support
- what the product could not support
- which diagnostic mattered most
- whether they exported the report
- whether they would share the report
- whether they would pay for the report
- whether they requested deeper review
- what richer artifact they could provide next

This is the demand test. If users pay for or share negative/conditional reports, the product has real pull. If they only want confirmation, reposition toward strategy sellers, allocators, and serious research teams.

## Final Recommendation

Build the full ambition, but do it through the existing app.

First:

> Make `invariance_research` Strategy Robustness Lab the best artifact-to-validation-report product available.

Then:

> Add claim capture, strategy workspaces, hypothesis planning, and tenant-scoped research memory.

Eventually:

> Become the claim-first strategy research OS.

The sequence matters. The existing app is not a throwaway prototype. It is the right starting surface. The job now is to make it honest, durable, report-worthy, and commercially sharp.
