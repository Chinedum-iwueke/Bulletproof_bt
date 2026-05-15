##ChatGPT Context

Below is a full structured report of the product concept we developed in this thread: the raw user ideas, the refined product interpretations, the architecture suggestions, the feature set, and the implementation logic.

⸻

Full Product Report: AI-Native Quantitative Backtesting, Hypothesis Validation, and Visual Market Structure Intelligence Platform

1. Core Product Vision

The product is not merely a backtesting web app.

The strongest formulation is:

An AI-native research operating system that transforms vague trading ideas, visual chart intuition, and discretionary strategy concepts into falsifiable, execution-realistic, historically validated quantitative research.

It sits at the intersection of:
	•	quantitative backtesting
	•	AI strategy interpretation
	•	market data infrastructure
	•	execution-aware simulation
	•	visual chart-reading intelligence
	•	research memory
	•	hypothesis management
	•	analog retrieval
	•	decision support

The product should be positioned as:

The first AI-native research environment for execution-realistic strategy validation.

Or more sharply:

From intuition to audited backtest.

Or:

Turn trading ideas into evidence.

The goal is to make the product as valuable for financial backtesting and quantitative research as ChatGPT or Claude Code are for coding, but more specialized, more rigorous, and better suited to the needs of traders and quantitative researchers.

⸻

2. Raw User Ideas Raised in This Thread

2.1 Wrapping bulletproof_bt as a web app

The user asked:

If I want to wrap the entire bulletproof_bt and all its capabilities as a web app, what would the process be like?

Key raw concerns:
	•	bulletproof_bt currently runs locally.
	•	It currently backtests on local OHLCV crypto data.
	•	The user wants a web app where users can run backtests on assets of their choosing.
	•	The core question: how would the app provide historical data instantly?
	•	What is the standard/best practice for making data available?

2.2 Venue-specific data

The user questioned why users must select a venue:

Why do they have to select Binance spot / Bybit perpetual / Coinbase spot?

Then asked:

If users have to select different venues, does that mean I’ll own pre-ingested normalized data for each supported venue?

Raw idea:
	•	A serious web app must support instruments by venue.
	•	The user realized that if venue matters, then the platform likely needs its own normalized historical data store for each venue.

2.3 Natural-language strategy-to-backtest agent

The user proposed:

A layer where a user can explain the strategy they want to backtest in simple English terms, no matter how simple, and an AI/LLM converts it to the actual accurate strategy they are explaining, guiding them step by step, turning it into a testable hypothesis using bulletproof_bt hypothesis wrappers, giving it back for confirmation, and actually running the backtest.

Raw idea:
	•	user describes strategy in simple English
	•	AI interprets
	•	AI guides step by step
	•	AI converts into formal strategy
	•	AI uses bulletproof_bt hypothesis wrappers
	•	AI returns interpretation for user confirmation
	•	system runs backtest

2.4 Question about training/fine-tuning

The user asked whether this would require extra training data, or whether existing open-source LLM/NLP frameworks are enough.

Raw concern:
	•	Is a custom-trained model necessary?
	•	Can existing LLMs and NLP frameworks perform strategy interpretation well enough?
	•	What should be trained later, if anything?

2.5 Standard for an AI financial backtesting agent

The user asked:

What would be the standard for an AI agent in the financial backtesting space?

Raw concern:
	•	Not just “can it generate strategies?”
	•	What should a serious, professional, trustworthy AI financial research agent actually do?
	•	How should it behave?
	•	How controlled/auditable should it be?

2.6 12-month build plan

The user asked for:

A concrete 12-month build plan with exact epics, APIs, schemas, and milestone deliverables.

Raw requirement:
	•	Take the product idea from concept to implementation.
	•	Include detailed phases.
	•	Include APIs.
	•	Include data schemas.
	•	Include milestones.

2.7 Crypto-only vs multi-asset reach

The user asked:

Streamlining to only crypto doesn’t that shorten my reach? Or is that the best way to go, as wider markets have way more users than crypto?

Raw concern:
	•	Crypto is easier technically.
	•	Broader markets have larger user bases.
	•	Should the product start crypto-only or immediately support equities, FX, futures, etc.?

2.8 Aristotle first-principles deconstruction

The user requested a rigorous first-principles breakdown:
	•	Phase 1: Assumption autopsy
	•	Phase 2: Irreducible truths
	•	Phase 3: Reconstruction from zero
	•	Phase 4: Assumption vs truth map
	•	Phase 5: The Aristotelian move

Raw goal:
	•	Rebuild the product from first principles.
	•	Avoid inherited assumptions from competitors and industry norms.
	•	Discover features users do not know they need yet.
	•	Make the product stronger than conventional backtesting tools.

2.9 Visual chart-reading AI layer

The user proposed:

Imagine how a chartist reads price action visually: candlestick patterns, shoulders, support/resistance zones, visual patterns. Could a tool take a screenshot of a market position and analyze visually whether an entry makes sense?

Raw concept:
	•	A screenshot-based chart reader.
	•	It interprets candlestick structure visually.
	•	It detects support/resistance, shoulders, patterns, visual location.
	•	It decides whether an entry at a point makes sense.
	•	It could use visual pattern recognition rather than only timestamped OHLCV rows.
	•	The user asked what technology would be required.
	•	The user asked whether this could lead to discovery.
	•	The user asked whether anything like this already exists.

2.10 Decision-support accuracy

The user asked:

Can the best product formulation and stronger features users don’t know they need be built accurately enough to be valuable analysis and powerful decision support?

Raw concern:
	•	Not just a toy.
	•	Can it actually provide reliable decision support?
	•	Can it avoid hallucinating visual edge?
	•	Can it be accurate, calibrated, and useful?

2.11 Historical analog retrieval for visual recommendations

The user asked:

When it says “good entry,” can it pull up over 100 same visual patterns that occurred in the past to show its recommendation is based on enough times the same pattern appeared?

Raw idea:
	•	If the system says “good entry,” it must show evidence.
	•	It should retrieve 100+ similar visual setups from history.
	•	It should show outcome distributions.
	•	Training could be forced toward always finding similar patterns.
	•	The system should learn faster by comparing the current chart to many historical analogs.

2.12 Live chart screenshot analysis

The user uploaded a TradingView-style screenshot of BTCUSDT perpetual on Bybit, 5-minute chart, and asked:

What do you see from this chart? What is the chart telling us? What are likely entry points or exit points?

Raw goal:
	•	See how the assistant would interpret a real chart.
	•	Understand the reasoning process.
	•	Translate this kind of reasoning into the product.

2.13 Borrowing the assistant’s chart-reading method

The user observed that the assistant had produced a structured chart interpretation and asked:

Since you already do that, can I not borrow your already working system and use it in my own web app?

Raw idea:
	•	Can the internal chart-reading method be used directly?
	•	Can the product replicate the assistant’s useful output?
	•	How can the assistant’s process be formalized into the user’s own system?

2.14 Product spec for visual chart-reading process

The user asked for:

Your exact chart-reading process as a product spec with VisualChartState, VisualSetupClass, DecisionAssessment, scoring rules, LLM prompt template, and roadmap to build mine to have the same useful outcomes in my own UI.

Raw requirement:
	•	Formalize chart reading into product objects.
	•	Include schemas.
	•	Include scoring rules.
	•	Include explanation prompt.
	•	Include roadmap.
	•	Build a system that produces similar useful analysis, but in the user’s UI.

⸻

3. Refined Product Definition

The product should be understood as a layered system with several interconnected modules.

3.1 Main product identity

The strongest product identity is:

A financial research OS that transforms market ideas into falsifiable claims, validates them under realistic execution assumptions, and compounds knowledge across experiments.

It should not be framed as just:
	•	“AI trading assistant”
	•	“strategy generator”
	•	“backtesting app”
	•	“screenshot chart analyzer”

Those framings are too shallow.

The stronger framing is:

An AI-native evidence engine for trading strategy research.

3.2 Core philosophical shift

The key conceptual shift:

Conventional framing

“Create a strategy and backtest it.”

Refined framing

“Define a falsifiable market claim, simulate decisions under constraints, diagnose the outcome, and learn from the failure/success.”

The unit of the product should not be the “strategy.”

The unit of the product should be the:

Falsifiable claim.

A strategy is merely one implementation of a claim.

⸻

4. The Product’s Major Pillars

4.1 AI natural-language strategy-to-hypothesis engine

Raw user idea:

User explains a strategy in simple English, and the AI converts it into an accurate testable hypothesis using bulletproof_bt wrappers.

Refined implementation:

The system should not directly convert English into executable code.

Instead:

English → StrategyIntentSpec → HypothesisDraft → CompiledBacktestRequest → validation → user confirmation → run

The LLM’s job is not to be the execution engine.

The LLM’s job is to:
	•	parse intent
	•	detect ambiguity
	•	ask clarifying questions
	•	produce structured strategy specs
	•	explain the translated hypothesis
	•	suggest missing assumptions
	•	help frame falsifiers

The deterministic compiler’s job is to:
	•	map the structured spec into supported bulletproof_bt hypothesis wrappers
	•	reject unsupported logic
	•	enforce no-lookahead rules
	•	insert explicit defaults
	•	produce run configs

The validator’s job is to:
	•	verify completeness
	•	verify data availability
	•	verify engine compatibility
	•	verify causal correctness
	•	surface assumptions and risks

4.2 Data platform

Raw user idea:

How will users instantly run backtests on any asset? Do I need pre-ingested normalized data for each supported venue?

Refined implementation:

The product needs a venue-specific, normalized, versioned market data system.

Best practice:
	•	do not fetch exchange candles directly inside every user backtest
	•	pre-ingest hot datasets for supported venues/assets
	•	use on-demand cold fetch for less common assets
	•	cache and normalize new requests
	•	allow BYO-data upload for unsupported assets
	•	record data provenance, version, coverage, gaps, and validation status

Data layers:
	1.	raw layer
Exact upstream payloads preserved.
	2.	normalized layer
Converted to canonical schema.
	3.	curated layer
Validated, deduplicated, monotonic, backtest-ready.
	4.	derived layer
Resampled data, benchmark series, features, visual setup records.

4.3 Execution-aware backtesting layer

The system wraps bulletproof_bt as the trusted simulation engine.

Core principles:
	•	no lookahead
	•	deterministic runs
	•	venue-specific execution assumptions
	•	fees
	•	spread
	•	slippage
	•	latency/delay
	•	worst-case intrabar assumptions
	•	realistic fills
	•	strict data contracts
	•	reproducible artifacts

Execution tiers remain important:
	•	Tier 1: structural clean
	•	Tier 2: realistic
	•	Tier 3: stress

4.4 Hypothesis and claim engine

The first-principles refinement was that the product should force users to define claims before testing.

A test should require:
	•	claim
	•	mechanism
	•	falsifiers
	•	expected condition of failure
	•	required data
	•	execution assumptions
	•	acceptance criteria
	•	diagnostics to inspect

This prevents vague trading ideas from being tested as if they were rigorous strategies.

4.5 Visual Market Structure Intelligence

Raw user idea:

Take chart screenshots and analyze them like a chartist.

Refined implementation:

The system should not be a generic “AI looks at chart and gives signal” tool.

It should be:

Visual Market Structure Intelligence: screenshot + chart structure parser + OHLCV context + analog retrieval + historical outcome validation.

The visual system should:
	•	parse chart screenshot
	•	identify market structure
	•	detect zones
	•	classify setup type
	•	assess entry quality
	•	propose confirmation/invalidation conditions
	•	retrieve historical analogs
	•	show outcome distributions
	•	translate the visual setup into a formal testable hypothesis

4.6 Historical analog evidence engine

Raw user idea:

If it says good entry, it should pull up over 100 similar patterns.

Refined implementation:

Every recommendation should be grounded in an analog set where available.

Not “same pattern” in a superficial sense.

Similarity should combine:
	•	visual similarity
	•	structural similarity
	•	regime similarity
	•	location similarity
	•	volatility similarity
	•	trend context similarity
	•	entry-location similarity

The product should retrieve:
	•	top 20 visual analogs for UI inspection
	•	100+ analogs internally for statistical grounding if available
	•	outcome distributions
	•	failure modes
	•	what separated winners from losers

The recommendation should be:

“This setup is structurally similar to 146 prior setups. In comparable conditions, 61% reached +1R before -1R. Direct entries into resistance underperformed; breakout-confirmation entries performed better.”

Not:

“Good entry.”

4.7 Research memory and lineage

Every experiment should become part of a research graph.

The system should remember:
	•	what the user tested
	•	which variants failed
	•	which parameter neighborhoods were fragile
	•	which setups worked only under specific regimes
	•	which assumptions destroyed edge
	•	what next experiment was suggested
	•	what the user actually ran next

This creates compounding research intelligence.

4.8 AI research co-pilot

Beyond running tests, the system should behave like a disciplined junior quant researcher.

It should:
	•	critique strategies
	•	identify missing assumptions
	•	propose falsification tests
	•	design next experiments
	•	diagnose result fragility
	•	compare variants
	•	generate research memos
	•	warn against overfitting
	•	enforce rigor

⸻

5. Architecture Summary

The product should not be one model or one app. It should be a coordinated system.

5.1 Top-level system architecture

Core layers:
	1.	Frontend UI
Strategy intake, chart upload, review/confirmation, result dashboards.
	2.	API backend
Authentication, projects, jobs, strategy specs, run metadata.
	3.	AI orchestration layer
Intent parser, clarifier, hypothesis drafter, result explainer, research planner.
	4.	Strategy ontology and compiler
Deterministic translation from structured specs to bulletproof_bt configs.
	5.	Validation layer
Completeness, no-lookahead, data support, execution support, strategy support.
	6.	Data service
Venue-specific normalized historical data, symbol master, data manifests.
	7.	Backtest worker system
Queue-based bulletproof_bt execution.
	8.	Artifact storage
Trades, equity, decisions, fills, performance, diagnostics, reports.
	9.	Diagnostic engine
Regime buckets, MFE/MAE, EV decomposition, fragility, analog evidence.
	10.	Research graph
Hypothesis lineage, experiment memory, failure taxonomy.
	11.	Visual intelligence system
Screenshot parsing, setup classification, analog retrieval, decision assessment.

⸻

6. Strategy-to-Hypothesis AI Layer

6.1 Raw user concept

The user wants:
	•	a user to explain strategy in English
	•	AI guides them step by step
	•	AI translates to accurate strategy
	•	AI adopts bulletproof_bt hypothesis wrappers
	•	AI gives it back for confirmation
	•	AI runs the backtest

6.2 Refined workflow

Step 1: User natural-language input

Example:

“Buy pullbacks in strong trends and exit when momentum fades.”

Step 2: AI parses the idea

The system identifies:
	•	strategy family: trend continuation
	•	direction: likely long-only unless clarified
	•	entry concept: pullback
	•	filter: strong trend
	•	exit: momentum fade
	•	missing items:
	•	timeframe
	•	venue/symbol
	•	stop logic
	•	take-profit logic
	•	execution tier
	•	exact momentum definition

Step 3: Clarification flow

The system asks targeted questions:
	•	Which market?
	•	Which venue?
	•	Which timeframe?
	•	What defines strong trend?
	•	What defines pullback?
	•	What invalidates the trade?
	•	Should this be long-only, short-only, or both?
	•	Should the system use ATR stop, swing stop, or time stop?

Step 4: Generate StrategyIntentSpec

Structured output is created.

Step 5: Generate HypothesisDraft

The system writes:
	•	claim
	•	mechanism
	•	rationale
	•	falsifiers
	•	expected behavior
	•	diagnostics to inspect

Step 6: Compile into bulletproof_bt wrapper

A deterministic compiler maps the spec into a supported wrapper.

Step 7: Validate

The validator checks:
	•	whether all required fields exist
	•	whether strategy is causal
	•	whether data exists
	•	whether indicators are supported
	•	whether execution assumptions are valid
	•	whether there is any lookahead risk

Step 8: Preview and confirmation

The system displays:
	•	plain-English interpretation
	•	exact strategy rules
	•	user-specified fields
	•	inferred fields
	•	defaulted fields
	•	execution assumptions
	•	data version
	•	expected diagnostics

The user confirms before execution.

Step 9: Run backtest

The run is queued and executed by bulletproof_bt workers.

Step 10: Explain results

The AI explains:
	•	what happened
	•	why it worked/failed
	•	what regimes mattered
	•	whether edge survived costs
	•	next experiments

⸻

7. Core Schemas

7.1 StrategyIntentSpec

Purpose:

Represents the user’s strategy idea in structured form before compilation.

Example:

{
  "schema_version": "1.0",
  "intent_id": "intent_uuid",
  "source": {
    "type": "natural_language",
    "raw_text": "Buy pullbacks in strong trends and exit when momentum fades"
  },
  "strategy_family": "trend_continuation",
  "market": {
    "asset_class": "crypto",
    "venue": "bybit",
    "symbol": "BTCUSDT",
    "instrument_type": "perpetual",
    "timeframe": "5m"
  },
  "direction": "long_only",
  "entry_logic": {
    "type": "pullback_continuation",
    "trend_definition": {
      "indicator": "ema",
      "fast_period": 20,
      "slow_period": 50,
      "condition": "fast_above_slow"
    },
    "pullback_definition": {
      "reference": "ema20",
      "max_distance_atr": 0.5
    },
    "confirmation": {
      "type": "bullish_close",
      "condition": "close_above_previous_high"
    }
  },
  "filters": [
    {
      "type": "trend_strength",
      "indicator": "adx",
      "period": 14,
      "threshold": 25
    }
  ],
  "risk": {
    "stop_model": {
      "type": "atr_frozen",
      "period": 14,
      "multiple": 2.0
    },
    "take_profit_model": {
      "type": "r_multiple",
      "multiple": 2.0
    },
    "position_sizing": {
      "type": "fixed_risk",
      "risk_per_trade_pct": 1.0
    }
  },
  "execution": {
    "tier": "tier_2",
    "order_type": "market",
    "signal_delay_bars": 1
  },
  "ambiguities": [],
  "defaults_applied": [
    {
      "field": "execution.signal_delay_bars",
      "value": 1,
      "reason": "Tier 2 realistic execution default"
    }
  ]
}

7.2 HypothesisDraft

Purpose:

Turns the strategy into a falsifiable research claim.

{
  "hypothesis_id": "hyp_uuid",
  "title": "Volatility-supported EMA pullback trend continuation",
  "claim": "In an established uptrend, shallow pullbacks to EMA20 followed by bullish confirmation produce positive net expectancy under realistic execution.",
  "mechanism": "Momentum persistence and order-flow continuation after temporary mean reversion.",
  "market_rationale": "Crypto trends can persist after leverage-driven repricing, especially when volatility remains supportive.",
  "falsifiers": [
    "Net expectancy becomes negative under Tier 2 execution.",
    "Performance is concentrated in fewer than 5 outlier trades.",
    "Results collapse outside the highest volatility regime.",
    "MAE exceeds expected stop behavior before favorable excursion."
  ],
  "required_data": [
    "OHLCV",
    "fees",
    "spread proxy",
    "ATR",
    "ADX"
  ],
  "diagnostics_required": [
    "EV gross/net",
    "fee drag",
    "MFE/MAE in R",
    "performance by volatility bucket",
    "holding duration distribution",
    "Tier 3 stress survival"
  ],
  "acceptance_criteria": {
    "min_trades": 100,
    "ev_net_r_min": 0.05,
    "max_drawdown_limit": 0.25,
    "tier3_ev_net_r_min": 0.0
  }
}

7.3 CompiledBacktestRequest

Purpose:

The executable request generated after validation.

{
  "run_id": "run_uuid",
  "project_id": "project_uuid",
  "compiled_from": {
    "intent_id": "intent_uuid",
    "hypothesis_id": "hyp_uuid",
    "compiler_version": "0.1.0"
  },
  "engine": {
    "name": "bulletproof_bt",
    "version": "0.1.0"
  },
  "strategy": {
    "wrapper_name": "l1_trend_pullback",
    "params": {
      "ema_fast": 20,
      "ema_slow": 50,
      "adx_period": 14,
      "adx_threshold": 25,
      "atr_period": 14,
      "stop_atr_multiple": 2.0,
      "take_profit_r": 2.0
    }
  },
  "data_spec": {
    "asset_class": "crypto",
    "venue": "bybit",
    "symbol": "BTCUSDT",
    "instrument_type": "perpetual",
    "timeframe": "5m",
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-12-31T23:59:59Z",
    "data_version": "bybit_btcusdt_5m_v2026_01_15"
  },
  "execution_profile": {
    "tier": "tier_2",
    "fees": "taker",
    "slippage": "atr_proportional",
    "spread": "modeled",
    "signal_delay_bars": 1,
    "intrabar_policy": "worst_case"
  },
  "outputs_required": [
    "trades.csv",
    "equity.csv",
    "performance.json",
    "decisions.jsonl",
    "fills.jsonl",
    "diagnostics.json"
  ]
}

7.4 RunProvenance

Purpose:

Every result must be reproducible.

{
  "run_id": "run_uuid",
  "created_at": "2026-05-14T00:00:00Z",
  "user_id": "user_uuid",
  "engine_version": "0.1.0",
  "strategy_wrapper": "l1_trend_pullback",
  "strategy_params_hash": "sha256_hash",
  "data_version": "bybit_btcusdt_5m_v2026_01_15",
  "data_manifest_hash": "sha256_hash",
  "execution_profile": "tier_2",
  "compiler_version": "0.1.0",
  "validator_version": "0.1.0",
  "ai_model_used_for_draft": "model_name",
  "user_confirmed": true
}


⸻

8. APIs

8.1 Strategy AI APIs

POST /ai/parse-strategy

Input:

{
  "text": "Buy pullbacks in strong trends and exit when momentum fades."
}

Output:

{
  "strategy_family": "trend_continuation",
  "confidence": 0.91,
  "detected_components": {
    "entry": "pullback",
    "filter": "strong_trend",
    "exit": "momentum_fade"
  },
  "missing_fields": [
    "market",
    "timeframe",
    "stop_model",
    "exact_trend_definition"
  ]
}

POST /ai/clarify-strategy

Input:

{
  "intent_id": "intent_uuid",
  "missing_fields": ["timeframe", "stop_model"]
}

Output:

{
  "questions": [
    {
      "field": "timeframe",
      "question": "Which timeframe should define the signal?",
      "options": ["1m", "5m", "15m", "1h", "4h"]
    },
    {
      "field": "stop_model",
      "question": "How should the trade be invalidated?",
      "options": ["ATR stop", "swing low/high", "time stop", "opposite signal"]
    }
  ]
}

POST /ai/generate-spec

Returns StrategyIntentSpec.

POST /ai/generate-hypothesis

Returns HypothesisDraft.

POST /ai/explain-results

Input:

{
  "run_id": "run_uuid",
  "diagnostics": {}
}

Output:

{
  "verdict": "Strategy showed positive gross edge but failed under realistic execution.",
  "key_findings": [
    "Most gains came from high-volatility regimes.",
    "Fee and slippage drag reduced EV by 70%.",
    "The rule performed poorly during range-bound periods."
  ],
  "next_experiments": [
    "Add volatility percentile gate.",
    "Test stricter trend filter.",
    "Stress test on Bybit volatile universe."
  ]
}

8.2 Compiler and validation APIs

POST /compiler/compile

Input:

{
  "strategy_intent_spec": {}
}

Output:

{
  "compiled": true,
  "compiled_request": {},
  "warnings": []
}

POST /validator/validate-strategy

Output:

{
  "valid": false,
  "errors": [
    {
      "field": "risk.stop_model",
      "message": "Stop model is undefined.",
      "suggestion": "Choose ATR-based stop or swing-based invalidation."
    }
  ],
  "warnings": [
    {
      "field": "market.data",
      "message": "Funding data not available for selected period."
    }
  ]
}

8.3 Market data APIs

GET /data/coverage

Params:
	•	venue
	•	symbol
	•	timeframe
	•	start
	•	end

Output:

{
  "available": true,
  "coverage_pct": 99.7,
  "start": "2024-01-01T00:00:00Z",
  "end": "2024-12-31T23:59:59Z",
  "gap_count": 14,
  "data_version": "bybit_btcusdt_5m_v2026_01_15",
  "quality_status": "eligible"
}

GET /symbols/search

Input query:

BTC

Output:

[
  {
    "asset_id": "btc",
    "venue": "bybit",
    "symbol": "BTCUSDT",
    "instrument_type": "perpetual",
    "label": "BTCUSDT Perpetual - Bybit"
  },
  {
    "asset_id": "btc",
    "venue": "binance",
    "symbol": "BTCUSDT",
    "instrument_type": "spot",
    "label": "BTCUSDT Spot - Binance"
  }
]

8.4 Backtest APIs

POST /runs

Creates a run.

Input:

{
  "compiled_backtest_request": {}
}

Output:

{
  "run_id": "run_uuid",
  "status": "queued"
}

GET /runs/{run_id}

Returns status and metadata.

GET /runs/{run_id}/results

Returns performance and diagnostics.

GET /runs/{run_id}/artifacts

Returns artifact links.

⸻

9. Data Platform Details

9.1 Why venue selection matters

The user questioned why the user must select venue.

The refined explanation:

There is no universal “BTC price.”

There is:
	•	BTCUSDT on Binance spot
	•	BTCUSDT perpetual on Bybit
	•	BTC-USD on Coinbase
	•	BTC perpetual on other venues

Each has different:
	•	liquidity
	•	spread
	•	fees
	•	funding
	•	price path
	•	contract type
	•	listing history
	•	execution behavior

A backtest is only valid when tied to a tradable instrument.

Therefore, the app should internally require:
	•	venue
	•	symbol
	•	instrument type
	•	timeframe
	•	data version

UX can hide complexity by letting user search “BTC” and then showing instrument choices.

9.2 Data ownership

The user asked whether the platform needs pre-ingested normalized data per venue.

Refined answer:

Yes, for supported venues and instruments.

But not everything upfront.

Use three coverage layers:

Hot data

Pre-ingested, validated, instant.

Examples:
	•	BTCUSDT Bybit perp
	•	ETHUSDT Bybit perp
	•	BTCUSDT Binance spot
	•	ETHUSDT Binance spot
	•	SOLUSDT
	•	top liquid assets

Warm data

On-demand fetch and cache.

If user requests an asset not in hot set:
	•	fetch from upstream
	•	normalize
	•	validate
	•	store
	•	run
	•	cache for future

BYO data

For unsupported assets:
	•	user uploads CSV/Parquet
	•	system maps columns
	•	validates
	•	runs backtest

9.3 Data storage layout

Suggested Parquet layout:

/market_data/
  raw/
    crypto/
      bybit/
        BTCUSDT/
          1m/
            2024/
              01/
  normalized/
    crypto/
      bybit/
        BTCUSDT/
          1m/
  curated/
    crypto/
      bybit/
        BTCUSDT/
          1m/
            year=2024/
              month=01/
  derived/
    visual_setups/
    benchmarks/
    features/

9.4 Symbol master schema

{
  "asset_id": "btc",
  "canonical_asset": "BTC",
  "venue": "bybit",
  "venue_symbol": "BTCUSDT",
  "instrument_type": "perpetual",
  "quote_asset": "USDT",
  "base_asset": "BTC",
  "status": "active",
  "listed_at": "2020-01-01T00:00:00Z",
  "delisted_at": null,
  "tick_size": 0.5,
  "lot_size": 0.001,
  "funding_supported": true,
  "open_interest_supported": true
}

9.5 Data manifest schema

{
  "data_version": "bybit_btcusdt_1m_v2026_01_15",
  "venue": "bybit",
  "symbol": "BTCUSDT",
  "instrument_type": "perpetual",
  "timeframe": "1m",
  "start": "2024-01-01T00:00:00Z",
  "end": "2026-01-15T00:00:00Z",
  "row_count": 1080000,
  "missing_bar_count": 23,
  "duplicate_count": 0,
  "monotonic": true,
  "source": "bybit_api",
  "ingested_at": "2026-01-15T03:00:00Z",
  "validation_status": "eligible",
  "checksum": "sha256_hash"
}


⸻

10. AI Agent Standard in Financial Backtesting

The refined standard:

The agent should behave like a junior quant researcher under strict supervision, not a creative trading guru.

It should be:
	•	structured
	•	auditable
	•	falsifiable
	•	conservative
	•	transparent
	•	validation-first
	•	execution-aware

It should not:
	•	generate arbitrary strategy code and run it
	•	hide assumptions
	•	pretend uncertainty does not exist
	•	optimize for exciting ideas
	•	overstate edge
	•	skip data quality checks

10.1 Required agent behavior

The agent should:
	1.	Translate vague intent into precise testable structure.
	2.	Ask clarification questions.
	3.	Detect missing assumptions.
	4.	Reject unsupported strategies.
	5.	Enforce causality.
	6.	Produce hypothesis drafts.
	7.	Surface all defaults.
	8.	Require user confirmation.
	9.	Launch runs only after validation.
	10.	Explain results and propose next experiments.

10.2 Agent components

Suggested multi-agent decomposition:
	1.	Intent Parser Agent
	2.	Clarification Agent
	3.	Hypothesis Drafting Agent
	4.	Compiler Controller Agent
	5.	Validator Explainer Agent
	6.	Result Interpreter Agent
	7.	Research Planner Agent
	8.	Adversarial Critic Agent

⸻

11. First-Principles Product Reconstruction

The user requested an Aristotelian first-principles deconstruction.

11.1 Assumptions identified

The product space inherits assumptions such as:
	•	backtesting is about testing strategies
	•	strategies are code
	•	users know what they mean
	•	performance metrics equal edge
	•	more indicators equal better strategy
	•	more data always helps
	•	AI should generate strategies
	•	user flexibility is always good
	•	all asset classes should be supported early
	•	visual chart reading is subjective and not quantifiable
	•	a good backtest means a good idea
	•	isolated tests are enough
	•	if an idea works once, it may be edge

11.2 Irreducible truths

The refined first principles:
	1.	Markets are sequences of transactions under constraints.
	2.	Price is an outcome, not a primitive.
	3.	A trading idea is a claim about future outcome distributions.
	4.	A claim is only useful if falsifiable.
	5.	A backtest is a simulation of decisions under constraints.
	6.	Simulation output is only as valid as its assumptions.
	7.	Most trading ideas are under-specified.
	8.	Edge is conditional advantage under constraints.
	9.	Performance metrics summarize outcomes but do not explain mechanism.
	10.	Knowledge compounds only when experiments are connected.
	11.	Truth emerges from structured falsification, not idea generation.
	12.	Human intuition is useful but must be formalized.

11.3 Reconstructed product approaches

Approach 1: Claim Engine

Product is a claim validation engine.

Flow:
	•	user states belief
	•	system formalizes claim
	•	system defines falsifiers
	•	system generates minimal test
	•	system generates adversarial test
	•	system measures claim validity

Features:
	•	Claim Strength Index
	•	Falsification Coverage Score
	•	Mechanism Confidence Layer

Approach 2: Decision Simulator

Product simulates decisions under uncertainty.

Flow:
	•	define what information is available at each step
	•	simulate decisions
	•	simulate alternatives not taken
	•	measure decision efficiency

Features:
	•	Counterfactual Engine
	•	Decision Efficiency Score
	•	Path Dependency Analysis

Approach 3: Research Operating System

Product compounds knowledge.

Flow:
	•	every experiment becomes a node
	•	variants are linked
	•	failures are categorized
	•	robust descendants are tracked

Features:
	•	Research Graph
	•	Edge Persistence Tracker
	•	Failure Pattern Library
	•	Strategy Evolution Tree

11.4 The Aristotelian Move

The highest-leverage move:

Make the fundamental unit of the product a falsifiable claim, not a strategy or backtest.

This means the UI should not start with:

“Create strategy.”

It should start with:

“Define claim.”

Every run requires:
	•	claim
	•	mechanism
	•	falsifiers
	•	data requirements
	•	execution assumptions
	•	acceptance criteria

⸻

12. Features Suggested Throughout the Thread

12.1 Claim and hypothesis features

Hypothesis Lock System

User cannot run a test without:
	•	claim
	•	mechanism
	•	falsifiers

Claim Strength Index

Measures how well-defined and testable a claim is.

Inputs:
	•	specificity
	•	causal clarity
	•	falsifiers
	•	data availability
	•	measurable acceptance criteria

Falsification Coverage Score

Measures how thoroughly the idea has been attacked.

Examples:
	•	tested under Tier 2?
	•	tested under Tier 3?
	•	tested across regimes?
	•	tested across assets?
	•	tested against benchmark?
	•	tested with alternate exits?
	•	tested for outlier dependence?

Mechanism Confidence Layer

Separates:
	•	strategy produced profit
	•	mechanism likely worked
	•	result was likely regime luck
	•	result was likely outlier-driven

Claim-to-Experiment Generator

Given a claim, automatically produces:
	•	minimal test
	•	stress test
	•	adversarial test
	•	robustness test
	•	portability test

12.2 Backtesting and diagnostics features

Edge Decomposition Engine

Separates edge contribution into:
	•	signal contribution
	•	execution contribution
	•	regime contribution
	•	volatility contribution
	•	sizing contribution
	•	outlier contribution

Reality Distortion Meter

Measures gap between:
	•	naive backtest
	•	realistic execution
	•	stress execution

Example:

Naive EV: +0.24R
Tier 2 EV: +0.04R
Tier 3 EV: -0.08R
Reality Distortion: High

Fee/Slippage Drag Analysis

Shows how much performance is destroyed by:
	•	fees
	•	spread
	•	slippage
	•	signal delay
	•	pessimistic intrabar assumptions

MAE/MFE Engine

Records:
	•	max favorable excursion
	•	max adverse excursion
	•	realized R
	•	exit efficiency
	•	whether +1R happened before -1R
	•	whether target was hit before stop

Robustness Heatmaps

Show performance across:
	•	parameter values
	•	volatility buckets
	•	regime states
	•	time periods
	•	symbols
	•	venues

Outlier Dependence Detector

Flags if edge comes from:
	•	one trade
	•	one day
	•	one asset
	•	one regime
	•	one volatility burst

Strategy Readiness Ladder

Classifies strategy as:
	1.	Idea
	2.	Testable
	3.	Backtested
	4.	Robustness checked
	5.	Execution stressed
	6.	Cross-market checked
	7.	Deployment candidate

12.3 Research memory features

Research Graph

Nodes:
	•	claims
	•	hypotheses
	•	runs
	•	variants
	•	diagnostics
	•	decisions
	•	conclusions

Edges:
	•	derived_from
	•	invalidated_by
	•	improved_by
	•	contradicted_by
	•	stress_test_of
	•	variant_of

Strategy Evolution Tree

Shows how an idea evolved from raw intuition into tested variants.

Failure Pattern Library

Classifies recurring failures:
	•	fee-sensitive
	•	spread-sensitive
	•	low-trade illusion
	•	single-outlier dependence
	•	regime-only edge
	•	parameter fragility
	•	overfit
	•	execution failure
	•	data illusion

Edge Persistence Tracker

Tracks whether a claim remains valid across:
	•	time
	•	assets
	•	venues
	•	regimes
	•	execution tiers
	•	data revisions

Weekly Research Memo

AI-generated summary:
	•	what was tested
	•	what failed
	•	what improved
	•	strongest current hypotheses
	•	what should be tested next

12.4 AI co-pilot features

Adversarial Critic Mode

A second agent attacks:
	•	hidden assumptions
	•	lookahead risks
	•	overfitting
	•	regime dependence
	•	data insufficiency
	•	execution optimism

Next Experiment Generator

After every run, suggests:
	•	simpler version
	•	stricter filter
	•	alternate exit
	•	stress test
	•	falsification test
	•	cross-asset test

AI Research Memo Generator

Creates reports in formats:
	•	executive memo
	•	technical memo
	•	investor memo
	•	PM memo
	•	audit memo

Assumption Audit Panel

Shows:
	•	user-specified assumptions
	•	AI-inferred assumptions
	•	system defaults
	•	untested assumptions
	•	unavailable data

⸻

13. Visual Market Structure Intelligence

13.1 Raw user concept

The user wanted a tool that could:
	•	receive a chart screenshot
	•	read price action visually like a chartist
	•	identify candlestick patterns
	•	identify shoulders
	•	identify support and resistance
	•	identify visual zones
	•	decide whether an entry makes sense
	•	potentially discover patterns not captured by raw OHLCV rows

13.2 Refined product formulation

The strongest formulation is:

A system that interprets visual market structure, evaluates entry quality, grounds it in historical analogs, and converts discretionary chart intuition into testable quantitative knowledge.

It is not:

“Upload chart and get signal.”

It is:

“Upload chart and get structured market-state interpretation, entry-quality assessment, invalidation logic, historical analog evidence, and testable setup classification.”

13.3 Visual Market Structure Engine pipeline

Screenshot
→ chart parser
→ VisualChartState
→ VisualSetupClass
→ DecisionAssessment
→ analog retrieval
→ outcome distribution
→ explanation
→ UI overlays
→ optional VisualHypothesisDraft
→ bulletproof_bt test

13.4 VisualChartState schema

Purpose:

The structured representation of what is visible on the chart.

{
  "schema_version": "1.0",
  "chart_id": "chart_uuid",
  "source": {
    "type": "screenshot",
    "platform": "tradingview",
    "symbol_text_detected": "BTCUSDT Perpetual",
    "venue_text_detected": "Bybit",
    "timeframe_text_detected": "5m"
  },
  "trend": {
    "direction": "uptrend",
    "strength": 0.78,
    "evidence": [
      "higher highs",
      "higher lows",
      "strong impulse from lower range"
    ]
  },
  "structure": {
    "phase": "consolidation",
    "prior_phase": "impulse_continuation",
    "impulse_strength": 0.84,
    "compression": {
      "active": true,
      "tightness": 0.72,
      "description": "tight sideways movement after impulse"
    }
  },
  "zones": {
    "resistance": [
      {
        "low": 78150,
        "high": 78300,
        "strength": 0.8,
        "reason": "multiple local rejections"
      }
    ],
    "support": [
      {
        "low": 77800,
        "high": 77900,
        "strength": 0.7,
        "reason": "local consolidation floor"
      },
      {
        "low": 77250,
        "high": 77350,
        "strength": 0.9,
        "reason": "previous breakout base"
      }
    ]
  },
  "location": {
    "relative_to_structure": "top",
    "distance_to_nearest_resistance_pct": 0.15,
    "distance_to_nearest_support_pct": 0.35,
    "entry_location_quality": "poor_without_confirmation"
  },
  "volatility": {
    "state": "contracting",
    "score": 0.66
  },
  "momentum": {
    "state": "weakening_after_impulse",
    "score": 0.52
  },
  "volume": {
    "state": "spike_on_impulse_then_fade",
    "interpretation": "impulse participation followed by consolidation"
  }
}

13.5 VisualSetupClass schema

Purpose:

Classifies the chart state into a tradable setup category.

{
  "setup_id": "setup_uuid",
  "setup_class": "compression_under_resistance",
  "bias": "bullish_conditional",
  "stage": "developing",
  "confidence": 0.74,
  "description": "Post-impulse bullish consolidation beneath local resistance.",
  "valid_long_paths": [
    "breakout_close_above_resistance",
    "pullback_to_support_and_reclaim"
  ],
  "valid_short_paths": [
    "failed_breakout_reclaim_back_inside_range"
  ],
  "invalid_conditions": [
    "breakdown below local support",
    "failed reclaim after breakout attempt"
  ]
}

Setup taxonomy v1:
	•	impulse_breakout
	•	trend_continuation
	•	compression_under_resistance
	•	compression_above_support
	•	range_mid_noise
	•	support_retest
	•	resistance_retest
	•	failed_breakout_candidate
	•	exhaustion_extension
	•	breakout_retest
	•	liquidity_trap_candidate
	•	late_stage_extension
	•	pullback_to_value
	•	distribution_range
	•	accumulation_range

13.6 DecisionAssessment schema

Purpose:

Turns setup classification into actionable decision support.

{
  "assessment_id": "assessment_uuid",
  "entry_quality_now": 0.42,
  "state": "no_trade",
  "primary_message": "The chart is at a decision zone, not a clean entry location.",
  "recommended_actions": [
    "wait_for_breakout_confirmation",
    "wait_for_pullback_to_support",
    "avoid_mid_range_entry"
  ],
  "long_plan": {
    "status": "conditional",
    "best_trigger": "5m candle close above 78200-78300 resistance zone with follow-through",
    "alternative_trigger": "pullback to 77800-77900 support followed by bullish reclaim",
    "entry_zone": [78200, 78300],
    "invalidation": 78000,
    "target_zones": [
      [78800, 79000]
    ],
    "reason": "Trend is bullish, but current price is under resistance and requires confirmation."
  },
  "short_plan": {
    "status": "conditional",
    "best_trigger": "breakout above resistance followed by immediate rejection back below 78000",
    "entry_zone": [77900, 78000],
    "invalidation": 78250,
    "target_zones": [
      [77800, 77900],
      [77250, 77350]
    ],
    "reason": "Short is only valid as a failed breakout/liquidity trap, not as random resistance shorting."
  },
  "avoid_conditions": [
    "buying inside range without breakout",
    "shorting resistance without failure confirmation"
  ],
  "confidence": 0.68
}

13.7 Scoring rules

Entry quality should not be arbitrary.

Suggested formula:

entry_quality =
    trend_alignment * 0.25
  + structure_quality * 0.25
  + location_quality * 0.25
  + volatility_alignment * 0.15
  + confirmation_quality * 0.10

Trend alignment

High if:
	•	higher highs/higher lows
	•	strong directional impulse
	•	price holding above prior breakout base

Structure quality

High if:
	•	clean compression
	•	clear zones
	•	clean impulse before consolidation
	•	low chop

Location quality

High if:
	•	near support for long
	•	after confirmed breakout/retest
	•	near invalidation with favorable R:R

Low if:
	•	mid-range
	•	directly under resistance before breakout
	•	late after extension

Volatility alignment

High if:
	•	compression likely to expand
	•	volatility not chaotic
	•	range tightening

Confirmation quality

High if:
	•	breakout close
	•	retest hold
	•	reclaim
	•	failure confirmation for shorts

Penalties

if price is under resistance and no breakout:
    entry_quality -= 0.25 to 0.35

if price is mid-range:
    entry_quality -= 0.20

if setup is late after impulse:
    entry_quality -= 0.15

if analog count is low:
    confidence -= 0.20

if similar setups have mixed outcomes:
    confidence -= 0.10 to 0.30

Classification thresholds

0.00 - 0.44 = no_trade
0.45 - 0.64 = conditional
0.65 - 0.79 = favorable
0.80 - 1.00 = high_quality

13.8 Explanation prompt template

The LLM should not improvise from raw image alone.

It should receive structured data.

System prompt:

You are a professional market structure analyst.

You are given structured chart-state data produced by a chart parser and decision engine.

Your job is to explain the chart clearly and practically.

Rules:
1. Do not invent levels that are not in the input.
2. Do not claim certainty.
3. Explain structure before recommendation.
4. Separate what is happening now from what would trigger a trade.
5. Identify bad entries clearly.
6. Identify conditional long and short plans.
7. State invalidation logic.
8. Use direct language.
9. Do not provide guaranteed predictions.
10. If the system says no-trade, explain why waiting is the decision.

Input:

{
  "visual_chart_state": {},
  "visual_setup_class": {},
  "decision_assessment": {},
  "analog_evidence": {}
}

Output format:

1. Market structure
2. Current phase
3. Key zones
4. What the chart is telling us
5. What not to do
6. Long setup conditions
7. Short setup conditions
8. Final decision
9. Evidence summary


⸻

14. Historical Visual Analog Engine

14.1 Raw user idea

The user suggested that when the system says “good entry,” it should retrieve over 100 similar past patterns to prove the recommendation.

14.2 Refined principle

The system should not retrieve analogs to “prove itself right.”

It should retrieve analogs to ground and calibrate the recommendation.

It must include:
	•	winners
	•	losers
	•	ambiguous cases
	•	edge cases

The analog set should be honest.

14.3 HistoricalVisualSetupRecord schema

{
  "record_id": "visual_setup_uuid",
  "market": {
    "asset_class": "crypto",
    "venue": "bybit",
    "symbol": "BTCUSDT",
    "instrument_type": "perpetual",
    "timeframe": "5m"
  },
  "timestamp": "2025-04-12T14:35:00Z",
  "image": {
    "screenshot_uri": "s3://visual-setups/...",
    "render_config": {
      "theme": "dark",
      "candles_visible": 120,
      "indicators": ["volume"]
    }
  },
  "visual_embedding": [0.123, 0.456],
  "structural_features": {
    "trend_direction": "uptrend",
    "setup_class": "compression_under_resistance",
    "location": "top",
    "compression_tightness": 0.74,
    "impulse_strength": 0.81,
    "distance_to_resistance_atr": 0.12,
    "distance_to_support_atr": 0.65
  },
  "regime_features": {
    "atr_percentile": 0.62,
    "volume_percentile": 0.71,
    "trend_strength": 0.78,
    "volatility_state": "contracting"
  },
  "entry_definition": {
    "entry_type": "hypothetical_now",
    "entry_price": 78034.6,
    "stop_price": 77800,
    "risk_r": 234.6
  },
  "outcomes": {
    "hit_1r_before_minus_1r": true,
    "hit_2r_before_minus_1r": false,
    "mfe_r": 1.43,
    "mae_r": -0.62,
    "realized_r_breakout_entry": 0.9,
    "realized_r_direct_entry": -0.3,
    "bars_to_mfe": 18,
    "bars_to_mae": 7
  },
  "execution_adjusted": {
    "tier_1_r": 1.0,
    "tier_2_r": 0.72,
    "tier_3_r": 0.21
  }
}

14.4 Similarity scoring function

Similarity should combine:

similarity_score =
    visual_embedding_similarity * 0.30
  + structural_similarity * 0.30
  + regime_similarity * 0.20
  + location_similarity * 0.15
  + instrument_context_similarity * 0.05

Visual similarity

Compares chart geometry.

Structural similarity

Compares setup class, trend state, compression state, zone interaction.

Regime similarity

Compares volatility, volume, trend strength.

Location similarity

Compares where entry is relative to support/resistance and invalidation.

Instrument context similarity

Compares venue, symbol class, timeframe.

14.5 AnalogEvidence schema

{
  "query_setup_id": "setup_uuid",
  "analog_count": 146,
  "displayed_examples": 16,
  "similarity_threshold": 0.72,
  "summary": {
    "hit_1r_before_minus_1r_rate": 0.61,
    "hit_2r_before_minus_1r_rate": 0.34,
    "median_mfe_r": 1.46,
    "median_mae_r": -0.72,
    "ev_net_r_tier2": 0.12,
    "ev_net_r_tier3": -0.02
  },
  "conditional_findings": [
    {
      "condition": "after breakout confirmation",
      "hit_1r_rate": 0.68,
      "ev_net_r": 0.19
    },
    {
      "condition": "direct entry under resistance",
      "hit_1r_rate": 0.47,
      "ev_net_r": -0.04
    }
  ],
  "failure_modes": [
    "failed breakout and return into range",
    "low-volatility chop after compression",
    "late entry after impulse exhaustion"
  ],
  "recommendation_effect": "Downgrade immediate entry; upgrade breakout/retest entry."
}

14.6 Product behavior

When a user asks:

Does this entry make sense?

The system should say:

This is a bullish post-impulse compression under resistance.

Current entry quality is conditional, not favorable.

146 similar historical setups were found.
Direct entries under resistance had weak expectancy.
Breakout-confirmation entries performed better.

Best action:
Wait for a close above the resistance zone or a pullback to support.


⸻

15. Screenshot Chart Analysis Example from Thread

The user uploaded a BTCUSDT Perpetual, Bybit, 5-minute chart.

15.1 Visual read produced

The analysis identified:
	•	left side: accumulation/range
	•	impulse breakout upward
	•	continuation/stair-step trend
	•	right side: compression/balance near 78k
	•	resistance around 78,150–78,300
	•	local support around 77,800–77,900
	•	stronger support around 77,250
	•	current state: decision zone, not clean entry
	•	bullish bias, but no immediate entry
	•	best long: breakout confirmation or pullback to support
	•	possible short: failed breakout/liquidity trap
	•	warning: the move already happened; do not chase mid-range

15.2 Formalized output

VisualChartState:

{
  "trend": {
    "direction": "uptrend",
    "strength": 0.78
  },
  "structure": {
    "phase": "consolidation",
    "prior_phase": "impulse_continuation",
    "compression": {
      "active": true,
      "tightness": 0.7
    }
  },
  "zones": {
    "resistance": [
      {"low": 78150, "high": 78300, "strength": 0.8}
    ],
    "support": [
      {"low": 77800, "high": 77900, "strength": 0.7},
      {"low": 77250, "high": 77350, "strength": 0.9}
    ]
  },
  "location": {
    "relative_to_structure": "top",
    "entry_location_quality": "poor_without_confirmation"
  },
  "momentum": {
    "state": "weakening_after_impulse",
    "score": 0.52
  }
}

VisualSetupClass:

{
  "setup_class": "compression_under_resistance",
  "bias": "bullish_conditional",
  "stage": "developing",
  "confidence": 0.74
}

DecisionAssessment:

{
  "entry_quality_now": 0.42,
  "state": "no_trade",
  "recommended_actions": [
    "wait_for_breakout_confirmation",
    "wait_for_pullback_to_support",
    "avoid_buying_inside_range"
  ],
  "long_plan": {
    "trigger": "clean 5m close above 78200-78300",
    "alternative": "pullback to 77800-77900 with bullish reaction",
    "target": [78800, 79000]
  },
  "short_plan": {
    "trigger": "failed breakout above resistance and reclaim back inside range",
    "target": [77800, 77900]
  }
}


⸻

16. Technologies Required

16.1 For natural-language strategy engine

v1 can use existing LLMs without custom training.

Required:
	•	strong prompting
	•	structured JSON outputs
	•	schema validation
	•	tool/function calling
	•	deterministic compiler
	•	validation layer

Fine-tuning is not necessary initially.

Later training could include:
	•	supervised fine-tuning on strategy-intent-to-wrapper examples
	•	small classifier for strategy family routing
	•	preference tuning for clarification quality
	•	model distillation for lower-cost inference

16.2 For visual chart intelligence

Suggested stack:

v1 deterministic/heuristic layer
	•	chart screenshot input
	•	OCR for symbol/timeframe when needed
	•	image preprocessing
	•	optional candle extraction
	•	OHLCV-backed reconstruction when symbol/timeframe known
	•	rule-based swing/zone/compression detection

v2 machine vision layer
	•	pretrained vision encoder
	•	fine-tuned chart screenshot model
	•	object detection for candles/zones/annotations
	•	segmentation for drawn regions
	•	keypoint detection for pivots

v3 multimodal layer

Inputs:
	•	screenshot embedding
	•	OHLCV features
	•	structural features
	•	volatility regime
	•	historical analog outcomes

Models:
	•	vision transformer
	•	CNN/ConvNeXt
	•	YOLO-style detector
	•	multimodal transformer
	•	ranking model for analog retrieval
	•	calibrated classifier for setup quality

v4 analog retrieval

Storage:
	•	vector database for visual embeddings
	•	relational/analytical store for setup features
	•	Parquet for historical records

Search:
	•	vector nearest neighbor
	•	structural filters
	•	regime filters
	•	similarity score ranking

⸻

17. UI/UX Concepts

17.1 Strategy intake UI

Sections:
	1.	Describe your idea
	2.	Clarification chat
	3.	Strategy interpretation preview
	4.	Claim/mechanism/falsifiers
	5.	Data and execution assumptions
	6.	Confirm and run

17.2 Truth Panel

Always visible before run.

Shows:
	•	user-specified values
	•	AI-inferred values
	•	system defaults
	•	untested assumptions
	•	missing data
	•	unsupported features
	•	confidence in interpretation

17.3 Hypothesis Studio

Dedicated workspace for:
	•	claim
	•	mechanism
	•	falsifiers
	•	variables
	•	required data
	•	expected behavior
	•	diagnostics
	•	acceptance criteria

17.4 Run results page

Tabs:
	•	Overview
	•	Equity
	•	Trades
	•	Diagnostics
	•	Regimes
	•	Robustness
	•	Execution Drag
	•	Analog Evidence
	•	AI Verdict
	•	Next Experiments

17.5 Visual chart analysis UI

Main areas:

Chart panel
	•	uploaded screenshot
	•	detected support/resistance zones
	•	entry heatmap
	•	invalidation map
	•	breakout/retest levels

AI insight panel
	•	setup class
	•	market structure read
	•	entry quality score
	•	recommended action
	•	long plan
	•	short plan
	•	no-trade warning

Evidence panel
	•	analog count
	•	success distribution
	•	displayed historical examples
	•	winner/loser decomposition
	•	conditional findings

17.6 Entry Quality Heatmap

Visual overlay showing:
	•	too early
	•	acceptable
	•	optimal
	•	too late
	•	invalidation area

17.7 Invalidation Map

Shows where the thesis is wrong.

For example:
	•	long invalid below support reclaim failure
	•	short invalid above breakout acceptance

17.8 Confirmation Ladder

Instead of “buy/sell,” the system provides:
	•	current state
	•	condition required for long
	•	condition required for short
	•	invalidation
	•	best wait condition

Example:

Now: no trade.
Long becomes valid if: 5m close above resistance and holds.
Long improves if: breakout retest holds.
Invalid if: price loses local support.
Short only valid if: breakout fails and returns inside range.

17.9 Similar Historical Setups UI

Shows:
	•	top 12–20 analog charts
	•	outcome summary
	•	median MFE/MAE
	•	distribution chart
	•	expand to view full analog set
	•	filters by venue/timeframe/regime

17.10 Chartist Blind-Spot Detector

Flags:
	•	good pattern, bad location
	•	clean structure, late entry
	•	visual strength but poor R:R
	•	attractive breakout but low analog expectancy
	•	pattern works only in high-vol regimes

⸻

18. 12-Month Build Roadmap

Month 1: Foundations

Epics:
	•	define strategy ontology
	•	define claim/hypothesis schema
	•	freeze wrapper contract
	•	define execution profiles
	•	define run provenance
	•	define data manifest
	•	define diagnostic bundle

Deliverables:
	•	schema repo
	•	ontology v1
	•	wrapper mapping spec
	•	validator requirements
	•	product principles document

Months 2–3: AI Strategy Drafting

Epics:
	•	natural-language parser
	•	strategy-family router
	•	clarification agent
	•	StrategyIntentSpec generator
	•	HypothesisDraft generator
	•	review/confirmation UI

APIs:
	•	/ai/parse-strategy
	•	/ai/clarify-strategy
	•	/ai/generate-spec
	•	/ai/generate-hypothesis

Milestone:
	•	user can describe a simple crypto strategy and get a structured, reviewable hypothesis.

Months 4–5: Compiler and Validation

Epics:
	•	deterministic compiler
	•	wrapper template library
	•	semantic validator
	•	no-lookahead validator
	•	data-dependency validator
	•	error explanation layer

APIs:
	•	/compiler/compile
	•	/validator/validate-strategy

Milestone:
	•	supported strategies compile to bulletproof_bt configs and invalid strategies are rejected clearly.

Months 6–7: Managed Crypto Data Platform

Epics:
	•	symbol master
	•	venue abstraction
	•	data ingestion
	•	data validation
	•	coverage API
	•	data versioning
	•	hot/warm/BYO data logic

APIs:
	•	/symbols/search
	•	/data/coverage
	•	/data/request-ingestion
	•	/uploads/inspect

Milestone:
	•	BTC/ETH on Binance/Bybit supported with reproducible data manifests.

Months 8–9: Full Backtest Execution Product

Epics:
	•	run creation API
	•	queue/worker system
	•	artifact storage
	•	result dashboards
	•	progress updates
	•	run provenance
	•	report generation

APIs:
	•	/runs
	•	/runs/{id}
	•	/runs/{id}/results
	•	/runs/{id}/artifacts

Milestone:
	•	user can go from English idea to confirmed backtest result in one flow.

Months 10–11: Diagnostics and Research Copilot

Epics:
	•	EV decomposition
	•	MAE/MFE
	•	regime buckets
	•	robustness reports
	•	AI result interpretation
	•	next experiment generator
	•	adversarial critic

APIs:
	•	/diagnostics/run/{id}
	•	/ai/explain-results
	•	/ai/suggest-next-experiments
	•	/ai/adversarial-critique

Milestone:
	•	product behaves like a research co-pilot, not just a backtester.

Month 12: Research Memory and Collaboration

Epics:
	•	research graph
	•	hypothesis lineage
	•	project system
	•	team workspaces
	•	comments
	•	approvals
	•	weekly AI research summaries

APIs:
	•	/projects
	•	/research-graph
	•	/hypotheses/{id}/lineage
	•	/ai/research-summary

Milestone:
	•	platform becomes cumulative research OS.

Parallel/Advanced Track: Visual Market Structure Intelligence

Suggested phased roadmap:

VMSE Phase 1: Deterministic chart analysis prototype
	•	accept screenshots
	•	parse metadata manually or by user input
	•	use OHLCV-backed chart reconstruction when possible
	•	detect pivots/zones/compression using rules
	•	output VisualChartState
	•	output DecisionAssessment

VMSE Phase 2: UI overlays
	•	support/resistance overlays
	•	entry-quality panel
	•	confirmation ladder
	•	invalidation map

VMSE Phase 3: Analog library
	•	render historical chart windows from OHLCV
	•	generate setup records
	•	label outcomes
	•	store embeddings
	•	retrieve analogs

VMSE Phase 4: Multimodal model
	•	train/fine-tune visual embedding model
	•	train retrieval/ranking model
	•	calibrate entry quality scores
	•	compare image-only vs OHLCV+image fusion

VMSE Phase 5: Visual-to-hypothesis bridge
	•	convert visual setup classes into hypothesis wrappers
	•	backtest visual classes
	•	produce evidence-backed setup library

⸻

19. Implementation Warnings

19.1 Do not build “LLM → code → run”

This is unsafe.

Correct path:

LLM → structured spec → deterministic compiler → validator → user confirmation → run

19.2 Do not build “screenshot → signal”

This is weak and dangerous.

Correct path:

screenshot → structure → setup class → decision assessment → analog evidence → explanation

19.3 Do not promise “any asset” too early

Correct path:
	•	crypto-first hot data
	•	on-demand cached data
	•	BYO data
	•	later multi-asset expansion

19.4 Do not hide assumptions

Every output must show:
	•	assumptions
	•	defaults
	•	limitations
	•	confidence
	•	evidence count

19.5 Do not optimize analog retrieval to prove the model right

Retrieve honest analogs:
	•	winners
	•	losers
	•	mixed cases

The product must ground recommendations, not justify them.

⸻

20. Product Differentiators

The strongest differentiators are:
	1.	AI turns English into falsifiable claims, not just code.
	2.	bulletproof_bt provides execution-realistic simulation.
	3.	every strategy must pass through validation.
	4.	data is venue-specific, normalized, versioned, and auditable.
	5.	visual chart reading becomes structured and testable.
	6.	entry recommendations are backed by historical analog distributions.
	7.	the system remembers research history.
	8.	every result produces next experiments.
	9.	the product actively attacks the user’s assumptions.
	10.	the product compounds knowledge over time.

⸻

21. Final Product Definition

The complete product can be defined as:

An AI-native quantitative research operating system that converts natural-language trading ideas and visual chart intuition into falsifiable, execution-aware hypotheses; validates them using normalized venue-specific market data and bulletproof_bt; diagnoses performance through deep regime, execution, and analog evidence; and compounds every experiment into a growing research memory.

The visual module can be defined as:

A Visual Market Structure Intelligence system that reads chart screenshots, extracts structure, classifies setup state, assesses entry quality, retrieves historical analogs, and converts discretionary chart reads into measurable research objects.

The fundamental unit of the product should be:

The falsifiable market claim.

Not:
	•	strategy
	•	signal
	•	backtest
	•	code
	•	screenshot

Everything else is built around validating or falsifying that claim.