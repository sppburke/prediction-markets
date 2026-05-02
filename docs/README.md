# Prediction Market Edge Stack v5 — Winner-Follow First, Rust 1.95.0
## Polymarket + Kalshi, resolver-first, cross-venue

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule, toolchain pin, lints, and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for vocabulary (wallet/trader/operator/leader/candidate), type aliases, latency budget, rate limits, anti-gaming flag thresholds, and configuration defaults.
> See [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md) for canonical risk caps, Kelly fractions, eligibility thresholds, and promotion ladders.

This v5 package makes **Winner-Follow** the first deployable strategy while preserving the prior resolver-first Polymarket/Kalshi architecture. The system starts by finding the fastest-compounding public traders/operators, reconstructing their behavior, and copying only the subset of trades that survive empirical latency, liquidity, cost, and risk-cap checks. Resolver/source-arbitrage strategies remain Strategy 1+, but the first build target is trader-intelligence plus speed.

The core idea remains:

> do not trade headlines; trade the exact resolver, the exact source path, the exact timing window, and the exact venue microstructure.

## Strategy 0 — Winner-Follow Copy Engine

The first production strategy continuously discovers, ranks, watches, and selectively copies the fastest-compounding public traders before the broader market has fully incorporated their action. The edge is the combination of public trader intelligence, rigorous skill filtering, latency-optimized copy execution, fractional-Kelly sizing, portfolio-level drawdown control, and continuous decay monitoring — proven by walk-forward simulation, not assumed.

Winner-Follow ships before weather, crypto, macro, sports, and chart/source-arbitrage strategies because it can be built using public venue/profile/trade data, deterministic analysis, and speed. Resolver-source strategies (Strategy 1+) are used later to validate whether copied trades have independent fundamental support.

Winner-Follow is **operator-aware**: a wallet is an observable proxy, not necessarily a distinct economic actor. The plan adds a native Polygon funding/collateral graph and pure `operator-graph` layer to collapse wallets into deterministic operators when public proxy-wallet, pUSD, deposit/onramp, and funder evidence supports it. CrowdIntel-style funding clusters are research inspiration, not a production data dependency.

Fresh-wallet first-trade following is a constrained incubator mode (default paper). Cluster-coordination is a separate mode (default shadow). Each has its own promotion ladder; see [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md).

### Venue support — summary (full detail in [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md))

- **Polymarket:** first-class. Public Data API exposes leaderboard, user trades, positions, activity. Funding identity is verified from public proxy/funder/collateral evidence.
- **Kalshi:** copy-trading disabled by default; public trade messages do not identify the trader. Kalshi remains a venue for resolver-source strategies, market-flow analytics, and cross-venue checks.

### Ranking objective — summary

Rank operators/traders by **walk-forward LCB_5pct expected log-growth per day** for a follower account after simulated latency, spread, slippage, fees, partial fills, and position caps. Raw PnL, win rate, leaderboard rank, and cluster reputation are inputs, not the final ranking target.

### Eligibility, sizing, risk caps

See [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md) for the canonical thresholds, Kelly fractions, hard caps, drawdown stops, and promotion ladders. Other docs reference that file rather than restating values.

## What changed in this pass

- Every markdown file updated to specify Rust 2024 implementation requirements pinned to stable Rust 1.95.0.
- The architecture is **event-sourced**: every source update, market update, model output, risk decision, order attempt, fill, cancel, and settlement is recorded and replayable.
- The system uses a **Rust workspace** with small crates, strict type boundaries, deterministic replay, and fake venues/sources before live trading.
- Winner-Follow includes `source-onchain-polygon` and `operator-graph` planning for native, replayable funding/collateral identity.
- Kalshi and Polymarket are implemented as separate venue adapters. A common trait exists for orchestration, but venue-specific behavior is preserved.
- All source connectors are Rust actors with bounded channels, parser versions, source health, raw payload hashes, and replay fixtures.
- All modeling is Rust-native: rule engines, finite-state machines, benchmark-window accumulators, Polars/DataFusion analytics, and optional Rust ML/inference.
- The execution engine uses typed order lifecycle states, idempotency keys, local journals, reconciliation, kill switches, and pure replayable risk checks.
- This pass extracted duplicated boilerplate into `_BASELINE.md` and consolidated vocabulary, types, and configuration defaults into `_GLOSSARY.md`. The canonical Winner-Follow risk-cap TOML lives in `19-WINNER-FOLLOW-STRATEGY.md`.

## File map

| # | File | Purpose |
|---:|---|---|
| – | `_BASELINE.md` | Rust toolchain, lints, common acceptance gate |
| – | `_GLOSSARY.md` | Vocabulary, types, acronyms, latency budget, rate limits, config defaults |
| 00 | `00-PROJECT-OVERVIEW.md` | Mission and Rust-only system thesis |
| 01 | `01-PHASE-FOUNDATION.md` | Rust workspace, types, schemas, CI |
| 02 | `02-PHASE-DATA-INGESTION.md` | Rust source bus and connector standards |
| 03 | `03-PHASE-MODEL-ENGINE.md` | Resolver cards, nowcasters, Rust ML/inference |
| 04 | `04-PHASE-TRADING-STRATEGY.md` | Rust strategies, typed order intents, risk gates |
| 05 | `05-PHASE-BACKTESTING.md` | Deterministic replay, fill simulation, Rust analytics |
| 06 | `06-PHASE-DEPLOYMENT.md` | Rust services, observability, failover, kill switches |
| 07 | `07-VENUE-KALSHI.md` | Kalshi adapter and opportunity map |
| 08 | `08-VENUE-POLYMARKET.md` | Polymarket adapter and venue details |
| 09 | `09-CROSS-VENUE-MISMATCHES-AND-HEDGES.md` | Compatibility and fake-hedge prevention |
| 10 | `10-VERTICALS-PLAYBOOKS.md` | Crypto, weather, sports, macro, charts, events |
| 11 | `11-KALSHI-OPPORTUNITY-MAP.md` | Kalshi-focused implementation plan |
| 12 | `12-SOURCE-CATALOG.md` | Source tiers and connector requirements |
| 13 | `13-WORLD-FIRST-EXPERIMENTS.md` | High-upside Rust-native experiments |
| 14 | `14-COMPLIANCE-AND-RISK.md` | Legal/source/operational risk |
| 15 | `15-SOURCES.md` | Venue, source, and Rust implementation references |
| 16 | `16-RUST-WORKSPACE-ARCHITECTURE.md` | Crate graph and dependency rules |
| 17 | `17-RUST-IMPLEMENTATION-ROADMAP.md` | Build sequence and acceptance gates |
| 18 | `18-CODEX-RUST-BOOTSTRAP-PROMPT.md` | One-shot Rust implementation prompt |
| 19 | `19-WINNER-FOLLOW-STRATEGY.md` | **Canonical** risk caps, Kelly, eligibility, promotion |
| 20 | `20-AWS-GIT-OPERATIONS.md` | Git, CI/CD, AWS deployment, secrets, observability |
| 21 | `21-RESEARCH-AND-SOURCE-DISCOVERY.md` | Official-doc lookup and source-discovery protocol |
| – | `AGENTS.md` | Coding-agent operating instructions |
| – | `SKILLS.md` | Project skill definitions and quality gates |
| – | `PACKAGING-NOTES.md` | Package change log |

## Non-negotiables

- Accuracy before speed; speed immediately after accuracy.
- The resolver card is mandatory before trading a market.
- No unbounded queues in hot paths.
- No `unwrap`, `expect`, or unchecked panics in production crates.
- No raw floating-point order prices or balances.
- No direct strategy-to-venue order submission. Strategies emit `OrderIntent`; execution routers submit.
- No live deployment before fake connector replay, historical replay, paper mode, and shadow mode.
- No cross-venue "hedge" label until resolver compatibility proves it.
- No CrowdIntel UI scraping or opaque third-party cluster scores in the production decision path.
- No fresh-wallet inherited-prior live sizing until proxy/funder/collateral mapping and mode-specific backtests are proven.

## Coding-agent instructions

Before coding, read `AGENTS.md`, `SKILLS.md`, `_BASELINE.md`, `_GLOSSARY.md`, and `19-WINNER-FOLLOW-STRATEGY.md`. The repository must be Git-hosted, CI-gated, and deployed through AWS with Rust 1.95.0 reproducible builds.
