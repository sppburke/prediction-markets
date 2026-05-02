# Prediction Market Edge Stack v5 — Winner-Follow First, Rust 1.95.0
## Polymarket + Kalshi, resolver-first, cross-venue

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

This v5 package is a Rust 1.95.0 update that makes **Winner-Follow** the first deployable strategy while preserving the prior resolver-first Polymarket/Kalshi architecture. The system now starts by finding the fastest-compounding public traders/operators, reconstructing their behavior, and copying only the subset of trades that survive empirical latency, liquidity, cost, and fractional-Kelly risk checks. Resolver/source-arbitrage strategies remain in the package as Strategy 1+, but the first build target is trader-intelligence plus speed.

The core idea is still:

> do not trade headlines; trade the exact resolver, the exact source path, the exact timing window, and the exact venue microstructure.


## Strategy 0 — Winner-Follow Copy Engine

The first production strategy is **Winner-Follow**: continuously discover, rank, watch, and selectively copy the fastest-compounding public traders before the broader market has fully incorporated their action. This is not treated as a risk-free or "edge-free" system. The edge is the combination of public trader intelligence, rigorous skill filtering, latency-optimized copy execution, fractional-Kelly sizing, portfolio-level drawdown control, and continuous decay monitoring.

Winner-Follow ships before weather, crypto, macro, sports, and chart/source-arbitrage strategies because it can be built using public venue/profile/trade data, deterministic analysis, and speed. Resolver-source strategies remain Strategy 1+ and are used later to validate whether copied trades have independent fundamental support.

Winner-Follow is operator-aware. A wallet is an observable proxy, not necessarily a distinct economic actor. The plan adds a native Polygon funding/collateral graph and pure `operator-graph` layer to collapse wallets into deterministic operators when public proxy-wallet, pUSD, deposit/onramp, and funder evidence supports it. CrowdIntel-style funding clusters are treated as research inspiration, not a production data dependency.

Fresh-wallet first-trade following is a constrained incubator mode. A fresh wallet may inherit a heavily shrunk operator/funder prior only when the funding/collateral path is public, replayable, low-hop, and not flagged as gaming. This mode starts in paper; cluster-coordination starts in shadow.

### Venue support

- **Polymarket:** first-class support. The public Data API exposes leaderboard, user trades, positions, activity, and related profile data, making trader-level reconstruction feasible. Proxy-wallet and pUSD collateral behavior means funding identity must be verified from official/public chain evidence.
- **Kalshi:** limited trader-copy support. Kalshi exposes public trades and a leaderboard feature, but public trade messages do not identify the trader and leaderboard participation is opt-in. Kalshi is therefore used for copy-trading only when a lawful public identity-to-trade mapping exists, when a trader explicitly authorizes API/portfolio access, or when future official endpoints expose sufficient public trader-level data. Otherwise, Kalshi remains a venue for resolver-source strategies, market-flow analytics, and cross-venue checks.

### Ranking objective

Rank operators/traders by **walk-forward lower-confidence expected log-growth per day** for a follower account after simulated latency, spread, slippage, fees, partial fills, and position caps. Raw PnL, win rate, leaderboard rank, and cluster reputation are inputs, not the final ranking target.

### Default eligibility thresholds

The user-proposed `average hold < 5 days` and `>= 15 trades` are too loose for production. Use adaptive thresholds instead:

- at least **60 closed trades** or **30 resolved markets** in the rolling 180-day audit window;
- at least **12 closed trades in the last 30 days** for active-copy eligibility;
- median capital-weighted holding period **<= 72 hours**;
- 75th percentile holding period **<= 7 days**;
- minimum simulated follower turnover of **0.35 bankroll-equivalent per day** after caps;
- positive lower 5% bootstrap estimate of daily log growth after copy delay and costs;
- no single resolved market contributes more than **20%** of audited profit;
- no more than **35%** of audited profit comes from positions too illiquid for the follower to enter within the latency/slippage budget.

### Default sizing

For a binary contract with current entry price `c` and calibrated copied-trade win probability `p`, the full-Kelly bankroll allocation to stake cost is:

```text
f_full = max(0, (p - c) / (1 - c))
f_live = kelly_fraction * f_full
```

Use **quarter Kelly** by default during live-tiny and scale to half Kelly only after a statistically meaningful live audit. `p` is not the trader's naive win rate. It is a calibrated, shrinkage-adjusted probability conditional on trader, market family, odds bucket, liquidity, holding-period bucket, side, recency, and observed copy latency.

Hard caps override Kelly: max 0.25% bankroll per copied trade in live-tiny, max 1.00% after promotion, max 3.00% per trader/operator, max 8.00% per market family, max 25.00% total open copy exposure, and stop new entries after 2.00% intraday drawdown or 6.00% rolling 7-day drawdown until review. Inherited-prior and cluster-coordination modes have lower separate caps and separate promotion ladders.


## What changed in this pass

- Every markdown file was rewritten or updated to specify Rust 2024 implementation requirements pinned to stable Rust 1.95.0.
- The architecture is now **event-sourced**: every source update, market update, model output, risk decision, order attempt, fill, cancel, and settlement is recorded and replayable.
- The system uses a **Rust workspace** with small crates, strict type boundaries, deterministic replay, and fake venues/sources before live trading.
- Winner-Follow now includes `source-onchain-polygon` and `operator-graph` planning for native, replayable funding/collateral identity.
- Kalshi and Polymarket are implemented as separate venue adapters. A common trait exists for orchestration, but venue-specific behavior is preserved.
- All source connectors are Rust actors with bounded channels, parser versions, source health, raw payload hashes, and replay fixtures.
- All modeling is Rust-native: rule engines, finite-state machines, benchmark-window accumulators, Polars/DataFusion analytics, and optional Rust ML/inference.
- The execution engine uses typed order lifecycle states, idempotency keys, local journals, reconciliation, kill switches, and pure replayable risk checks.


## Rust baseline

- **Edition/toolchain:** Rust 2024 Edition, stable Rust 1.95.0 toolchain, `Cargo.lock` committed, reproducible builds.
- **Async:** `tokio`, `tokio-tungstenite`, `reqwest`/`hyper`, `tower`, `axum`.
- **Serialization/parsing:** `serde`, `serde_json`, `simd-json` for benchmarked hot paths, `csv`, `quick-xml`, `scraper`, `schemars`.
- **Numerics:** `rust_decimal` and integer ticks/cents/basis-points. No raw `f64` for venue prices, money, contract counts, or probabilities.
- **Analytics:** Rust `polars`, Apache Arrow/Parquet, `datafusion`.
- **ML/inference:** Rust `linfa`, `burn`, `candle`, and/or `ort` for ONNX inference. No production Python model server.
- **Messaging/storage:** local append-only framed event log first; `async-nats` JetStream or Kafka/Redpanda-compatible streams later; Parquet lakehouse for replay.
- **Observability:** `tracing`, `tracing-opentelemetry`, OpenTelemetry metrics/logs/traces.
- **Testing:** `proptest`, `loom`, `shuttle`, `cargo-nextest`, `criterion`, `insta`, fake source/venue servers.


## File map

1. `00-PROJECT-OVERVIEW.md` — mission and Rust-only system thesis
2. `01-PHASE-FOUNDATION.md` — Rust workspace, types, schemas, CI
3. `02-PHASE-DATA-INGESTION.md` — Rust source bus and connector standards
4. `03-PHASE-MODEL-ENGINE.md` — resolver cards, nowcasters, Rust ML/inference
5. `04-PHASE-TRADING-STRATEGY.md` — Rust strategies, typed order intents, risk gates
6. `05-PHASE-BACKTESTING.md` — deterministic replay, fill simulation, Rust analytics
7. `06-PHASE-DEPLOYMENT.md` — Rust services, observability, failover, kill switches
8. `07-VENUE-KALSHI.md` — Kalshi adapter and opportunity map
9. `08-VENUE-POLYMARKET.md` — Polymarket adapter and venue details
10. `09-CROSS-VENUE-MISMATCHES-AND-HEDGES.md` — compatibility and fake-hedge prevention
11. `10-VERTICALS-PLAYBOOKS.md` — crypto, weather, sports, macro, charts, events
12. `11-KALSHI-OPPORTUNITY-MAP.md` — Kalshi-focused implementation plan
13. `12-SOURCE-CATALOG.md` — source tiers and connector requirements
14. `13-WORLD-FIRST-EXPERIMENTS.md` — high-upside Rust-native experiments
15. `14-COMPLIANCE-AND-RISK.md` — legal/source/operational risk
16. `15-SOURCES.md` — venue, source, and Rust implementation references
17. `16-RUST-WORKSPACE-ARCHITECTURE.md` — crate graph and dependency rules
18. `17-RUST-IMPLEMENTATION-ROADMAP.md` — build sequence and acceptance gates
19. `18-CODEX-RUST-BOOTSTRAP-PROMPT.md` — one-shot Rust implementation prompt
20. `PACKAGING-NOTES.md` — package notes
21. `19-WINNER-FOLLOW-STRATEGY.md` — first strategy: top-trader discovery, ranking, copy execution, Kelly sizing
22. `20-AWS-GIT-OPERATIONS.md` — Git, CI/CD, AWS deployment, secrets, observability
23. `21-RESEARCH-AND-SOURCE-DISCOVERY.md` — official-doc lookup and source-discovery protocol
24. `AGENTS.md` — coding-agent operating instructions
25. `SKILLS.md` — project skill definitions and quality gates

## Non-negotiables

- Accuracy before speed; speed immediately after accuracy.
- The resolver card is mandatory before trading a market.
- No unbounded queues in hot paths.
- No `unwrap`, `expect`, or unchecked panics in production crates.
- No raw floating-point order prices or balances.
- No direct strategy-to-venue order submission. Strategies emit `OrderIntent`; execution routers submit.
- No live deployment before fake connector replay, historical replay, paper mode, and shadow mode.
- No cross-venue “hedge” label until resolver compatibility proves it.
- No CrowdIntel UI scraping or opaque third-party cluster scores in the production decision path.
- No fresh-wallet inherited-prior live sizing until proxy/funder/collateral mapping and mode-specific backtests are proven.

## Coding-agent instructions

Before coding, read `AGENTS.md`, `SKILLS.md`, and `19-WINNER-FOLLOW-STRATEGY.md`. The repository must be Git-hosted, CI-gated, and deployed through AWS with Rust 1.95.0 reproducible builds.
