# 18 — Codex Rust Bootstrap Prompt

```text
Build a Rust 2024 workspace named `prediction-edge`, pinned to stable Rust 1.95.0.

Goal:
A winner-follow-first, resolver-first, cross-venue trading/research system for Kalshi and Polymarket. All first-party production code must be Rust. No Python, Node, or browser automation in the production hot path.

Architecture:
- event-sourced;
- deterministic replay;
- Tokio async runtime;
- bounded channels only;
- Axum internal health/control APIs;
- typed source connectors;
- typed venue adapters;
- resolver-card compiler/validator;
- Rust-native models;
- risk-gated execution;
- fake sources/venues before live APIs;
- shadow/paper/live-tiny modes before scaled trading.

Create crates:
core-types, config, event-log, resolver-card, source-core, source-trader, source-weather, source-crypto, source-sports, source-macro, source-charts, source-events, venue-core, venue-kalshi, venue-polymarket, model-core, trader-index, copy-signal-engine, kelly-sizer, strategy-core, strategy-winner-follow, execution-core, risk-engine, replay, backtest, observability, cli, service.

Strict requirements:
- Rust edition 2024.
- Rust toolchain pinned to 1.95.0 in `rust-toolchain.toml`.
- No unwrap/expect/panic in production crates.
- unsafe forbidden by default.
- Strong newtypes for prices, quantities, timestamps, probabilities, source IDs, market IDs, order IDs.
- No raw f64 for money, prices, contract counts, or probabilities.
- Every event uses EventEnvelope with schema version, timestamps, parser version, payload hash, and typed data.
- Strategies emit OrderIntent only; execution router submits orders.
- Risk checks are pure and replayable.
- Replay uses the same crates as production.

First milestone:
1. workspace and CI with AGENTS.md and SKILLS.md checks;
2. core-types;
3. event envelope and local event log;
4. fake source connector;
5. fake venue connector;
6. manual resolver card;
7. fake model;
8. Winner-Follow strategy producing risk-checked OrderIntent;
9. replay reproducing the decision;
10. tests: unit, property, fixture, replay.
```


Winner-Follow requirements:
- Implement Polymarket public trader ingestion first: leaderboard, user trades, user positions, user activity, market/orderbook data.
- Implement Kalshi copy support only as authorized/public trader-level data; otherwise keep Kalshi public trades as anonymous market-flow data.
- Rank traders by walk-forward lower-confidence expected follower log-growth per day, not raw PnL.
- Use adaptive thresholds: 60 closed trades or 30 resolved markets, 12 recent closed trades, median hold <=72h, p75 hold <=7d.
- Copy only entry/add trades that survive liquidity, latency, slippage, cost, and portfolio risk checks.
- Size with calibrated quarter-Kelly by default, hard capped by trade, trader, market, family, total exposure, and drawdown.
- Produce deterministic replay showing why each copied trade was or was not taken.
