# 18 — Codex Rust Bootstrap Prompt

> Hand-off prompt for a coding agent. Before running the prompt, the agent must read `_BASELINE.md`, `_GLOSSARY.md`, `AGENTS.md`, `SKILLS.md`, and `19-WINNER-FOLLOW-STRATEGY.md`. The full crate list is in `01-PHASE-FOUNDATION.md` ("Workspace layout") — this prompt does not duplicate it.

```text
Build a Rust 2024 workspace named `prediction-edge`, pinned to stable Rust 1.95.0.

Goal:
A winner-follow-first, resolver-first, cross-venue trading/research system for Kalshi
and Polymarket. All first-party production code must be Rust. No Python, Node, or
browser automation in the production hot path.

Architecture:
- event-sourced;
- deterministic replay;
- Tokio async runtime;
- bounded channels only;
- Axum internal health/control APIs;
- typed source connectors;
- typed venue adapters;
- resolver-card compiler/validator (sub-types in _GLOSSARY.md);
- Rust-native models;
- risk-gated execution;
- fake sources/venues before live APIs;
- shadow/paper/live-tiny modes before scaled trading.

Crates: see `01-PHASE-FOUNDATION.md` "Workspace layout". Use that list verbatim.

Strict requirements:
- Rust edition 2024.
- Rust toolchain pinned to 1.95.0 in `rust-toolchain.toml`.
- Lints from `_BASELINE.md` "Required workspace lints"; `unsafe` forbidden by default.
- Strong newtypes for prices, quantities, timestamps, probabilities, source IDs, market
  IDs, order IDs (canonical list in `_GLOSSARY.md` "Type aliases").
- No raw f64 for money, prices, contract counts, or probabilities.
- Every event uses EventEnvelope with schema version, timestamps, parser version,
  payload hash, and typed data (envelope in `00-PROJECT-OVERVIEW.md`).
- Strategies emit OrderIntent only; execution router submits orders.
- Risk checks are pure and replayable.
- Replay uses the same crates as production.

First milestone:
1. workspace and CI with AGENTS.md, SKILLS.md, _BASELINE.md, _GLOSSARY.md checks;
2. core-types matching `_GLOSSARY.md`;
3. event envelope and local event log;
4. fake source connector;
5. fake venue connector;
6. manual resolver card using sub-types from `_GLOSSARY.md`;
7. fake model;
8. Winner-Follow strategy producing risk-checked OrderIntent (caps from canonical TOML
   in `19-WINNER-FOLLOW-STRATEGY.md`);
9. replay reproducing the decision;
10. tests: unit, property, fixture, replay.

Winner-Follow requirements (full spec in `19-WINNER-FOLLOW-STRATEGY.md`):
- Implement Polymarket public trader ingestion first: leaderboard, user trades, user
  positions, user activity, market/orderbook data.
- Implement Kalshi copy support only as authorized/public trader-level data; otherwise
  keep Kalshi public trades as anonymous market-flow data.
- Rank operators/traders by walk-forward LCB_5pct of follower log-growth per day,
  not raw PnL.
- Use the eligibility thresholds and Kelly fractions from `19-WINNER-FOLLOW-STRATEGY.md`.
- In the current production profile, copy only first-ever BUY entries that survive
  liquidity, latency, slippage, cost, and portfolio risk checks; classify later actions
  for replay (gates in `04-PHASE-TRADING-STRATEGY.md`).
- Size with calibrated quarter-Kelly by default; hard caps from `19-`.
- Produce deterministic replay showing why each copied trade was or was not taken.
```
