# AGENTS.md — Coding Agent Instructions

## Mission

Build `prediction-edge`: a Rust 1.95.0, Rust 2024, event-sourced, replayable trading/research system for Polymarket and Kalshi. The first strategy is Winner-Follow.

## Mandatory reading before coding

1. `README.md`
2. `SKILLS.md`
3. `19-WINNER-FOLLOW-STRATEGY.md`
4. The specific phase/venue/source document relevant to the task
5. Latest official docs for touched APIs

## Non-negotiable rules

- First-party production code must be Rust.
- Toolchain must be pinned to stable Rust 1.95.0.
- No production Python, Node, or browser automation.
- No `unwrap`, `expect`, or unchecked `panic!` in production crates.
- No raw `f64` for money, prices, probability, quantities, or balances.
- No unbounded channels in hot paths.
- Strategies emit `OrderIntent`; only execution routers submit orders.
- Every external input is logged with raw hash, parser version, timestamps, and source ID.
- Every live decision must be replayable.
- Do not implement Kalshi trader-copy attribution unless the data is official/public or explicitly authorized.
- Do not use CrowdIntel UI data or opaque third-party cluster scores in live decisions unless an authorized, replayable API/export is reviewed.
- Do not size fresh-wallet first-trade signals until proxy-wallet/funder/collateral mapping is proven from public data and the mode has its own paper/backtest validation.

## Winner-Follow first milestone

Implement in this order:

1. workspace and CI;
2. core types;
3. event log;
4. Polymarket public trader ingestion;
5. public Polygon funding/collateral ingestion;
6. operator graph and identity collapse;
7. trader ledger reconstruction;
8. walk-forward operator-aware ranker;
9. copy-signal classifier;
10. fractional-Kelly sizer;
11. risk gates;
12. paper-copy execution;
13. live-tiny after approval.

## Required quality gates

Before claiming a task is done, run or document why you could not run:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo nextest run --workspace --all-features
cargo deny check
cargo audit
```

## Design style

- Prefer small crates and explicit boundaries.
- Prefer pure functions for risk, sizing, ranking, and classification.
- Prefer typed state machines over booleans.
- Prefer deterministic fixtures and replay tests.
- Prefer official APIs over scraping.
- Make unsafe code forbidden unless a separate design review approves it.

## Pull request format

Every PR must include summary, files changed, tests run, replay impact, risk impact, API docs checked, deployment impact, and rollback plan.
