# AGENTS.md — Coding Agent Instructions

## Mission

Build `prediction-edge`: a Rust 1.95.0, Rust 2024, event-sourced, replayable trading/research system for Polymarket and Kalshi. The first strategy is Winner-Follow.

## Mandatory reading before coding

1. `README.md`
2. `_BASELINE.md` — toolchain, lints, common acceptance gate
3. `_GLOSSARY.md` — vocabulary, type aliases, latency budget, rate limits, configuration defaults
4. `SKILLS.md`
5. `19-WINNER-FOLLOW-STRATEGY.md` — canonical risk caps, Kelly fractions, eligibility thresholds, promotion ladders
6. The specific phase/venue/source document relevant to the task
7. Latest official docs for touched APIs (per `21-RESEARCH-AND-SOURCE-DISCOVERY.md`)

## Non-negotiable rules

- First-party production code must be Rust.
- Toolchain must be pinned to stable Rust 1.95.0 (`_BASELINE.md`).
- No production Python, Node, or browser automation.
- No `unwrap`, `expect`, or unchecked `panic!` in production crates.
- No raw `f64` for money, prices, probability, quantities, or balances.
- No unbounded channels in hot paths.
- Strategies emit `OrderIntent`; only execution routers submit orders.
- Every external input is logged with raw hash, parser version, timestamps, and source ID.
- Every live decision must be replayable.
- Do not implement Kalshi trader-copy attribution unless the data is official/public or explicitly authorized.
- Do not use CrowdIntel UI data or opaque third-party cluster scores in live decisions unless an authorized, replayable API/export is reviewed.
- Do not introduce any new numeric threshold without adding a default to `_GLOSSARY.md` or the canonical TOML in `19-`.

## Doc authority order (when conflicts arise)

1. `_BASELINE.md` — toolchain and gate
2. `_GLOSSARY.md` — types, vocabulary, defaults
3. `19-WINNER-FOLLOW-STRATEGY.md` — Winner-Follow risk caps and gates
4. Phase/venue files (`00–17`, `20`, `21`)
5. Bootstrap prompt (`18`)

If a phase doc disagrees with `19-` on a Winner-Follow value, `19-` wins. If a doc disagrees with `_GLOSSARY.md` on a type or default, `_GLOSSARY.md` wins. Fix the lower-priority doc.

## Winner-Follow first milestone

Implement in this order (full list in `17-RUST-IMPLEMENTATION-ROADMAP.md` "Phase 0A"):

1. workspace and CI;
2. core types;
3. event log;
4. Polymarket public trader ingestion;
5. trader ledger reconstruction;
6. walk-forward per-wallet ranker;
7. copy-signal classifier;
8. fractional-Kelly sizer;
9. risk gates;
10. paper-copy execution;
11. live-tiny after approval.

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
- `unsafe` is forbidden unless a separate design review approves it (`_BASELINE.md`).

## Pull request format

Every PR must include summary, files changed, tests run, replay impact, risk impact, API docs checked (`Last checked` updates in `15-SOURCES.md`), deployment impact, and rollback plan.
