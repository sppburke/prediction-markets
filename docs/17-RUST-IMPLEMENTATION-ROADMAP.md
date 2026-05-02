# 17 — Rust Implementation Roadmap

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Phase 0 — Toolchain and workspace

Create workspace, strict lints, CI, `rust-toolchain.toml`, fake service binary, config loader, health endpoint.

## Phase 1 — Core types and event log

Implement typed IDs/prices/probabilities/timestamps, event envelope, local append-only log, raw payload hashes, replay reader.

## Phase 2 — Fake connectors

Build fake source and fake venue connectors. They must run end-to-end without real APIs.

## Phase 3 — Kalshi adapter

Implement REST/WS client, order book reconstruction, market discovery, queue position, order journal, reconciliation, demo/paper mode first.

## Phase 4 — Polymarket adapter

Implement market/user/sports/RTDS sockets, market discovery, signing module with fixtures, order journal, reconciliation, dry-run mode first.

## Phase 5 — Resolver cards

Manual authoring, schema, validator, draft compiler, human review workflow, sample cards for weather/crypto/sports/macro/charts.

## Phase 6 — Sources

Build in this order: trader-intelligence, crypto, weather, macro, charts, sports, events. Every source gets fixtures, raw hashes, health metrics, and replay.

## Phase 7 — Models

Finalizers, benchmark windows, nowcasters, cross-venue compatibility, calibration reports, model artifacts.

## Phase 8 — Strategy/risk

Cost model, latency budget, risk gates, kill switches, order-intent pipeline.

## Phase 9 — Backtesting

Replay CLI, fill models, fake exchanges, reports, DataFusion/Polars analysis.

## Phase 10 — Production shadow/live-tiny

Record-only, shadow, paper, live-tiny, scaling rules.

## Recommended first live candidate

Start with **Winner-Follow in Polymarket paper mode**, then live-tiny. Do not start with weather/crypto live execution until the Winner-Follow scanner, ranker, Kelly sizing, event log, and risk gates are working end to end. Kalshi copy-trading remains disabled unless authorized trader-level data exists.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Phase 0A — Winner-Follow first milestone

1. Add `trader-index`, `copy-signal-engine`, `kelly-sizer`, and `strategy-winner-follow` crates.
2. Build Polymarket public trader ingestion.
3. Build trader ledger reconstruction.
4. Build walk-forward ranker.
5. Build top-50 active watchlist.
6. Build copy-signal classification.
7. Build pure fractional-Kelly sizing with caps.
8. Build paper-copy execution.
9. Build live-tiny mode with hard-coded bankroll cap.
10. Only then proceed to larger source/resolver strategies.
