# 13 — World-First Experiments

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

List differentiated, high-upside Rust-native experiments. These are not guaranteed winners; they are where engineering depth can create unusual edge.

## 1) Resolver mesh auto-compiler

A Rust compiler that ingests market metadata/pages, extracts source links, classifies market family, parses time windows/tie rules/finality, emits draft `ResolverCard`s, and routes high-risk cards to human review.

## 2) Dual-resolver crypto lab

A paired engine for Chainlink-style Polymarket crypto and benchmark-window Kalshi crypto, backed by shared exchange microstructure and cross-venue basis modeling.

## 3) Weather station finality engine

Station-level observations, local-standard-time windows, HRRR/NBM forecasts, max/min finality, final climate report watcher, and cross-venue station compatibility.

## 4) Official-page first-seen network

Rust page watchers that hash responses, track first-seen timestamps, detect semantic table/rank changes, and archive replayable snapshots.

## 5) Cross-venue fake-hedge detector

Classify lookalike markets into same resolver, same underlying/different resolver, correlated, title-only, or incompatible. Warn before strategy assumes a hedge.

## 6) Deterministic fake exchange

Rust simulators for Kalshi and Polymarket with snapshots, deltas, partial fills, queue, cancel races, disconnects, maintenance windows, and settlement events.

## 7) Rust-native document verifier

Official document capture, text hash, section-aware parser, exact phrase matcher, constrained ML classifier, deterministic verifier.

## 8) Latency attribution profiler

Trace source delay, parser delay, bus delay, model delay, strategy delay, routing delay, venue ack delay, and fill delay.

## Promotion rule

An experiment cannot become live until source legality, resolver fixtures, replay corpus, shadow-mode metrics, false-positive modes, and live-tiny risk limits are complete.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## 0) Real-time public-trader alpha decay map

Build a Rust system that measures how fast public leader trades lose edge after publication. For every leader, market family, odds bucket, and liquidity bucket, estimate the decay curve from milliseconds to minutes. The output is a routing rule: copy immediately, post passively, wait for pullback, or ignore.

## 0.1) Leader-resolver hybrid veto engine

Combine Winner-Follow with resolver-source engines. Copy a leader only when the independent resolver model is neutral or supportive; downsize or block when the resolver model contradicts the leader. This creates a hybrid strategy that starts from public skill but does not blindly follow it.

## 0.2) Operator-aware first-trade incubator

Build a replayable Polygon funding/collateral graph for Polymarket proxy wallets and use it to detect when a fresh wallet's first meaningful trade is linked to a known high-quality operator. The experiment is useful only if proxy/funder/collateral mapping is public and reproducible, inherited priors remain heavily shrunk, anti-gaming flags are effective, and paper-mode outcomes beat baseline after latency and costs.
