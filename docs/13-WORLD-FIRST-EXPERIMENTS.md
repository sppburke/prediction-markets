# 13 — World-First Experiments

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.

## Objective

List differentiated, high-upside Rust-native experiments. These are not guaranteed winners; they are where engineering depth can create unusual edge.

## 0) Real-time public-trader alpha decay map

Build a Rust system that measures how fast public leader trades lose edge after publication. For every leader, market family, odds bucket, and liquidity bucket, estimate the decay curve from milliseconds to minutes (use the delay buckets in `19-WINNER-FOLLOW-STRATEGY.md`). The output is a routing rule: copy immediately, post passively, wait for pullback, or ignore.

## 0.1) Leader-resolver hybrid veto engine

Combine Winner-Follow with resolver-source engines. Copy a leader only when the independent resolver model is neutral or supportive; downsize or block when the resolver model contradicts the leader. Hybrid strategy that starts from public skill but does not blindly follow it.

## 1) Resolver mesh auto-compiler

A Rust compiler that ingests market metadata/pages, extracts source links, classifies market family (`MarketFamily` enum in `_GLOSSARY.md`), parses time windows/tie rules/finality, emits draft `ResolverCard`s, and routes high-risk cards to human review.

## 2) Dual-resolver crypto lab

A paired engine for Chainlink-style Polymarket crypto and benchmark-window Kalshi crypto, backed by shared exchange microstructure and cross-venue basis modeling.

## 3) Weather station finality engine

Station-level observations, local-standard-time windows, HRRR/NBM forecasts, max/min finality, final climate report watcher, and cross-venue station compatibility.

## 4) Official-page first-seen network

Rust page watchers that hash responses, track first-seen timestamps, detect semantic table/rank changes, and archive replayable snapshots.

## 5) Cross-venue fake-hedge detector

Classify lookalike markets using `CompatibilityClass` (`09-CROSS-VENUE-MISMATCHES-AND-HEDGES.md`). Warn before strategy assumes a hedge.

## 6) Deterministic fake exchange

Rust simulators for Kalshi and Polymarket with snapshots, deltas, partial fills, queue, cancel races, disconnects, maintenance windows, and settlement events.

## 7) Rust-native document verifier

Official document capture, text hash, section-aware parser, exact phrase matcher, constrained ML classifier, deterministic verifier.

## 8) Latency attribution profiler

Trace source delay, parser delay, bus delay, model delay, strategy delay, routing delay, venue ack delay, and fill delay against the per-stage budgets in `_GLOSSARY.md`.

## Promotion rule

An experiment cannot become live until source legality, resolver fixtures, replay corpus, shadow-mode metrics, false-positive modes, and live-tiny risk limits are complete. Promotion gates for Winner-Follow sub-experiments use `_GLOSSARY.md` "Promotion criteria — quantified".
