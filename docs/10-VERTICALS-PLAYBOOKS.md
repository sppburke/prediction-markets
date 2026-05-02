# 10 — Vertical Playbooks

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## 1) Short-horizon crypto

**Rust crates:** `source-crypto`, `model-crypto`, `venue-kalshi`, `venue-polymarket`, `cross-venue`.

**Sources:** Chainlink where specified, Kalshi benchmark source where specified, exchange books/trades, Pyth/reference feeds where permitted, venue books/RTDS.

**Models:** oracle shadow, benchmark-window accumulator, microprice predictor, volatility shock detector, lead/lag model.

**Trap:** spot price is not necessarily the resolver.

## 2) Weather

**Rust crates:** `source-weather`, `model-weather`.

**Sources:** NWS climate report, METAR/station observations, AWC, station history page, HRRR/NBM, independent station networks.

**Models:** station high/low nowcast, local-standard-time engine, report finality detector, cross-venue station compatibility.

**Trap:** station mismatch and time-window mismatch.

## 3) Sports

**Rust crates:** `source-sports`, `model-sports`.

**Sources:** official league result/status, licensed live feed if permitted, venue sports sockets, official box scores.

**Models:** game finite-state machine, correction detector, live win-condition evaluator.

**Trap:** unofficial score feed can be faster and wrong.

## 4) Macro/government

**Rust crates:** `source-macro`, `model-macro`.

**Sources:** BLS, BEA, Census, Fed, Treasury.

**Models:** release watcher, typed table parser, consensus/threshold evaluator, revision detector.

**Trap:** wrong table, adjustment, vintage, or revision.

## 5) Charts/rankings

**Rust crates:** `source-charts`, `model-charts`.

**Sources:** Spotify, Netflix, Apple App Store, official ranking pages.

**Models:** rank snapshot parser, publication detector, entity normalizer, rank movement nowcaster.

**Trap:** region/date/page-cache mismatch.

## 6) Company/text markets

**Rust crates:** `source-documents`, `model-documents`.

**Sources:** SEC EDGAR, company IR pages, official press releases, licensed transcripts.

**Models:** exact phrase verifier, document hash, section parser, Rust-native classifier with deterministic validation.

**Trap:** LLM hallucination and corrected transcripts.

## 7) Event feeds

**Rust crates:** `source-events`, `model-events`.

**Sources:** USGS, NHC, NASA FIRMS, official public agency feeds.

**Models:** first-confirmation detector, geospatial filter, threshold evaluator, preliminary/final revision handler.

**Trap:** preliminary data revisions.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## 0) Winner-Follow vertical — first implementation

**Rust crates:** `trader-index`, `copy-signal-engine`, `kelly-sizer`, `strategy-winner-follow`, `venue-polymarket`, optional `venue-kalshi` for authorized trader data.

**Goal:** compound bankroll by copying selected leaders whose public trades historically generate positive follower log-growth after copy delay and costs.

**Primary source:** Polymarket public leader/profile/trade/position/activity data.

**Secondary use:** resolver/source playbooks validate or veto copied trades. Example: if a top leader buys a weather contract but the weather resolver engine strongly disagrees, reduce size or block.

**Risk:** crowding, false skill, uncopiable speed, hidden exits, low liquidity, one-off luck, and strategy drift.
