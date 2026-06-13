# 17 — Rust Implementation Roadmap

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) "Phase vs Strategy index" for the distinction between Phase N (build order) and Strategy N (strategy identity).

## Phase 0 — Toolchain and workspace

Create workspace, strict lints, CI, `rust-toolchain.toml`, fake service binary, config loader, health endpoint.

## Phase 0A — Winner-Follow first milestone

1. Add `trader-index`, `copy-signal-engine`, `kelly-sizer`, and `strategy-winner-follow` crates.
2. Build Polymarket public trader ingestion.
3. Build trader ledger reconstruction with wallet-level facts.
4. Build walk-forward per-wallet ranker.
5. Build top-`active_watchlist_size` watchlist.
6. Build copy-signal classification with action labeling.
7. Build pure fractional-Kelly sizing (canonical TOML in `19-`).
8. Build risk gates with per-wallet concentration and drawdown caps (canonical TOML in `19-`).
9. Build paper-copy execution.
10. Build live-tiny mode for leader-follow with hard-coded bankroll cap.
11. Only then proceed to larger source/resolver strategies.

## Phase 1 — Core types and event log

Implement typed IDs/prices/probabilities/timestamps from `_GLOSSARY.md`, event envelope, local append-only log, raw payload hashes, replay reader.

## Phase 2 — Fake connectors

Build fake source and fake venue connectors. They run end-to-end without real APIs.

## Phase 3 — Kalshi adapter

REST/WS client, order book reconstruction, market discovery, queue position, order journal, reconciliation, demo/paper mode first.

## Phase 4 — Polymarket adapter

Market/user/sports/RTDS sockets, market discovery, signing module with fixtures, order journal, reconciliation, dry-run mode first.

## Phase 5 — Resolver cards

Manual authoring, schema, validator, draft compiler, human review workflow, sample cards for weather/crypto/sports/macro/charts (sub-types in `_GLOSSARY.md`).

## Phase 6 — Sources

Build in this order: trader-intelligence, crypto, weather, macro, charts, sports, events. Every source gets fixtures, raw hashes, health metrics, and replay.

## Phase 7 — Models

Finalizers, benchmark windows, nowcasters, cross-venue compatibility, calibration reports, model artifacts.

## Phase 8 — Strategy/risk

Cost model, latency budget, risk gates, kill switches, order-intent pipeline.

## Phase 9 — Backtesting

Replay CLI, fill models, fake exchanges, reports (`BacktestReport` + `WinnerFollowReport` from `05-`), DataFusion/Polars analysis.

## Phase 10 — Production shadow/live-tiny

Record-only, shadow, paper, live-tiny, scaling rules. Promotion gates per `_GLOSSARY.md` and `19-`.

## Recommended first live candidate

Start with **Winner-Follow in Polymarket paper mode**, then live-tiny for leader-follow. Do not start with weather/crypto live execution until the Winner-Follow scanner, per-wallet ranker, Kelly sizing, event log, and risk gates are working end to end. Kalshi copy-trading remains disabled unless authorized trader-level data exists.
