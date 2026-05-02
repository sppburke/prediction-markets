# 17 — Rust Implementation Roadmap

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) "Phase vs Strategy index" for the distinction between Phase N (build order) and Strategy N (strategy identity).

## Phase 0 — Toolchain and workspace

Create workspace, strict lints, CI, `rust-toolchain.toml`, fake service binary, config loader, health endpoint.

## Phase 0A — Winner-Follow first milestone

1. Add `source-onchain-polygon`, `operator-graph`, `trader-index`, `copy-signal-engine`, `kelly-sizer`, and `strategy-winner-follow` crates.
2. Build Polymarket public trader ingestion.
3. Build `source-onchain-polygon` for public Polygon funding/collateral events and replay fixtures.
4. Verify Polymarket proxy-wallet, funder, pUSD, deposit, and bridge/onramp mapping against official docs and historical chain fixtures (per `21-`).
5. Build `operator-graph` for strict funder-root clustering, operator identities, inherited priors, and anti-gaming flags using thresholds from `_GLOSSARY.md`.
6. Build trader ledger reconstruction with wallet-level facts and operator annotations.
7. Build walk-forward operator-aware ranker.
8. Build top-`active_watchlist_size` watchlist.
9. Build copy-signal classification with separate action and signal-kind fields.
10. Build pure fractional-Kelly sizing with normal, inherited-prior, and cluster-coordination fractions (canonical TOML in `19-`).
11. Build risk gates with operator, funder, inherited-prior, and cluster caps (canonical TOML in `19-`).
12. Build paper-copy execution.
13. Build live-tiny mode for ordinary leader-follow with hard-coded bankroll cap.
14. Keep inherited-prior first-trade in paper and cluster-coordination in shadow until separately validated per `19-` "Promotion ladder".
15. Only then proceed to larger source/resolver strategies.

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

Start with **Winner-Follow in Polymarket paper mode**, then live-tiny for ordinary leader-follow only. Do not start with weather/crypto live execution until the Winner-Follow scanner, operator-aware ranker, Kelly sizing, event log, and risk gates are working end to end. Kalshi copy-trading remains disabled unless authorized trader-level data exists.

`inherited_prior_first_trade` and `cluster_coordination` modes share the Winner-Follow infrastructure but remain shadow/paper until they have separate walk-forward validation per `19-` "Promotion ladder".
