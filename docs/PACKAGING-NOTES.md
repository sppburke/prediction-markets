# Packaging Notes — v5 Winner-Follow Rust 1.95.0 Pass

This package updates the Rust-only Polymarket + Kalshi edge system so the first strategy is **Winner-Follow**.

## v5 changes

- Updated all markdown files to target stable **Rust 1.95.0** with Rust 2024 Edition.
- Made Winner-Follow the first deployable strategy.
- Added trader-discovery, ledger reconstruction, leader ranking, copy-signal classification, and fractional-Kelly requirements.
- Winner-Follow is per-wallet copy-trading; the operator/funder/cluster machinery (native Polygon funding/collateral graph, `operator-graph`, inherited-prior first-trade incubator, cluster-coordination shadow mode) was removed in #326 — see `docs/28-OPERATOR-GRAPH-ARCHIVE.md`.
- Clarified that Polymarket is the primary public trader-copy venue.
- Clarified that Kalshi public trades are anonymous market-flow data unless official/public/authorized trader-level attribution exists.
- Clarified that CrowdIntel-style funding clusters are research inspiration only, not a production dependency without an authorized replayable API/export.
- Added AWS + Git deployment requirements.
- Added `AGENTS.md` and `SKILLS.md` for coding agents.
- Added:
  - `19-WINNER-FOLLOW-STRATEGY.md`
  - `20-AWS-GIT-OPERATIONS.md`
  - `21-RESEARCH-AND-SOURCE-DISCOVERY.md`
  - `AGENTS.md`
  - `SKILLS.md`

## v5.1 structural pass (this package)

Documentation reorganization to make the spec concrete, explicit, and non-redundant:

- Added `_BASELINE.md`: single-source-of-truth for the Rust-only rule, toolchain pin, lints, and common acceptance gate. Other files reference it instead of restating.
- Added `_GLOSSARY.md`: vocabulary (wallet/trader/leader/candidate), type aliases, resolver-card sub-types with a fully populated example, latency budget (p50/p95/p99), per-venue rate-limit table, idempotency-key definition (`observed_at_bucket`), promotion/demotion criteria with KS p-value and z-score thresholds, "very liquid market" gate, source freshness defaults, and concrete defaults for previously vague qualifiers.
- Hosted the canonical Winner-Follow risk-cap TOML, Kelly-fraction table, mode definitions, promotion ladders, and risk-block-with-halt-scope taxonomy in `19-WINNER-FOLLOW-STRATEGY.md`. Other docs no longer restate these values; they reference `19-`.
- Stripped the duplicated Rust-only-rule preamble and common-acceptance-gate footer from every numbered phase/venue/source doc.
- Added `BacktestReport` + `WinnerFollowReport` (extension struct) in `05-` covering turnover, copy-delay percentiles, edge decay by delay bucket, leader churn, demotion-cause histogram, paper-vs-backtest KS p-value, and z-score.
- Picked **ECS Fargate** as the default compute and **RDS Aurora-Postgres** as the default metadata database in `06-` and `20-`, with explicit migration triggers.
- Added a per-class TTL table to `15-SOURCES.md` and `Last checked` / `Re-verify by` columns to every link; `21-` references it.
- Annotated ranking weights in `03-` as illustrative starting values tuned by walk-forward optimization.
- Cross-linked the "Cross-venue mismatch" entry in `00-` "Edge taxonomy" to the `CompatibilityClass` enum in `09-`.
- Reconciled `source-trader-polymarket`/`source-trader-kalshi` naming to a single `source-trader` crate with venue submodules.
- Updated the root `README.md` from a two-line stub to a pointer at `docs/`.
- Updated `18-CODEX-RUST-BOOTSTRAP-PROMPT.md` to reference `01-` for the crate list rather than restating it.

## Interpretation

Winner-Follow is not guaranteed profit and not a substitute for risk management. It is a measurable, replayable strategy hypothesis: selected public traders/operators may have repeatable skill that survives copy delay, liquidity, costs, and fractional-Kelly caps. The full edge claim is the inequality at the top of `19-WINNER-FOLLOW-STRATEGY.md`.

## Next step

Use `18-CODEX-RUST-BOOTSTRAP-PROMPT.md`, `AGENTS.md`, `SKILLS.md`, `_BASELINE.md`, and `_GLOSSARY.md` to bootstrap the Git repository. Build Polymarket paper-copy Winner-Follow before any scaled live trading.
