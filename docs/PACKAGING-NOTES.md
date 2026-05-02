# Packaging Notes — v5 Winner-Follow Rust 1.95.0 Pass

This package updates the Rust-only Polymarket + Kalshi edge system so the first strategy is **Winner-Follow**.

## Changes

- Updated all markdown files to target stable **Rust 1.95.0** with Rust 2024 Edition.
- Made Winner-Follow the first deployable strategy.
- Added trader-discovery, ledger reconstruction, leader ranking, copy-signal classification, and fractional-Kelly requirements.
- Added operator-aware Winner-Follow planning with native Polygon funding/collateral graph, `operator-graph`, inherited-prior first-trade incubator, and cluster-coordination shadow mode.
- Clarified that Polymarket is the primary public trader-copy venue.
- Clarified that Polymarket proxy-wallet, pUSD, deposit/onramp, and funder behavior must be verified before funding identity is used for sizing.
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

## Interpretation

Winner-Follow is not guaranteed profit and not a substitute for risk management. It is a measurable, replayable strategy hypothesis: selected public traders/operators may have repeatable skill that survives copy delay, liquidity, costs, and fractional-Kelly caps.

## Next step

Use `18-CODEX-RUST-BOOTSTRAP-PROMPT.md`, `AGENTS.md`, and `SKILLS.md` to bootstrap the Git repository. Build Polymarket paper-copy Winner-Follow before any scaled live trading. Keep inherited-prior first-trade and cluster-coordination modes paper/shadow until separately validated.
