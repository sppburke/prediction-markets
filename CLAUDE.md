# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository state

`prediction-edge` is a Rust 1.95.0 / Rust 2024 trading and research system for Polymarket and Kalshi. Phase 0 (toolchain, workspace, CI, fake service binary) is in place: `crates/`, `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `deny.toml`, and `.github/workflows/ci.yml` exist and the full acceptance gate is green. Subsequent phases per `docs/17-RUST-IMPLEMENTATION-ROADMAP.md` add core types, event log, sources, venue adapters, and strategies.

The root `AGENTS.md` is the human contributor guide; this `CLAUDE.md` is for coding-agent context. They are complementary — the contributor guide covers commit/PR conventions and dev workflow at a human level, while this file captures architecture, doc authority order, and the gotchas a fresh agent needs to avoid.

The CI gate is exactly:

```bash
rustc --version              # must contain 1.95.0
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --doc --workspace --all-features
cargo nextest run --workspace --all-features
cargo deny check
cargo audit
cargo metadata --locked
```

A single test runs as `cargo nextest run -p <crate> <test_name>` once `cargo-nextest` is present. Document any gate that cannot be run.

## Commit and PR conventions

Commit messages: concise imperative summaries, optionally with a scoped prefix. Examples from history: `Restructure docs: canonical baseline...`, `Update Winner-Follow operator graph plans`, `Add repository contributor guide`.

Pull requests must include: summary, files changed, tests run, replay impact, risk impact, API docs checked (with `Last checked` updates in `docs/15-SOURCES.md` when a venue/source doc was re-verified), deployment impact, and rollback plan. Link issues when available; add screenshots only for visual changes.

## Doc naming and authoring

Documentation files use numbered prefixes for sequence and scope (e.g. `docs/03-PHASE-MODEL-ENGINE.md`, `docs/07-VENUE-KALSHI.md`). Underscore-prefixed files (`_BASELINE.md`, `_GLOSSARY.md`) are canonical references that other docs link to. Keep new docs focused; cross-reference canonical values (Winner-Follow caps in `19-`, types and defaults in `_GLOSSARY.md`) instead of duplicating them.

## Mandatory reading before editing anything

The docs are deliberately structured so that numeric thresholds, types, and conventions live in one place each. Read in this order:

1. `docs/_BASELINE.md` — Rust toolchain pin, workspace lints, common acceptance gate.
2. `docs/_GLOSSARY.md` — vocabulary (wallet/trader/operator/leader/candidate), `core-types` aliases, resolver-card sub-types with a fully populated example, latency budget (p50/p95/p99), per-venue rate-limit table, idempotency-key definition (`observed_at_bucket`), anti-gaming flag thresholds, "close to simulation" definition (KS p≥0.10, abs(z)≤2.0), promotion/demotion criteria, "very liquid market" gate, and ~50 default config values that the prose elsewhere refers to by name.
3. `docs/19-WINNER-FOLLOW-STRATEGY.md` — canonical risk-cap TOML, Kelly fractions per mode, eligibility thresholds, three-mode promotion ladders, risk-block taxonomy with halt scope.
4. `docs/AGENTS.md` and `docs/SKILLS.md`.
5. The specific phase/venue/source doc relevant to the task.
6. Latest official venue/source docs per `docs/21-RESEARCH-AND-SOURCE-DISCOVERY.md` — re-verification TTLs are in `docs/15-SOURCES.md`.

## Doc authority order (when files disagree)

Conflicts are common when restating values; resolve them this way and fix the lower-priority doc:

1. `_BASELINE.md`
2. `_GLOSSARY.md`
3. `19-WINNER-FOLLOW-STRATEGY.md`
4. Phase/venue files (`00–17`, `20`, `21`)
5. Bootstrap prompt (`18`)

Specifically: any Winner-Follow numeric value (caps, Kelly fractions, eligibility thresholds, mode states, idempotency-key shape) lives canonically in `19-` or `_GLOSSARY.md`. Other docs reference these — do not introduce a second copy.

## Architecture in one screen

The system is a **Rust workspace** of small crates with strict dependency direction. Source events flow through a single append-only event log; production and replay use the same crates so every live decision is reproducible from `(event log hash, git SHA, config hash, resolver-card version, model artifact version, venue adapter version)`.

```
core-types
  -> config, event-log, resolver-card, source-core, venue-core
      -> source-* and venue-*
      -> operator-graph
      -> model-core and model-*
      -> strategy-core, trader-index, copy-signal-engine, kelly-sizer,
         strategy-winner-follow, execution-core, risk-engine
      -> replay, backtest, service, cli
```

Hard rules from `docs/16-RUST-WORKSPACE-ARCHITECTURE.md`:

- Source crates cannot depend on venue crates.
- Venue crates cannot depend on strategy crates.
- Strategy crates cannot submit orders — they emit `OrderIntent`; only `execution-core` submits.
- `risk-engine` is pure: no network, deterministic from typed snapshots.
- `operator-graph` is pure: no I/O, deterministic from event snapshots + config; never network or DB.
- Replay uses production crates, not duplicate research logic.
- `unsafe` is forbidden by default; per-crate override requires a separate review.
- No raw `f64` for money/prices/quantities/probabilities — use `rust_decimal` or integer ticks/cents/bps.
- No unbounded channels in hot paths; backpressure policy is declared per connector.
- No `unwrap`/`expect`/unchecked `panic!` in production crates (clippy-denied).

## Strategy 0 (Winner-Follow) and Phase 0/0A are different axes

`docs/_GLOSSARY.md` "Phase vs Strategy index": **Strategy N** is the strategy identity (Strategy 0 = Winner-Follow, the first deployable strategy). **Phase N** is the build sequence (Phase 0 = toolchain, Phase 0A = Winner-Follow first milestone). Don't conflate them.

Winner-Follow ships first because it can be built from public Polymarket trader/profile/trade/position/activity data plus public Polygon funding/collateral events — no proprietary weather network, oracle predictor, or sports feed required. It has three modes with separate promotion ladders:

- `leader_follow` — ordinary copy-trading; defaults to `live_tiny` after passing gates.
- `inherited_prior_first_trade` — fresh wallet linked to a known operator; defaults to **paper**.
- `cluster_coordination` — same-operator clusters entering same `(market, outcome, side)` within window; defaults to **shadow**.

Promotion of one mode never promotes another (`docs/19-` "Promotion ladder").

## Things that are easy to get wrong

- **Vocabulary**: "wallet" ≠ "trader" ≠ "operator" ≠ "leader" ≠ "candidate". `_GLOSSARY.md` defines each. When operator identity is confident, ranking and risk caps apply at the operator level, not the wallet level.
- **Polymarket funding identity**: never infer from a "first USDC sender" rule. Use the proxy-wallet, pUSD collateral, deposit, and bridge/onramp evidence in `source-onchain-polygon` + `operator-graph`. If the mapping isn't proven for a wallet class, inherited-prior and cluster-coordination signals stay shadow-only.
- **Kalshi public trades are anonymous**. Never attribute a public Kalshi trade to a leaderboard trader. Kalshi copy-trading is enabled only for explicitly authorized portfolio access or future official trader-level endpoints.
- **CrowdIntel** is research inspiration, not a production input. UI scraping or opaque scores are forbidden in live decisions.
- **Idempotency key**: `(leader, source_trade_id, market, outcome, side, observed_at_bucket)` where `observed_at_bucket = floor(observed_at_ms / 1_000)` (1-second buckets). Cluster-coordination signals add `operator_id`. Defined in `_GLOSSARY.md`.
- **Approval flags**: `flip_human_approved` and `kelly_fraction_above_default_human_approved` are first-class inputs to `risk-engine`. They require a signed config change; flipping them at runtime is forbidden.
- **Kelly sizing**: `c` is the **net** price (after fees, expected slippage, adverse-selection buffer). Reject trades where `f_live > 0` only because `p` is stale.
- **Resolver card is mandatory** before trading any market. Sub-types and a fully populated example are in `_GLOSSARY.md`.

## When proposing changes to numeric defaults

Add the number to `_GLOSSARY.md` (or the canonical TOML in `19-`) and reference it from prose elsewhere. Do not introduce a second copy in another doc. The `AGENTS.md` rule is: "Do not introduce any new numeric threshold without adding a default to `_GLOSSARY.md` or the canonical TOML in `19-`."
