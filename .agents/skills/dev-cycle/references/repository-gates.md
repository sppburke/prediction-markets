# Prediction-Markets Repository Gates

Read this reference for every `dev-cycle`. It owns repository-specific sources, architecture,
change-type verification, and PR evidence. The parent skill owns lifecycle and stopping behavior.

## Contents

- Mandatory sources
- Architecture and domain invariants
- Rust safety
- Change-type classification
- Focused tests and scenarios
- Gate exceptions
- Pull request evidence

## Mandatory sources

Read before editing:

1. `AGENTS.md` and `docs/_EVIDENCE-FIRST.md`.
2. `docs/_BASELINE.md` — toolchain, lints, and common acceptance contract.
3. `docs/_GLOSSARY.md` — vocabulary, types, defaults, idempotency, rate/latency, promotion, and config.
4. `docs/19-WINNER-FOLLOW-STRATEGY.md` — risk caps, Kelly, eligibility, modes, and halt behavior.
5. `docs/16-RUST-WORKSPACE-ARCHITECTURE.md` and `docs/SKILLS.md`.
6. The relevant phase, venue, source, deployment, or runbook.
7. Current official external documentation when the source TTL or material contract requires it.

Authority on conflict:

`_BASELINE` > `_GLOSSARY` > `19-WINNER-FOLLOW-STRATEGY` > phase/venue/source docs > bootstrap prompt `18`.

Fix the lower-priority source. Never duplicate a numeric default outside `_GLOSSARY.md` or the
canonical TOML in `19-`.

## Architecture and domain invariants

Dependency direction:

`core-types → config/event-log/resolver-card/source-core/venue-core → source-*/venue-* → model-* → strategy-* → execution/risk → replay/backtest/service/cli`

- Source crates never depend on venue; venue never depends on strategy.
- Strategies emit `OrderIntent`; only execution routers submit orders.
- `risk-engine` stays pure, deterministic, and free of I/O.
- Production and replay use the same crates and decisions are reproducible from recorded inputs and
  version identifiers.
- Wallet, trader, leader, and candidate are distinct glossary terms. Ranking and concentration
  accounting are per-wallet.
- Wallet-to-operator clustering and on-chain funder discovery were removed in #326. Archived
  machinery is not a current semantic owner or live-decision input.
- Public Kalshi trades are anonymous. Do not add trader-copy attribution without official trader-level
  data or explicitly authorized portfolio access.
- CrowdIntel/UI scraping or opaque third-party scores are not live decision inputs.
- Runtime approvals, risk caps, Kelly ceilings, idempotency shape, promotion ladders, and mode defaults
  come from `_GLOSSARY.md` and `19-`, not recollection.

## Rust safety

- First-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are
  Rust 2024 on pinned Rust 1.95.0.
- No `unsafe` by default, unchecked `panic!`, or production `unwrap`/`expect`.
- No raw `f64` for money, prices, probabilities, quantities, fees, slippage, or balances. Use
  `rust_decimal` or validated integer ticks/cents/bps.
- Avoid narrowing `as` casts; use validated `TryFrom` or retain the wider type.
- No unbounded hot-path channels. Declare backpressure/drop/retry policy.
- External inputs and decisions retain the raw hash, source ID, timestamps, schema/parser version,
  config/model/resolver/adapter versions required for replay.
- Stubs and missing evidence fail closed. Do not choose a sentinel that passes eligibility or risk.
- Public methods whose output depends on prior ingestion, a non-sentinel clock, or call ordering must
  document the precondition.

## Change-type classification

Classify by the actual diff; mixed changes take the union.

### `rust`

Touches Rust source, `Cargo.toml`, or `Cargo.lock` beyond a lockfile-only tooling update.

Run the full acceptance gate after targeted tests:

```bash
rustc --version                                                        # must contain 1.95.0
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --doc --workspace --all-features
cargo nextest run --workspace --all-features
cargo deny check
cargo audit
cargo metadata --locked --format-version 1 > /dev/null
```

Run `cargo fmt --all` before this final gate. No code/test edit may occur between the passing gate and
the commit/push it supports.

Before the gate, inspect added Rust lines for narrowing casts, `unwrap`, `expect`, `panic!`, `unsafe`,
FFI, raw pointers, and raw financial floats. Test-only allowances must be explicit and scoped.

### `docs`

Touches only Markdown or documentation. Run:

```bash
git diff --check
```

Also run any link, source-date, generated-doc, or contract check relevant to the edited truth. Do not
run the Cargo gate merely because the workflow advanced.

### `tooling`

Touches scripts, skills, agent projections, CI, `deny.toml`, or toolchain/config files without runtime
Rust behavior.

- Run syntax checks and the exact operational command/contract touched.
- Skill/instruction changes run:

```bash
bash scripts/check_skill_parity.sh
python3 scripts/check_agent_workflow_contract.py
git diff --check
```

- `deny.toml` also runs `cargo deny check`.
- `rust-toolchain.toml` also runs `rustc --version` and `cargo metadata --locked`.
- CI YAML is validated by the repository's available workflow/YAML checks; GitHub CI is authoritative.
- Run the full Rust gate only when tooling can alter build, test, packaging, deployment, or generated
  runtime behavior.

## Focused tests and scenarios

Run the narrowest useful check first, for example:

```bash
cargo nextest run -p <crate> <test_name>
```

Add deterministic scenarios for externally observable behavior, cross-crate wiring, replay, risk, or
failure recovery not proven by unit/property tests. Use the existing convention:

`crates/<crate>/tests/scenario_<name>.rs`, gated by the crate's `scenario` feature when applicable.

For every scenario define `Scenario`, `PASS`, and `FAIL` before running. Use fixed timestamps/RNG,
recorded fixtures, no live network, and isolated temporary storage. A scenario failure blocks.

## Gate exceptions

- A failure is pre-existing only after it reproduces on an unmodified disposable `origin/main` at the
  same revision and environment.
- A new test that fails or a previously passing path broken by the branch is a regression.
- A nondeterministic test is a determinism defect; identify and fix the clock/RNG/shared-state source.
- If a required command is unavailable locally, record the exact command/reason in the PR. CI is
  authoritative only for that unavailable command; it does not excuse other local failures.

## Pull request evidence

Every PR body includes:

- Summary — user/system behavior and why.
- Source issue (`Closes #N`) when applicable.
- Scope tag — `rust`, `docs`, `tooling`, or mixed union.
- Files changed.
- Tests/checks with exact commands and results.
- Scenario verification, or `N/A` with reason.
- Gate exceptions.
- Review outcomes, including reasoned declines.
- Replay impact.
- Risk/financial impact.
- API/source docs checked, including `docs/15-SOURCES.md` updates when re-verified.
- Deployment/config/migration impact.
- Rollback plan.
- Shortcuts/hacks/workarounds, explicitly `none` when none.

Use body-file or standard-input transport. Never interpolate arbitrary issue, plan, or reviewer text
inside a shell command.
