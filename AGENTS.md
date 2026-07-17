# Repository Agent Standard

## Scope and precedence

`AGENTS.md` is the canonical shared instruction file for this repository. `CLAUDE.md` must be a
relative symlink to `AGENTS.md` so supported coding-agent hosts receive the same project standard.
The managed projections require a symlink-capable Git checkout (Linux, macOS, or WSL with symlinks
enabled); a native checkout that materializes symlinks as link-text files is unsupported.

Follow the active instruction hierarchy. An applicable repository skill may specialize execution,
approval gates, mutation boundaries, verification, stopping conditions, and output. Reconcile
compatible rules and report a genuine conflict rather than silently choosing one.

The user's request defines the desired outcome. It does not authorize unrelated changes, production
mutation, live trading, destructive recovery, or external side effects outside the selected workflow.

## Repository workflow catalog

The canonical, host-neutral workflow catalog is `.agents/skills/*/SKILL.md`.

- Codex reads `.agents/skills/` as the repository catalog.
- Claude Code reads `.claude/skills/`, whose managed per-skill directory symlinks point to
  `.agents/skills/`.
- A runtime without repository-skill discovery must inspect `.agents/skills/*/SKILL.md` directly.

For every task, select the smallest skill set whose frontmatter covers the request. If the user names
an available skill, use it. Before acting, read every selected `SKILL.md` completely and every
reference it marks required for the task. Do not load unrelated references merely because they
exist.

The selected skill owns its task-specific lifecycle. The primary agent remains responsible for
applying it correctly, verifying helper output, and preserving user work.

Skill content must remain host-neutral: describe capabilities and outcomes, not model names,
vendor-specific gates, or one host's tool spelling. When a skill changes, edit only `.agents/skills`,
then run:

```bash
bash scripts/check_skill_parity.sh --sync
bash scripts/check_skill_parity.sh
python3 scripts/check_agent_workflow_contract.py
```

Commit the canonical change and managed Claude projections together. Never edit a generated
projection independently.

## Mission and canonical sources

This repository hosts `prediction-edge`, a Rust 1.95.0 / Rust 2024 trading and research workspace
for Polymarket and Kalshi. It is event-sourced and replayable; Winner-Follow is the first strategy.

Before planning or editing, read in this order:

1. `docs/_BASELINE.md` — toolchain, lints, and common acceptance contract.
2. `docs/_GLOSSARY.md` — canonical vocabulary, types, config defaults, rate/latency limits,
   idempotency, and promotion rules.
3. `docs/19-WINNER-FOLLOW-STRATEGY.md` — canonical risk caps, Kelly fractions, eligibility, modes,
   approvals, and halt behavior.
4. `docs/AGENTS.md` and `docs/SKILLS.md`.
5. `docs/16-RUST-WORKSPACE-ARCHITECTURE.md` plus the relevant phase, venue, source, or runbook.
6. Current official venue/source documentation when required by
   `docs/21-RESEARCH-AND-SOURCE-DISCOVERY.md`; source TTL and `Last checked` policy lives in
   `docs/15-SOURCES.md`.

`README.md` is the overview, not the canonical home for duplicated thresholds or type contracts.
Docs use numbered prefixes for sequence and scope; underscore-prefixed files are canonical shared
references. Keep new docs focused and link canonical values instead of restating them.

Doc authority on conflict:

1. `docs/_BASELINE.md`;
2. `docs/_GLOSSARY.md`;
3. `docs/19-WINNER-FOLLOW-STRATEGY.md`;
4. phase/venue/source docs (`00–17`, `20`, `21`, and later task-specific docs);
5. bootstrap prompt `docs/18-CODEX-RUST-BOOTSTRAP-PROMPT.md`.

Fix the lower-priority source. Do not duplicate a numeric threshold: add the default only to
`_GLOSSARY.md` or the canonical TOML in `19-`, then reference it elsewhere.

**Phase N** is build sequence; **Strategy N** is strategy identity. They are different axes. Use
`docs/_GLOSSARY.md` terminology exactly.

## Evidence-first verification

Apply `docs/_EVIDENCE-FIRST.md` to factual claims in answers, planning, diagnosis, review, and
self-checks.

1. State the material claim internally and break it into concrete conditions.
2. Pre-commit evidence that would falsify it and search for that first.
3. Inspect the highest applicable authority for the exact revision, environment, venue, wallet class,
   and time window.
4. Continue when the first source is incomplete, indirect, stale, or scoped to a different target.
5. Reconcile conflicts explicitly and state only what the evidence supports.

Evidence order is claim-specific:

- **T1:** current repository code/config/schema/lockfiles/tests, reproducible derivations, and scripted
  system-of-record queries.
- **T2:** commands, builds, logs, deployments, live APIs, and recorded I/O captured now.
- **T3:** repository specifications and exact-version official primary documentation read now.
- **T4:** indirect sources, summaries, prior-session recollection, and intuition; use only when higher
  applicable authority cannot resolve the claim and expose the limitation.

Hypotheses may guide investigation; they are not conclusions. A test proves only the failure modes it
actually exercises. Current checkout code proves that revision's implementation; deployed state
proves the target environment; specifications prove intended contracts. None substitutes for another.

When proof is blocked, report:

1. **Checked:** exact sources/commands inspected.
2. **Showed:** what they establish.
3. **Unknown:** what remains unresolved and why.
4. **Needed:** the exact evidence or access that would resolve it.

## Engineering decision standard

Apply these principles together and in order:

1. **Preserve intent fidelity.** Implement the stated behavior and observable acceptance criteria;
   preserve explicit non-goals.
2. **Require evidence-backed correctness.** Material assumptions and contracts must agree with
   current code, schema, config, tests, runtime evidence, and the rest of the change.
3. **Make the minimum complete deployable change.** Include all required consumers, event/replay
   effects, data transitions, invalidation, config, failure behavior, observability, rollout,
   recovery, documentation, and focused proof proportionate to risk.
4. **Reuse the correct semantic owner.** Extend existing crates, traits, types, flows, clients,
   config, scripts, and tests only when their contracts genuinely fit.
5. **Follow repository precedent when reuse is impossible.** Prefer explicit invariants and canonical
   current owners over frequently repeated legacy shapes. Justify every new durable surface.
6. **Optimize the scoped whole system.** Compare the strongest simpler repository-native alternative
   for material non-obvious choices.
7. **Prefer structural simplicity.** Minimize durable surfaces, states, contracts, indirection, and
   control paths; keep one authoritative owner.
8. **Define elegance as clarity.** Prefer explicit state/failure semantics, clean boundaries, and a
   minimal exposed API. Elegance never licenses unrelated refactoring.
9. **Reject over-engineering and under-engineering equally.** Avoid speculative abstraction, but do
   not omit correctness, financial safety, replay, compatibility, recovery, operations, or proof.

## Architecture and domain invariants

The workspace uses small crates and strict dependency direction:

```text
core-types
  -> config, event-log, resolver-card, source-core, venue-core
      -> source-* and venue-*
      -> model-core and model-*
      -> strategy-core, trader-index, copy-signal-engine, kelly-sizer,
         strategy-winner-follow, execution-core, risk-engine
      -> replay, backtest, service, cli
```

Hard rules:

- Source crates cannot depend on venue crates. Venue crates cannot depend on strategy crates.
- Strategies emit `OrderIntent`; only execution routers submit orders.
- `risk-engine` is pure and deterministic with no network or database I/O.
- Production and replay use the same crates. Every live decision must be reproducible from recorded
  inputs, hashes, config, resolver/model artifacts, and adapter versions.
- Every external input retains the raw payload hash, source ID, source/observed timestamps, schema
  version, and parser version required for replay and audit.
- First-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses
  are Rust 2024 on pinned Rust 1.95.0. Research/operational scripts may remain tooling; do not
  introduce Python, Node, or browser automation into the production hot path.
- No production `unwrap`, `expect`, unchecked `panic!`, or `unsafe`. A reviewed per-crate unsafe
  exception must be explicit; workspace `unsafe_code = "forbid"` is the default.
- No raw `f64` for money, prices, probability, quantity, fees, slippage, or balances. Use
  `rust_decimal` or validated integer ticks/cents/bps. Avoid narrowing `as` casts.
- No unbounded hot-path channels. Declare backpressure, retry, and drop behavior.
- Prefer typed state machines, pure risk/ranking/sizing/classification functions, explicit errors,
  deterministic fixtures, property tests, replay tests, and scenario tests.

Domain constraints:

- Wallet, trader, leader, and candidate are distinct. Ranking and concentration accounting are
  per-wallet, as defined in `_GLOSSARY.md` and `19-`.
- Wallet-to-operator clustering and on-chain funder discovery were removed in #326. Do not treat the
  archived machinery as a current semantic owner or live-decision input.
- Public Kalshi trades are anonymous. Do not attribute them to a leaderboard trader without official
  trader-level data or explicitly authorized portfolio access.
- CrowdIntel/UI scraping and opaque third-party cluster scores are research inspiration, not live
  production inputs.
- Resolver evidence is mandatory before trading. Missing, stale, ambiguous, or deferred evidence
  fails closed.
- Kelly cost/price inputs use the canonical net-of-fees, slippage, and adverse-selection semantics;
  do not size from a stale probability merely because the resulting fraction is positive.
- Risk approvals, Kelly ceilings, idempotency shape, promotion/demotion criteria, halt scope, and
  runtime config ownership come from `_GLOSSARY.md` and `19-`; do not reproduce values from memory.

## Working method and mutation boundaries

- Before planning or editing, identify the semantic owner, consumers, nearest healthy precedent,
  persistence/external effects, failure behavior, replay/risk impact, and verification path.
- Trace material changes end to end rather than reasoning from one file or a happy path.
- Preserve existing user work and unrelated changes. Never reset, force, broadly clean, or overwrite
  them to simplify the task.
- Keep production and external-system investigation read-only unless the user explicitly authorizes
  the exact mutation or the selected workflow clearly includes it. A development workflow never
  authorizes live trading.
- Use read-only helper agents selectively for bounded independent discovery or adversarial review
  when an applicable skill requests them and they materially improve coverage or latency. The primary
  reads load-bearing sources, verifies results, owns writes, and reconciles disagreements.
- Ask only when an unresolved choice changes behavior, permissions, persisted/external contracts,
  lifecycle, rollout, verification policy, or consequential risk acceptance. Otherwise use and state
  the repository-native reversible default.
- Run focused verification first, then broader checks required by blast radius and the selected
  workflow. Inspect the final diff and repository state before declaring completion.

## Build, test, and workflow gates

The Rust acceptance gate used locally and in CI is:

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

Run a targeted test first with `cargo nextest run -p <crate> <test_name>`. The full Rust gate is
required for Rust/workspace changes before push; document any command that genuinely cannot run.

Tests must be deterministic and proportional to behavior: pure/property tests for rules, replay tests
for production/replay parity, and scenarios for externally observable wiring, recovery, or risk. Use
fixed clocks/RNG and recorded fixtures; no live network in tests.

Docs/skills/tooling-only changes use their relevant contract/syntax/parity checks and `git diff
--check`; do not run the Cargo gate merely because a workflow advanced. Agent workflow changes must
pass:

```bash
bash scripts/check_skill_parity.sh
python3 scripts/check_agent_workflow_contract.py
git diff --check
```

CI also runs repository Python drift guards and the Postgres RPC parity/concurrency scenario. When a
change touches those surfaces, run the relevant exact checks locally or document CI as the unavailable
environment-specific authority.

## Git, commits, and pull requests

- Use isolated `feat/<short-name>` worktrees from fresh `origin/main` for implementation cycles. Do
  not mix task edits into the primary checkout while that worktree is active.
- Inspect open PRs, remote branches, and worktrees before claiming scope. Coordinate semantic overlap;
  filename overlap alone is not the contract.
- Commit concise imperative summaries, optionally with a scoped prefix. Bodies explain why.
- Never push a failing or stale-gate head. Bind verification and review to the exact commit SHA; a
  code/test edit or base merge invalidates affected evidence.
- PRs include summary, source issue, scope/files, exact tests and results, scenario evidence, gate
  exceptions, review outcomes, replay impact, risk/financial impact, API/source docs checked,
  deployment/config/migration impact, rollback, and shortcuts/hacks.
- Add screenshots only for visual changes.
- Update `docs/15-SOURCES.md` `Last checked` entries when venue/source contracts are re-verified.
- Wait for required CI on the current PR head, resync `main`, rerun affected gates/review, then
  squash-merge with an expected-head guard. Verify the merge's `main` CI before cleanup.
- Clean up only after remote merge state and post-merge CI are confirmed. Preserve the PR, branch,
  and worktree when merge or remote state is ambiguous.

## Security and configuration

Do not commit secrets, API keys, credentials, private datasets, live trading configuration, or copied
production data. Prefer official APIs and primary documentation over scraping or opaque scores.
Validate configuration at the owning boundary, fail closed where required, and make rollout/rollback
and audit behavior explicit for risk-bearing changes.

## Communication

Lead with the conclusion, delivered behavior, decision, or blocker. Be precise and direct. Distinguish
verified fact, inference, judgment, and unknown. Explain enough evidence and reasoning to reproduce
material conclusions without narrating irrelevant scratch work.

In implementation summaries, report tests, review decisions, replay/risk/deployment impact, skipped
checks, temporary workarounds, and unresolved concerns. State `none` when a selected workflow requires
explicit shortcut accounting and there were none.
