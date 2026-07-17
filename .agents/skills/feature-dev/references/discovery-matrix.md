# Prediction-Markets Feature Discovery Matrix

Use this checklist only when planning depth, evidence routing, or cross-cutting impact is unclear.
The parent `SKILL.md` owns the workflow.

## Route each claim to its authority

| Claim | Highest applicable source |
| --- | --- |
| Process, scope, safety, doc authority | Root `AGENTS.md`, applicable nested instructions, selected skills |
| Current implementation or contract | Code, tests, `Cargo.toml`, config, SQL, fixtures, lockfile |
| Domain terms and numeric defaults | `docs/_GLOSSARY.md` and canonical TOML in `docs/19-WINNER-FOLLOW-STRATEGY.md` |
| Architecture and dependency ownership | `docs/16-RUST-WORKSPACE-ARCHITECTURE.md` plus current crate graph |
| Current runtime or stored state | Target-scoped command, logs, API, or scripted read-only query |
| Rationale and precedent | Relevant recent issues, PRs, commits, and decision records |
| Pinned dependency behavior | Exact-version source or official documentation |
| Current venue/source contract | Current official documentation under the TTL policy in `docs/15-SOURCES.md` |

For each material premise record the source, observation, what it establishes, and a falsifier. When
blocked, use `Checked / Showed / Unknown / Needed`. Evidence for the wrong revision, environment,
venue, wallet class, or time window does not resolve the claim.

## Scale by uncertainty

One focused pass is sufficient only when ownership, precedent, integration points, failure shape,
and verification are clear. Otherwise use separate lenses tied to the actual uncertainty:

- data/schema, replay compatibility, migration, or backfill;
- money, probabilities, rounding, Kelly sizing, risk caps, or runtime approvals;
- concurrency, ordering, retries, idempotency, or backpressure;
- source/venue identity, freshness, rate limits, or external side effects;
- crate boundaries, shared contracts, generated artifacts, or multiple consumers;
- live execution, rollout, rollback, operational controls, or observability.

More risk means more evidence, safeguards, and verification. It does not automatically mean more
delivery phases.

## Prediction-markets impact sweep

Check only applicable rows, but make every applicable disposition explicit.

| Surface | Questions to resolve |
| --- | --- |
| Vocabulary and identity | Are wallet, trader, leader, and candidate used correctly? Are ranking and concentration caps per-wallet, and is identity evidence authoritative for this venue and wallet class? |
| Dependency ownership | Does source stay independent of venue, venue of strategy, and strategy of execution? Is the rule owned by the narrowest correct crate? |
| Events and replay | What raw input, schema/parser version, timestamps, hashes, decision inputs, and version identifiers must be recorded? Can production and replay use the same path deterministically? |
| Financial semantics | Are money, price, probability, quantity, fees, slippage, and rounding represented without raw financial `f64` or narrowing casts? |
| Risk and defaults | Does the change alter eligibility, sizing, caps, promotion, halt scope, or runtime approval? Is every numeric default added only to `_GLOSSARY.md` or canonical `19-` TOML? |
| Source/venue contracts | Are official endpoints, anonymity/authorization limits, rate limits, pagination, ordering, and stale/missing evidence handled explicitly? Does `docs/15-SOURCES.md` need a `Last checked` update? |
| Ordering and idempotency | What happens on duplicates, stale/out-of-order events, retries, partial failure, reconnect, or bounded-channel saturation? |
| Persistence and lifecycle | Are schema/default/backfill, cache/read-model convergence, archive/delete/settlement, and rollback/recovery explicit? |
| Verification | Which pure, property, replay, scenario, and operational checks prove each acceptance criterion? Are clocks, RNG, fixtures, and network inputs deterministic? |
| Deployment and operations | What config, secret, migration, service restart, health signal, rollback, and post-deploy evidence is required? |

## User-owned choices

Ask only when evidence cannot settle a choice that changes behavior, actors/permissions, persisted or
external shape, lifecycle, rollout, verification policy, or accepted risk. Do not use a default to
hide one of those decisions.

## Stop conditions

Stop exploration when every material premise has applicable source/falsifier coverage and no
unresolved contradiction remains. Stop clarification when intent, after-state, ownership, lifecycle,
permissions, failure semantics, acceptance criteria, non-goals, and verification are evidenced or
decided. If evidence proves no change is needed, stop without filing work.
