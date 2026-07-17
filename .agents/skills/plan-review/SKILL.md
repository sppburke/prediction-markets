---
name: plan-review
description: >-
  Review a proposed, not-yet-implemented prediction-markets plan, feature spec,
  design document, GitHub issue, or handoff for correctness, internal
  consistency, repository fit, executability, and minimum-complete deployable
  scope before code is written. Use when asked whether an unbuilt plan is
  ready, comprehensive, exhaustive, minimum viable, aligned with repository
  guidelines, or ready for implementation or dev-cycle. Return an
  evidence-backed approve, approve-with-revisions, reject, or blocked verdict
  with exact plan deltas. Do not author a plan from scratch or review completed
  code, a standalone diff, branch, or pull request. Partial code may accompany
  an otherwise unbuilt plan only as evidence of plan drift, not for code review.
---

# Plan Review

Review a proposed plan against the prediction-markets repository it will change. Return a verdict
and exact revisions. Never implement, mutate the submitted artifact, wholesale-author a replacement,
or perform completed-code review.

## Rigor

Use the evidence-first standard in `AGENTS.md` and `docs/_EVIDENCE-FIRST.md`. Inspect the proposed
behavior and repository path end to end. Do not stop at the first verdict-determining defect, leave a
material claim unresolved, or substitute confidence for proof.

Run every accessible non-destructive check required by a material claim. Keep production and live
trading checks read-only. Runtime, database, log, or API checks are required only when the claim
depends on current runtime, deployment, or stored state.

## Host Runtime and Execution Mode

Use the host runtime's native progress and read-only review capabilities. Never require a particular
vendor, model name, or tool spelling.

Record one truthful execution mode:

- `contained-direct`: contained review performed directly;
- `caller-launched-independent`: another workflow launched this review; never launch another
  reviewer;
- `direct-with-counter-reviewer`: a direct complex/risky review used at most one fresh read-only
  counter-reviewer;
- `direct-fallback`: independent review was useful but unavailable or failed; review directly and
  state why.

A caller-launched independent review never launches another reviewer.

A counter-reviewer receives the artifact, intent, repository target, and raw evidence without the
primary's desired conclusion. The primary verifies and dispositions its output and remains
responsible for complete inventories, evidence, deltas, and verdict. Never recurse.

## Evidence Authority

Read the nearest applicable repository instructions first. For prediction-markets, the core authority
includes `AGENTS.md`, `docs/_EVIDENCE-FIRST.md`, `docs/_BASELINE.md`, `docs/_GLOSSARY.md`,
`docs/19-WINNER-FOLLOW-STRATEGY.md`, `docs/16-RUST-WORKSPACE-ARCHITECTURE.md`, and the relevant
phase/venue/source document. The repository's documented authority order wins.

The submitted plan is the **Artifact**, not an evidence tier. Route each claim to the highest
applicable source:

- **T1 — repository/system ground truth:** current code, config, schema/SQL, lockfiles, tests,
  reproducible derivations, and scripted read-only queries.
- **T2 — runtime evidence captured now:** commands, logs, live APIs, deployments, and other
  time-scoped observations.
- **T3 — specifications read now:** current repository docs and exact-version official primary
  documentation.
- **T4 — indirect or fallible context:** summaries, secondary sources, prior-session recollection,
  and intuition; never let it impersonate higher authority.

When accessible authority cannot resolve a material claim, record an **Evidence Gap** with exact
`Checked / Showed / Unknown / Needed` fields after exhausting applicable sources. Missing evidence
is not proof that a plan is defective.

## Required Inputs

| Missing input | Action |
| --- | --- |
| Plan/spec/handoff body | Return `blocked`; there is no artifact to review. |
| Intent or desired behavior | Infer from the artifact; ask only if materially unrecoverable. |
| Acceptance criteria | Infer behavioral criteria, label them inferred, and use Should Fix to make them explicit. |
| Non-goals or constraints | Use Should Fix only when absence leaves scope ambiguous. |
| Touchpoints | Discover them through repository inspection; do not ask the user to search. |
| Code diff | Optional only alongside the plan. Apply "Handle Existing Code and Multiple Phases" and never assess code correctness. |

Ask only when ambiguity changes shipped behavior, actor/permission, lifecycle, persistence, rollout,
verification policy, or consequential risk acceptance. Otherwise use the repository-default minimum
complete shape and state the assumption.

## Owner Guidelines

1. **accurate to the intent of the change** — solve the stated problem; criteria trace to intent.
2. **internally consistent** — steps, contracts, criteria, non-goals, edges, verification, and rollout agree.
3. **the minimum viable code change required for deployment** — plan the smallest complete deployable change; exclude optional scope.
4. **re-uses as much as possible of the current code base** — extend correct existing flows, crates, traits, types, config, scripts, and tests.
5. **leverages repo precedence when re-use is not possible** — match established layering, naming, event, config, test, and deployment patterns.
6. **is as close to a global optimum as possible while being the minimum viable code change** — choose the right complexity for this repository.
7. **simplicity is important and essential** — minimize durable surfaces, states, and control paths.
8. **code elegance is the highest form of beauty** — use clear ownership, clean seams, and a minimal exposed API.
9. **over-engineering and shortcuts are equally bad and undesirable** — reject speculative machinery and underfit work that omits correctness, replay, risk, recovery, operations, or proof.

Minimum viable means **minimum complete and deployable**, not the smallest diff at any cost.
Correctness, security, financial integrity, contracts, replayability, migration safety, rollout,
operability, recovery, and verifiability are constraints. Within them, prefer intent fidelity, reuse,
precedent, simplicity, and elegance.

## Review Method

### 1. Normalize the Artifact

Extract intent, actor, trigger, after-state, behavioral acceptance criteria, non-goals, constraints,
touchpoints, failure behavior, rollout, recovery, and verification. Mark each `explicit`, `inferred`,
`missing`, or `contradicted`. Do not treat artifact assertions as proof.

### 2. Build Two Complete Inventories

Keep material reasoning separate from mechanical coverage.

**Material-claim ledger:** enumerate every assertion affecting shipped behavior, financial/risk
semantics, replay/event contracts, persistence, permissions, lifecycle, external APIs, operations,
recovery, or verification. Before checking each row, record evidence that would falsify it. Resolve
every row to exactly one outcome:

- `supported`: applicable authority supports it and its falsifier was checked;
- `contradicted`: applicable authority disproves it;
- `evidence-gap`: accessible applicable authority was exhausted without resolving it.

**Named-surface/citation inventory:** in one batched sweep, check every named crate, module, path,
symbol, output, field, return value, function/method/helper, SQL table/view/column, event/type, CLI
surface, config/environment key, numeric default, and `file:line` citation. Promote consequential
mismatches into the material ledger.

Complete both inventories before any verdict. Coverage must balance: material claims enumerated
equals `supported + contradicted + evidence-gap`, and every named surface/citation is accounted for.

### 3. Falsify and Trace End to End

For every material claim, check the pre-committed falsifier against the highest applicable authority.
Trace source/input → parsing/event log → state/model/strategy → risk/execution → persistence/output and
replay, as applicable. Enumerate consumers when schemas, types, events, config, labels, shared helpers,
generated artifacts, or public APIs change.

Use runtime/data checks only when repository evidence is insufficient or the claim is inherently
current-state dependent. Never convert unavailable evidence into a defect.

### 4. Search Reuse and the Strongest Simpler Counterfactual

Independently search for the nearest correct semantic owner and healthy precedent. A proposed new
crate, helper, client, module, script, event, config surface, or dependency must be justified by
net-new behavior, data shape, external contract, or operational invariant.

For every material non-obvious design choice, compare the strongest lower-complexity
repository-native alternative on intent, completeness, replay/risk correctness, scope, and operational
risk. Do not demand alternatives ceremony for routine work with one established shape.

Consult current primary specifications only when correctness materially depends on an external
venue/source contract, exact dependency behavior, security, concurrency, or absent repository
precedent.

### 5. Audit the Plan

The failure signal says what to investigate; severity comes only from the Severity section.

| Area | Required | Failure signal | Resolving evidence |
| --- | --- | --- | --- |
| Intent and criteria | Actor, trigger, observable condition → result, traceability | Feature nouns or vague verbs replace behavior | Intent mapped to focused proof |
| Scope and executability | Concrete ordered steps, repository-relative owners, non-goals, recovery | `TBD` or material rediscovery/design deferred to coding | Walk first edit through verification/rollback |
| Reuse and precedent | Correct semantic owner extended; new surfaces justified | Parallel crate/helper/client/script/config without need | Repository-wide reuse and precedent search |
| Architecture | Source/venue/strategy/execution direction and pure risk boundaries | Wrong layer owns the rule or execution leaks into strategy | Current crate graph and architecture doc |
| Events and replay | Raw evidence/versioning, deterministic decisions, production/replay parity | Live-only path or unversioned derived state | Event types, replay consumers, scenario tests |
| Financial and risk semantics | Typed decimals/ticks, rounding, Kelly/risk/default ownership, approval/halt scope | Raw financial `f64`, duplicated threshold, or bypassed risk owner | `_GLOSSARY`, canonical `19-` TOML, core/risk types |
| Data lifecycle | Schema/default/backfill, idempotency, invalidation/recompute, settlement/archive/delete | Stateful write without authoritative read convergence or recovery | Schema/SQL, readers, caches, event/replay path |
| Contracts and sources | Consumers, compatibility, venue identity/authorization, freshness/rate limits | Anonymous/unauthorized attribution or stale/assumed API behavior | Call-site sweep and current official source docs |
| Failure and concurrency | Null/empty/stale, duplicate, retry, ordering, partial failure, backpressure | Missing evidence becomes valid state or work is unbounded | Nearest analogous handling and deterministic tests |
| Operations | Config, deploy order, observability, rollback/recovery proportionate to risk | Live/stateful change without safe rollback or health proof | Service/config/deploy surfaces and current state |
| Verification | Criterion-specific pure/property/replay/scenario checks plus applicable gate | Generic "add tests" or indirect-only proof | Existing tests, runnable commands, asserted outcomes |
| Delivery phases | Each phase independently reviewable/deployable with explicit contracts | Artificial phases or phase 1 depends on unshipped phase 2 | Ordered rollout and inter-phase contract walk |

### 6. Handle Existing Code and Multiple Phases

If partial code exists, review plan quality first, then classify each plan step
`done | partial | missing | extra | changed-shape`. Material changed-shape drift is contradicted unless
the consolidated artifact adopts it. Defer code correctness to the PR/diff-review workflow.

For multi-PR/multi-phase plans, require phase-level criteria, verification, and rollback/recovery;
explicit schema/flag/event compatibility between phases; and a first phase deployable without later
phases. Do not invent phases for an atomic change.

## Severity and Exact Deltas

| Tag | Meaning |
| --- | --- |
| **Blocking** | Proven material defect affecting intent, correctness, security, financial integrity, replay/contracts, migration/invalidation, scope, rollout/recovery, operations, or authoritative verification. |
| **Should Fix** | Non-material revision needed for clarity, explicit scope, ownership, evidence, or handoff quality. |
| **Consider** | Supported optional trade-off or simplification that does not gate implementation. |

An Evidence Gap is not a defect severity. Every contradicted material claim must appear in a Blocking
finding.

Tag every finding with guideline IDs, for example `[G3,G4,G9]`. Every Blocking and Should Fix must
include cited evidence and one structured plan delta:

- operation: `add | replace | delete | move`;
- target: named artifact section or claim;
- supersedes: exact prior claim/text, or `none` for a true addition;
- replacement: minimum wording or behavior the consolidated artifact must contain.

Check each proposed delta against repository reuse, precedent, and minimum-complete scope before
emitting it. Remain non-mutating. Append-only amendments cannot earn approval while contradictory or
superseded text remains live; require one consolidated artifact and a complete rereview.

## Verdict Precedence

Apply this order only after both inventories are complete:

1. Any `contradicted` material claim → `reject`.
2. Otherwise, any exhausted material `evidence-gap` → `blocked`.
3. Otherwise, any non-material required revision → `approve with revisions`.
4. Otherwise → `approve`.

`blocked` means material correctness cannot yet be determined; it does not mean the plan is bad.

## Output Contract

```text
## Coverage
product / logic: planned behavior → intended user/operational impact; governing rule.
material claims enumerated: N | supported: S | contradicted: C | evidence gaps: G | named surfaces/citations checked: X/Y
execution mode: contained-direct | caller-launched-independent | direct-with-counter-reviewer | direct-fallback

## Verdict
approve | approve with revisions | reject | blocked — one sentence applying verdict precedence.

## Blocking
- [G#,...] finding — evidence → operation / target / supersedes / replacement.

## Should Fix
- [G#,...] finding — evidence → operation / target / supersedes / replacement.

## Consider
- [G#,...] trade-off — evidence → optional delta if useful.

## Global Optimum
- Reuse / precedent: nearest semantic owner and repository pattern checked.
- Simpler counterfactual: strongest lower-complexity complete alternative.
- Complexity: under-engineered | minimum-complete | over-engineered — evidence.
- Cleanliness: material ownership, seam, duplication, or consistency issue.

## Evidence Gaps
- Checked: ...
  Showed: ...
  Unknown: ...
  Needed: ...
```

Omit empty severity sections. Write `None` under Evidence Gaps when none exist. Keep evidence next to
the finding it determines.

## Final Gate

Before emitting, verify:

1. every material ledger row has a falsifier and exactly one outcome, with balanced counts;
2. every named surface/citation is resolved and consequential mismatches are in the ledger;
3. every conclusion has applicable evidence or an exact Evidence Gap;
4. reuse and the strongest simpler counterfactual were checked independently;
5. every required finding has guideline tags and a structured, evidence-backed delta;
6. every delta itself passed reuse, precedent, and minimum-complete checks;
7. verdict precedence is exact and no early finding truncated the sweep;
8. execution mode is truthful and no review recursion occurred;
9. the output reviews only the plan and does not implement, mutate, rewrite, or perform completed-code review.
