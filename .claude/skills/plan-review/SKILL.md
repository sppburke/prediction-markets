---
name: plan-review
description: >-
  Review an implementation plan, spec, design doc, GitHub issue, or handoff for
  correctness, internal consistency, repo-fit, and minimum-viable scope BEFORE
  any code is written. Outputs a verdict (approve / approve-with-revisions /
  reject) plus Blocking and Should-fix findings, each with an exact plan delta
  and a tier-cited evidence trail. Use this skill whenever the user shares a
  not-yet-implemented plan and asks whether it is ready, comprehensive,
  exhaustive, internally consistent, minimum-viable, or whether it "follows our
  guidelines" — and whenever they say things like "review this plan", "check
  this spec before I build it", "is this design good enough to implement", "do
  you agree with this approach", or are about to hand a plan off to
  implementation / dev-cycle. Trigger even when the user never says the words
  "plan review": any request to vet a proposed-but-unbuilt design against the
  codebase is this skill. Do NOT use it to review a finished code diff or PR
  (that is the diff/PR-review skill's job) or to author the plan from scratch.
---

# plan-review

Review a plan, spec, or handoff against the repo it will be built in. Output a verdict and findings; never implement, never rewrite the plan into a new plan.

## Required Inputs (and what to do if missing)

| Missing | Action |
|---------|--------|
| Plan/spec/handoff body | Block — nothing to review |
| Intent / desired behavior | Infer from artifact; block only if unrecoverable |
| Acceptance criteria | Infer behavioral AC, mark inferred, Should-fix to make explicit |
| Non-goals, constraints | Should-fix; do not block |
| Touchpoints | Discover via repo scan; do not ask |
| Code diff | Optional; if present → drift check (see Partial) |

Ask the user only when ambiguity changes shipped behavior, actor/permission, lifecycle, persistence, rollout, or verification. Otherwise pick the repo-default v1 and mark the assumption.

## User Guidelines (verbatim)

Each label is the user's exact wording; gloss is operational.

1. **accurate to the intent of the change** — plan solves stated problem; AC traces to intent.
2. **internally consistent** — steps, contracts, AC, non-goals, edges, verification align.
3. **the minimum viable code change required for deployment** — smallest deployable diff for AC; optional scope → non-goal.
4. **re-uses as much as possible of the current code base** — extend existing flows, helpers, models, clients, config, tests.
5. **leverages repo precedence when re-use is not possible** — match stack, layers, naming, env/config, migration style, test style, deploy model.
6. **is as close to a global optimum as possible while being the minimum viable code change** — right complexity for *this* repo; not speculative, not underfit.
7. **simplicity is important and essential** — fewer surfaces, fewer states, one coherent path.
8. **code elegance is the highest form of beauty** — clean seams, minimal exposed API, repo-native shape; no clever novelty.

**Conflict ladder** (earlier wins unless later is Blocking for correctness, data loss, security, contract break, migration safety, rollout, operability):

```
1 intent  >  2 consistency  >  3 MVCC  >  4 reuse  >  5 precedent  >  6 optimum  >  7 simplicity  >  8 elegance
```

Scope expansion past MVCC requires cited Blocking evidence; record the trade.

## Method

Tier annotations follow the **evidence-first standard** (the global `CLAUDE.md` "Evidence-First Investigation" — Primary Sources tiers: T1 repo/DB ground truth, T2 runtime evidence, T3 specs/docs read this session, T4 indirect/fallible).

1. **Normalize [T0]** — extract intent, AC, non-goals, constraints, touchpoints, verification; tag each as explicit / inferred / missing / contradicted.
2. **Claim map [T0]** — convert plan statements into testable assertions (behavior, contracts, data, tier boundaries, schema/migrations, cache/read-model invalidation, permissions, rollout, observability, tests). A claim is **non-trivial** if it affects shipped behavior, a contract, persistence, ops, or the verification path. Build it as an explicit **ledger**, one row per claim. You **must** enumerate a row for each of: every named output / field / return value, every new or modified fn / method / constructor / helper, every table / view / column touched, every CLI / subcommand / flag surface, every config / env key, and every `file:line` the plan cites. Each row carries its own falsifier (step 3). The sweep is complete only when every row is resolved (`checked` or `Blocked`) — **a verdict-determining finding does not license leaving rows unresolved.** Carry the enumerated/resolved counts into the Output `## Coverage` line.
3. **Falsify [T1 → T2]** — per non-trivial claim: pre-commit a falsifier; check Tier 1 (code, configs, migrations/schema, tests, lockfiles, docs read this session); descend to Tier 2 (targeted command, test, log, API call, scripted data-store query) only when T1 is silent; else **Blocked** with the exact next check.
   - **3b. Citation sweep [T1]** — resolve every `file:line` / symbol the plan cites against the current tree in one pass. A citation that does not resolve to the claimed symbol is a Should-fix (drift) at minimum; batch them — do not surface one stale citation per review round.
4. **Trace [T1]** — entry (route/job/CLI/UI/handler) → service/state → persistence/cache/IO → consumer; enumerate callers when contracts, schemas, labels, env keys, or shared helpers change.
5. **Score** — apply Guidelines + Plan Audit matrix + diff-review lenses to the *predicted implementation*.
6. **Drift** — if code exists, classify each plan step `done | partial | missing | extra | changed-shape` (see Partial).

## Plan Audit (merged matrix)

For each area: what the plan must contain, the smell that signals failure, and the evidence that resolves it.

| Area | Required | Smell (Blocking unless justified) | Evidence |
|------|----------|------------------------------------|----------|
| Intent | Problem, desired behavior, actor, trigger | Restated as features, not behaviors | Read intent vs AC |
| AC | Behavioral, testable, condition → result | Names features; vague verbs | Map each AC to a test or smoke step |
| Non-goals | Explicit deferrals; ≥1 if optional scope present | Empty when scope obviously trims | Diff plan scope vs intent |
| Touchpoints | Concrete files/dirs/routes/modules/schemas/tests/docs | "TBD in code", "figure out during impl" | grep proposed names; read neighbors |
| Path | Ordered steps; cross-tier symmetry (UI/API/service/worker as applicable); layer ownership clear | Out-of-order; wrong layer owns the rule | Trace entry → persistence |
| Reuse | Extend an existing surface | New helper/component/client/module without a grep first | grep for an existing wrapper before accepting a new one |
| Precedent | Match stack, naming, and env/config via the repo's established config mechanism | Ad-hoc env reads outside the config layer; an alternate runtime/stack | Read the repo's config/settings module or equivalent |
| Alternatives | ≥1 rejected option with reason; chosen shape near-optimal *here* | Single path, no rationale | Plan must state the rejected option |
| Data | Schema, lifecycle, defaults/backfill, cache/read-model invalidation/recompute | "May need a migration"; cache untouched on writes | Models/schema + current migration head + cache/read consumers |
| Contracts | API/types/events; backward compat; consumers listed | Rename without consumer audit | grep old symbol/label/env across the repo |
| Edges | Empty/null, duplicates, stale, retry, ordering, timezone, permission, partial failure, lifecycle (archive/dismiss/delete/merge) | Edges absent or implied | Read existing edge handling for a similar feature |
| Integrations | Worker/webhook idempotency; audit/admin visibility when relevant | "Worker handles it" without retry/ordering | Read the handler + its retry/dedupe path |
| Ops | Deploy order, rollback, flags, observability when non-trivial | Stateful change without rollback | Read deploy notes + similar prior changes |
| Runtime/Platform | Matches the repo's deployment model (e.g. stateless + env-driven, if that is the model); no durable local-disk assumption unless the platform guarantees it | Durable state written to ephemeral storage; assumptions the deploy target doesn't guarantee | Confirm against the repo's runtime/deploy primitives |
| Verification | Specific tests + the repo's verification gate (build/lint/test commands) + a manual smoke path | "Add tests later"; indirect/UI-only check for authoritative state | Read existing test files; confirm the commands are runnable |
| Risks | Unknowns, accepted shortcuts, safeguards | Hedged factual claims ("probably reuses") | Tier 1 falsifier missing |
| Granularity | Steps describe outcome + key file ownership | Over-specified line-level edits OR vague prose | Compare to the repo's prior plans |

## Severity

| Tag | Meaning | Example |
|-----|---------|---------|
| **Blocking** | Correctness, data loss, security, contract break, missing migration/invalidation, unsupported state claim, unbounded scope, unverifiable AC, unsafe rollout, duplicates existing infra | Schema rename without consumer audit |
| **Should fix** | Clarity, missing non-goals, weak verification, ambiguous ownership, repo-precedent drift | Non-goals empty with optional scope present |
| **Consider** | Tradeoff, alternate shape, optional simplification | Alternative cache strategy worth noting |
| **Blocked** | Review evidence missing | Tier 1 silent on caller list; need grep of the data-access module |

Any **Blocking** ⇒ verdict cannot be approve. Any unverified material claim ⇒ **Blocked**, not speculation.

## Partial Implementation

If plan + code both exist:

1. Review plan quality first.
2. Classify each plan step: `done | partial | missing | extra | changed-shape`.
3. `changed-shape` without a plan update → Blocking unless harmless and documented.
4. `extra` must satisfy MVCC or move to non-goals.
5. Code-correctness findings → defer to the diff/PR-review skill; here flag plan/code contract drift only.

## Multi-PR Plans

If the plan spans multiple PRs/phases:

- Each phase gets its own AC, non-goals, verification, rollback.
- Inter-phase contracts (schema versions, flag states, dual-read windows) must be explicit.
- Phase 1 must be deployable without phase 2.

## Worked Example (illustrative; stack-agnostic)

```text
Plan claim: "Add a `dismissed_at` field to the Item entity; the active-list view hides dismissed items."

Assertions:
  A1 Item lacks dismissed_at today
  A2 schema change adds a nullable field, default null
  A3 active-list query filters dismissed_at IS NULL
  A4 dismiss action sets dismissed_at = now()

Falsifiers:
  A1 — grep the entity/model definition + the current migration/schema head
  A3 — locate the active-list query and any read-model/cache in front of it
  A4 — locate the dismiss handler, or confirm its absence

Finding (Blocking):
  A3 names a filter but omits cache / read-model invalidation. Active items are
  served through a cached list (`<cache module>:<line>`). Plan delta: add a step
  "invalidate the active-list cache on dismiss" to §Implementation, or document
  why serving stale-dismissed rows is acceptable.
```

## Output

```text
## Coverage
claims enumerated: N | falsified: N | blocked: M | citations checked: C
# N must equal (falsified + blocked). If not, the sweep is incomplete — do not emit a verdict.

## Verdict
approve | approve with revisions | reject — one sentence.

## Handoff-Ready
yes | yes-after-revisions | no — if not yes, list the minimum missing items.

## Blocking
- [finding] — [plan §X | file:line | grep | command output] → [exact plan delta].

## Should Fix
- [finding] — [evidence] → [delta].

## Consider
- [tradeoff] — [evidence] → [delta if any].

## Guideline Fails
- [n] [name]: [one-line reason]   # only list fails; passes implied

## Global Optima
One paragraph: where the plan sits on the complexity curve; what moves it toward the optimum *for this repo*.

## Blocked
- Checked: [...]   Showed: [...]   Unknown: [...]   Needed: [exact command/query/file].
```

Omit empty severity sections. Inline plan deltas in each finding — no separate deltas block.

## Self-Check

Before emitting:

1. Every factual claim has a Tier 1/2 citation or is Blocked. The claim-map ledger is fully resolved — every enumerated row is `checked` or `Blocked` — **regardless of whether the verdict is already determined.** The verdict decides approve/reject; it does not signal that enumeration is complete. The `## Coverage` tally must balance (`enumerated == falsified + blocked`).
2. Every Blocking has an exact plan delta.
3. MVCC honored — no speculative additions demanded.
4. Reuse falsifier run before accepting any new helper/component/client/module.
5. Every delta you propose is itself a plan claim. Run the reuse + precedent falsifier on each delta (grep for the shape/helper/signature it names) **before writing it.** A recommendation that introduces a new helper/signature/surface without that grep is the same defect you'd flag in the plan — and becomes the next round's finding.

## Do Not

Implement fixes. Rewrite the plan into a new plan. Approve while a Blocking exists. Demand speculative architecture. Use UI/indirect signals for authoritative state. Duplicate diff-review lens prose. Accept "TBD". Ask repo questions before grepping. Propose a delta you have not falsified against the repo — a recommendation that names a new symbol/signature/shape without a precedent grep can introduce the next round's finding. Stop the claim-map sweep early because the verdict is already decided.
