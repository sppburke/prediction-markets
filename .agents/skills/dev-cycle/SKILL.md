---
name: dev-cycle
description: >-
  Run a full prediction-markets development iteration through merge to main:
  analyse the established task, validate or form the implementation plan,
  implement in an isolated worktree, run blast-radius and full acceptance
  gates, review the exact diff, open a pull request, wait for CI, sync, squash
  merge, verify main, and clean up. Use when the user asks to dev-cycle, ship,
  or implement a specific change through merge. Do not use when the requested
  endpoint is only a plan, review, diagnosis, or unmerged local edit.
---

# Dev Cycle

Deliver one established task through a verified squash merge to `main`:

`analyse → plan-gate → isolate → implement → verify → review → CI → sync → merge → verify main → cleanup`

## Rigor

Use the evidence-first and engineering decision standards in `AGENTS.md`. Prefer the smallest direct,
minimum-complete, repository-native change. Do not accept speculative abstractions or correctness,
replay, risk, recovery, operations, or verification shortcuts.

Read both required references before creating a worktree or editing:

- [references/repository-gates.md](references/repository-gates.md) — mandatory docs, architecture,
  change-type classification, Rust/replay/scenario gates, and PR evidence.
- [references/git-review-delivery.md](references/git-review-delivery.md) — current-state analysis,
  worktree isolation, exact-HEAD review, CI, sync, merge, post-merge verification, and cleanup.

## Host Runtime Adapter

Use the host runtime's native progress tracker, shell, and read-only helper/reviewer capabilities.
Never require a particular vendor, model name, nested agent CLI, or tool spelling.

- Use read-only helpers for bounded independent discovery or adversarial review when they materially
  improve coverage or latency. Give them raw task artifacts, exact diff/range, repository path, and
  acceptance criteria rather than the primary's conclusion.
- Keep worktree write ownership, Git, GitHub mutation, finding triage, verification, and merge in the
  primary workflow. Do not let concurrent writers share a worktree.
- For difficult work—financial/risk behavior, concurrency/state, migrations, security/permissions,
  external side effects, live execution, or broad shared infrastructure—use one independent native
  read-only reviewer when available. If unavailable or failed, perform a separated direct review and
  disclose the fallback. Never block solely on a vendor-specific optional reviewer.
- The primary verifies and dispositions every helper or reviewer result.

## Communication

Keep updates brief and high-signal. Lead with product behavior, risk, and decisions; use code detail
only when it helps intervention or verification. Surface material questions before coding. A user who
explicitly requested this full cycle has authorized routine worktree, branch, push, PR, merge, and
cleanup actions within the agreed scope, but not unrelated work or a material scope expansion.

The final summary must include the PR URL, behavior delivered, tests/checks, replay/risk/deployment
impact, post-merge `main` CI state, cleanup state, and `Shortcuts / hacks taken: none` or the exact
exceptions. Report every review finding declined and why; addressed findings may be summarized by
count.

## Engineering Contract

Apply the repository's nine-part decision standard throughout:

1. preserve intent and acceptance criteria;
2. keep implementation, contracts, tests, and rollout internally consistent;
3. ship the minimum complete deployable change;
4. extend the correct existing semantic owner;
5. follow current healthy repository precedent when reuse does not fit;
6. optimize the scoped whole system;
7. minimize durable surfaces and states;
8. prefer clear ownership and a minimal exposed API;
9. reject over-engineering and under-engineered shortcuts equally.

Before creating any new crate, module, helper, client, script, config surface, event, dependency, CI
job, or durable abstraction, search the nearest existing owners and record why none fits. A new
surface is justified only by net-new behavior, data shape, external contract, or operational
invariant.

## 1. Analyse Current State

1. Confirm the requested endpoint is a merge to `main` and resolve the task from the conversation or
   named issue. Do not invent roadmap work when scope is already established.
2. Read the mandatory repository sources and task-specific docs from `repository-gates.md`.
3. Fetch `origin/main`; inspect relevant recently merged PRs/commits, open PRs, remote feature
   branches, and worktrees. Detect work that already satisfies or conflicts with the task.
4. Trace the current path end to end and identify the semantic owner, consumers, nearest healthy
   precedent, failure behavior, replay/risk impact, and verification path.
5. Right-size the task. If evidence shows the request is one instance of a broader defect, include
   the minimum class-level fix when it remains within intent. Ask before a material expansion.

If the task is already satisfied or obsolete, present the evidence and stop without creating a
branch or PR.

## 2. Establish the Plan Gate

Build one consolidated, implementation-ready artifact from the issue or conversation:

- product behavior and governing logic;
- concrete repository owners and reuse;
- implementation steps and acceptance criteria;
- failure, replay, financial/risk, migration/config, rollout/rollback, and verification impact;
- explicit non-goals.

Run the current `plan-review` contract against that exact artifact before coding:

- Reuse a prior review only when the artifact is unchanged and refreshed `origin/main`, relevant
  claims/PRs, owners, touchpoints, contracts, and verification inputs do not falsify it.
- Apply verified revisions in one batch. Material behavior, scope, architecture, contract, or
  acceptance-criteria changes require a fresh full review; localized wording corrections require
  only affected-delta confirmation.
- Do not code while verdict is `reject` or `blocked`. Resolve required revisions before proceeding.

Ask concise questions only when ambiguity changes shipped behavior, permission, lifecycle,
persistence, external contract, rollout, verification, or risk acceptance. Otherwise state the
repo-native recommendation. Present the plan before implementation; an explicit full-cycle request
authorizes proceeding once no material user-owned choice remains.

## 3. Isolate and Claim the Work

Create a new `feat/<short-name>` worktree from the exact fetched `origin/main` tip using the commands
in `git-review-delivery.md`. Never edit this task in the primary checkout after the worktree exists.
Record the primary checkout's absolute path, branch, and status before isolation. Preserve it exactly;
if it is dirty, detached, or not on `main`, implementation may continue in the isolated worktree but
the final local-`main` refresh must stop unless those conditions are independently resolved.

Before coding:

- verify the worktree is clean and based on the recorded remote SHA;
- check semantic and file overlap with open PRs, branches, and worktrees;
- record the issue number when one exists;
- classify the planned diff as `rust`, `docs`, `tooling`, or a mixed union.

Coordinate unavoidable overlap with the user or other owner before touching the contested surface.

## 4. Implement the Minimum Complete Change

1. Work only inside the isolated worktree and follow neighboring style.
2. Implement one real phase unless the approved plan has a genuine independently deployable,
   migration, compatibility, cutover, or blast-radius boundary.
3. Keep source, venue, strategy, risk, execution, and replay ownership correct. Treat all financial
   values, defaults, ordering, idempotency, and fail-closed behavior according to
   `repository-gates.md` and canonical docs.
4. Add or update focused deterministic tests alongside each behavior. Prefer pure, property, replay,
   and scenario tests at the narrowest useful boundary.
5. Update canonical docs only when shipped truth changes. Re-verified source/venue contracts require
   the applicable `docs/15-SOURCES.md` date update.
6. For each real phase, run its focused checks, inspect the phase diff, commit a coherent checkpoint,
   and review its integration with prior phases before continuing.

Never commit secrets, live trading configuration, private data, generated credentials, or unrelated
workspace changes.

## 5. Verify the Candidate

1. Build a blast-radius verification plan from changed paths and shared owners. Run focused checks
   first, then the scope-required gate from `repository-gates.md`.
2. Write the scenario pass/fail criterion before running any new end-to-end scenario. Keep fixtures,
   clocks, RNG, network inputs, and disk paths deterministic.
3. Run formatting before the final gate. For a `rust` or mixed-rust change, the complete acceptance
   gate must be the last code-validation action before the commit/push it supports. Any later code or
   test edit invalidates that evidence and requires rerunning the affected checks and full gate.
4. Verify a suspected pre-existing failure on an unmodified disposable `origin/main` worktree. A
   failure introduced or exposed by the branch is a regression, not an exception.
5. Bind every verification record to the exact commit SHA and working-tree state it covered.

Do not publish a commit with failed, stale, partial, or wrong-HEAD required evidence.

## 6. Review and Publish

1. Confirm every intended change is committed with a concise imperative message and the worktree is
   clean. Do not create an empty or duplicate "verified" commit after a phase already committed the
   exact candidate.
2. Review the complete `origin/main...HEAD` diff once using correctness/invariants,
   repository-fit/reuse, and verification/operations lenses. Add financial/risk, replay/data,
   security, source/venue, or concurrency lenses only when touched.
3. Every finding must state the claim/risk, falsifier or repro, cited diff/test/runtime evidence,
   shipped impact, and minimum fix. Triage every finding as `fixed`, `declined`, or `deferred`:
   - fix every verified in-scope defect;
   - decline only when wrong, intended behavior, out of scope, or genuinely over-engineered;
   - effort, size, or churn is never sufficient reason to decline a sound fix.
4. After a localized review-fix commit, review only the exact unreviewed delta plus affected
   integration invariants and rerun only checks whose inputs changed. Restart whole-branch review
   only for material scope, architecture, shared-contract, or base-diff changes.
5. Sync from `origin/main`. If it advanced, merge it, inspect the incoming range, rerun affected
   verification/review, and bind new evidence to the new HEAD.
6. Apply the exact-HEAD push guard in `git-review-delivery.md`, push, and open one PR using the
   repository PR evidence contract. Use byte-safe body transport.

## 7. Wait, Resync, and Merge

1. Wait for every required GitHub check on the current PR head to finish green. Inspect and fix
   failures; do not merge on local evidence alone.
2. Triage every available PR-side review finding with the same rule as local review. A provider's
   absence or quota may be reported as unavailable; it never substitutes for required native/direct
   review and CI.
3. Fetch `origin/main` immediately before merge. If it advanced, merge it and repeat every affected
   gate, review, push, and CI check on the new head.
4. Confirm the PR is open, non-draft, targets `main`, remote head equals the reviewed/tested head, CI
   is green, and no unresolved material finding remains.
5. Squash-merge using the expected-head guard from `git-review-delivery.md`. A nonzero merge command
   is ambiguous until the remote PR state is inspected. Never clean up unless remote state is
   confirmed `MERGED`.

## 8. Verify Main, Clean Up, and Report

1. Identify the post-merge `main` workflow for the merge commit and wait for it to finish green. If
   it fails, preserve evidence and run a scoped hotfix cycle; do not declare completion.
2. After confirmed merge and green `main`, remove the worktree, delete local/remote feature branches,
   and fast-forward the primary checkout's local `main`. Never force a divergent local main.
3. Comment the merged result on the source issue when one exists.
4. Return the required final summary and review outcomes. Explicitly account for every skipped check,
   workaround, pre-existing failure, declined finding, and cleanup result.

## Failure Boundary

When authentication, GitHub state, CI, required evidence, merge permission, or local branch state is
ambiguous, preserve the branch, PR, worktree, and evidence. Report `Checked / Showed / Unknown /
Needed`; do not force, clean up, fabricate success, or broaden scope to work around the blocker.
