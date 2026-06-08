---
name: dev-cycle
description: Full development iteration for prediction-markets — analyse, plan, implement in an isolated worktree, run the full acceptance gate locally, open a PR, run code-review, fix findings, wait for CI green, sync, then squash-merge to main and clean up. Use for "next phase", "ship feature X", "implement Y", or any work that ends in a merge to main.
---

# Dev Cycle

Full iteration: analyse → plan → isolate → implement → verify → **scenario-verify** → review → ship → cleanup. Target branch is `main`.

## Communication

Terse, high information density. Final summary lists every shortcut, hack, or skipped check. Default is `none`.

## Model selection

Coding (analysis, planning, implementation, verification, fix-up of review findings, merging, cleanup) runs on the **main session model** — keep this on Sonnet.

PR review runs on **Opus** automatically via `Agent` dispatch in step 6. The main session does not switch models; the review subagent runs Opus regardless of the active model. No `/model` ceremony required.

If the main session is on Opus, all coding will also run on Opus — that's tolerable but wasteful. State the current model in your first status update so the user can `/model sonnet` if desired; do not block on it.

## Subagent policy

Delegate **research and discovery** to subagents — codebase exploration, prior-PR/diff analysis, doc lookup, "where is X / how does Y work". Use the `Explore` / `general-purpose` / `Plan` agents so the main session's context stays clean.

Never delegate **coding** to a subagent. Implementation, gate fix-ups, and review fix-up run on the main session (see Model selection). The only non-research subagent dispatch is the step-6 Opus PR review, which reviews — it does not write code.

## Mandatory reading before touching code

Per `CLAUDE.md`:

1. `docs/_BASELINE.md` — toolchain pin, workspace lints, acceptance gate.
2. `docs/_GLOSSARY.md` — vocabulary, type aliases, default config values, idempotency-key shape.
3. `docs/19-WINNER-FOLLOW-STRATEGY.md` — risk caps, Kelly fractions, mode promotion ladders.
4. `AGENTS.md` and `docs/SKILLS.md`.
5. The phase/venue/source doc relevant to the task.

**Doc authority on conflict:** `_BASELINE` > `_GLOSSARY` > `19-` > phase/venue (`00–17`, `20`, `21`) > bootstrap (`18`). Fix the lower-priority doc; never duplicate a numeric default.

## Task scope classification

Before any work, classify the task by what files the diff will touch. Tag with one or more:

- **rust** — diff touches `*.rs`, `Cargo.toml`, or `Cargo.lock` (beyond lockfile-only updates).
- **docs** — diff touches only `docs/`, `*.md`, `AGENTS.md`, `CLAUDE.md`.
- **tooling** — diff touches only `.github/`, `deny.toml`, `rust-toolchain.toml`, `.claude/`, `scripts/`.

The tag controls:

- **Step 4 skill invocations** — rust skills fire only on `rust` scope.
- **Step 5 gate subset** — `docs`/`tooling` scopes skip most cargo commands; CI is authoritative.
- **Step 5 unsafe scan** — runs only on `rust` scope and only if grep finds matches.
- **Step 5b scenario verification** — see existing skip rule there.

Mixed-scope diffs take the union of requirements. State the tag explicitly in the PR body.

## Parallel-work model

Multiple feature branches may be in flight. First to merge wins; everyone else syncs to `origin/main`. Prefer tasks that touch different crates than other in-flight work.

`clash status` reports file overlap with other active worktrees. If overlap exists, coordinate with the user or pick a different task.

Sync points: before creating worktree, before verify, before code-review, before merge. Each sync = `git fetch origin main && git merge origin/main` inside the worktree.

## 1. Analyse

State the active session model in your first status update. If Opus, note it; do not block.

```bash
git fetch origin main
gh pr list --base main --state merged --limit 5 --json number,title,mergedAt,files
gh pr list --base main --state open --json number,title,headRefName
git worktree list
git branch -a | grep feat/ || true
```

For each recently merged PR that touches relevant areas or introduces new patterns, fetch the full body and diff:

```bash
gh pr view <number>
gh pr diff <number>
```

Identify new conventions, shared utilities, and crates that landed. Carry these forward. If a merged PR contradicts a roadmap item or makes a task obsolete, note it in step 2.

`docs/17-RUST-IMPLEMENTATION-ROADMAP.md` defines phase order. **Phase N** (build sequence) ≠ **Strategy N** (strategy identity); see `_GLOSSARY.md` "Phase vs Strategy index".

## 2. Decide

```bash
clash status
```

`clash status` shows which files other active worktrees are touching. If overlap with a candidate task exists, coordinate or pick a different task before proceeding.

Pick a task that is:

- Unblocked by roadmap order.
- Not already claimed by an open PR or local/remote `feat/*` branch.
- Touches different crates than other in-flight work, or coordinates with the user if overlap is unavoidable.

Record the GitHub issue this task closes (`gh issue list --state open`); carry `#<issue>` forward into the PR body (step 6) and the completion comment (step 12). If no issue exists, note that and skip the `Closes` line.

Respect dependency direction in `docs/16-RUST-WORKSPACE-ARCHITECTURE.md`: `core-types → event-log/source-core/venue-core → source-*/venue-*/operator-graph → model-* → strategy-* → execution/risk → replay/backtest/service/cli`. Source crates cannot depend on venue. Venue cannot depend on strategy. Strategies emit `OrderIntent`; only `execution-core` submits.

## 3. Plan and isolate

Present plan, open questions, and design choices to the user — then **answer the open questions yourself with your recommended choice and proceed**. Do not pause for approval. The user has standing authorization to use your recommendations; they will interrupt if they disagree.

Format the plan section so the user can scan it and intervene if needed:

- **Plan:** numbered steps.
- **Open questions:** each question followed by `→ Recommendation: <choice>` and a one-line rationale.
- **Proceeding** with the recommendations above unless interrupted.

Then immediately move on to the worktree and implementation. Do not wait.

Create an isolated worktree from a fresh `origin/main`. Worktrees prevent working-tree contamination from other branches and let parallel cycles run side by side.

```bash
git fetch origin main
git worktree add ../pm-<short-name> -b feat/<short-name> origin/main
cd ../pm-<short-name>
```

`<short-name>` is the crate or feature (`event-log`, `source-onchain-polygon`, …). The worktree is the single source of truth for this branch — never edit files in another checkout while a worktree is active for the same branch.

## 4. Implement

For `rust` scope, invoke before writing code:
- `rust-skills:coding-guidelines` — naming, formatting, comment, and clippy conventions.
- `rust-skills:domain-fintech` — **only** when touching `crates/{strategy-*, kelly-sizer, risk-engine, execution-core, model-*}/` or any path handling money, decimals, or order pricing. Skip for plumbing crates (`event-log`, `source-*` low-level adapters, `venue-core` transport-only).

If the change adds new error types, new `Result`-returning public APIs, or alters error propagation, also invoke:
- `rust-skills:m06-error-handling` — `Result` vs `panic`, `thiserror` vs `anyhow` boundaries.

Skip all rust skill invocations for `docs` or `tooling` scope.

### Style and structure
- Follow neighbouring file style. Check existing crates before writing new abstractions.
- Update `AGENTS.md` / `CLAUDE.md` only when shipped work changes a previously stated rule. Prefer ticking a checkbox or making the shortest possible tweak.
- New crate? Register it in the workspace root `Cargo.toml` `[workspace] members` array — a new crate with no entry compiles in isolation but is invisible to `cargo test --workspace`.

### Production-crate safety (clippy-denied — these are non-negotiable)
- **No `unsafe`** by default; per-crate override needs separate review.
- **No raw `f64` for money/prices/quantities/probabilities.** Use `rust_decimal` or integer ticks/cents/bps. `f64` conversions go through `decimal_from_f64` + `RoundingPolicy`.
- **No `unwrap`/`expect`/unchecked `panic!`** in production crates. For "infallible" `Result`s (e.g. `ReconstructionQuality::new(100)` where `100` is in range), use `.map_err(|_| MyError::Internal)?` and add an `Internal` / `Unreachable` variant to your error enum. The variant is dead code in practice but keeps the lint and the proof-of-totality together.
- **No narrowing `as` casts.** `usize as i32`, `u64 as u32`, `i64 as i32` silently truncate; clippy's default lints don't catch them (`cast_possible_truncation` is pedantic-level). Idioms:
  - Saturating: `i32::try_from(x).unwrap_or(i32::MAX)`
  - Hard fail: `i32::try_from(x).map_err(|_| Error::OutOfRange)?`
  - Hold in the wider type until a validated boundary, then convert once.

  Detect before pushing: `git diff origin/main...HEAD -- '*.rs' | grep -E '\) as (i|u)(8|16|32|64|size)\b'`. Every match is a review item.
- **No unbounded channels in hot paths.** Backpressure policy declared per connector.

### Numeric thresholds and stubs
- **No new numeric threshold without adding a default to `_GLOSSARY.md` or the canonical TOML in `19-`.** Reference it from prose elsewhere; never duplicate.
- **Stub/deferred fields use honest sentinels.** When a field is deferred to a later phase, use the value that fails safely if downstream code applies a gate: `0`, `false`, `None`, `HashMap::new()`, empty `Vec`. A stub that *passes* a downstream eligibility check is a logic error, not a deferral. Concrete: `closed_trades_in_window: 0` not `: 1`; `operator_id: None` not `Some(default)`; `realized_pnl_usd: HashMap::new()` not a synthesised value.
- **Document non-obvious preconditions** on every `pub fn` that returns a sentinel or default for edge inputs. Triggers: methods that read accumulated state and may be called before any input has been ingested; methods whose output depends on a clock that may be the epoch sentinel; methods whose validity depends on prior calls in a specific order. A one-line `# Precondition` note on the method is enough.

### Tests
- Tests live alongside implementation. Property tests via `proptest`, snapshot via `insta`, golden via `trybuild` where it fits. Use `rust_decimal_macros::dec!` in test literals.
- Inline tests need `#[cfg(test)] #[allow(clippy::unwrap_used, clippy::expect_used)] mod tests { … }` because the workspace-level deny is global. For `tests/scenario_*.rs` files, use crate-level `#![cfg(feature = "scenario")] #![allow(clippy::unwrap_used, clippy::expect_used)]` at the top.

### Pre-gate hygiene
- **Format before gating.** Run `cargo fmt --all` (not `--check`) as the last action before the gate. The gate's `cargo fmt --all --check` will then pass on the first try; otherwise you waste a fmt → re-edit → re-run-gate cycle every PR.
- **The full gate must be the LAST thing before `git commit` — no edits in between.** If you fix one finding from the gate or make any code change after the gate ran, RE-RUN THE FULL GATE before committing. A passing gate from 5 minutes ago is worthless if the file changed since. Past failure: PR #122 (2026-05-09) was committed with a stale fmt state because an edit slipped in after the local gate had passed.
- **Pre-flight grep for the silent-bug class.** Quick sanity scan before the gate:
  ```bash
  git diff origin/main...HEAD -- '*.rs' | \
    grep -nE '\) as (i|u)(8|16|32|64|size)\b|\.unwrap\(\)|\.expect\(' | \
    grep -v '^[+-][[:space:]]*//'
  ```
  Hits inside production code (non-`tests/`, non-`#[cfg(test)]`) need to be addressed before the gate.

## 5. Verify (local acceptance gate)

The full gate below assumes `rust` scope. By task scope:

- **rust**: full gate (all commands).
- **tooling**: only the gate(s) relevant to changed files. `deny.toml` change → `cargo deny check`. `rust-toolchain.toml` change → `rustc --version` + `cargo metadata --locked`. `.github/workflows/*.yml` change → none locally; CI is authoritative.
- **docs**: skip the cargo gate entirely. Run a markdown link check if available; otherwise rely on CI.

Sync first:

```bash
git fetch origin main && git merge origin/main
```

If anything was pulled in, re-read the new code paths before continuing.

Check for file overlap with other active worktrees before running the gate:

```bash
clash status
```

If overlap exists, coordinate with the user before proceeding — integration conflicts are cheaper to resolve before CI than after.

Run the exact gate from `CLAUDE.md`:

```bash
rustc --version                                                              # must contain 1.95.0
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --doc --workspace --all-features    # doctests only — nextest covers the rest
cargo nextest run --workspace --all-features   # unit + integration + scenario tests
cargo deny check
cargo audit
cargo metadata --locked --format-version 1 > /dev/null
```

Single-test repro: `cargo nextest run -p <crate> <test_name>`.

### Push gate (referenced from steps 6 and 8)

Every gate command above must exit 0 against the current working tree before any `git push`. Fix locally until green.

**Pre-existing exception.** A failure that reproduces on unmodified `origin/main` *without your branch's changes* is pre-existing. Verify by stashing or switching to `origin/main` and confirming the failure exists there independently. Note in the PR body's **Gate exceptions** slot. Do not block.

Pre-existing does **not** cover:
- A test you added that fails — that is a new failure.
- A test that previously passed but now fails on this branch — that is a regression. Fix before pushing.

**Flaky tests.** If the same test fails non-deterministically across 3 sequential runs, it is flaky — a determinism leak in the test itself (RNG seed, clock, parallel state — see step 5b's determinism rules). Fix the test, then push. Flakes are not pre-existing.

**Gate-not-runnable exception.** If a command cannot be run locally (e.g. `cargo-audit` not installed), name it in the PR body's **Gate exceptions** slot with the reason. CI is then the authoritative gate for that one command.

### Unsafe / FFI scan (rust scope only)

After the gate is green, scan the diff:

```bash
git diff origin/main...HEAD -- '*.rs' | \
  grep -E '\bunsafe\b|\bextern\s*"|MaybeUninit|NonNull|\*mut\s|\*const\s' && echo MATCH || echo CLEAN
```

If any match, invoke `rust-skills:unsafe-checker` on the matching files and document each block in the PR body with a `SAFETY:` rationale. `unsafe` is forbidden by default per `CLAUDE.md` — landing it requires explicit user approval.

Skip this scan for `docs` or `tooling` scope, and for `rust` diffs whose grep returns CLEAN.

## 5b. Use-case verification (scenario gate)

Step 5 proves the code compiles, lints, and passes unit/property tests. This step proves the **feature does the right thing end-to-end** under realistic conditions. Run after step 5 is green and before opening the PR.

### When to skip

Skip if the diff has no observable behavioural change: doc-only PRs, pure refactors with unchanged public APIs, dependency bumps that pass CI, CI/tooling tweaks. State `Scenario verification: N/A — <reason>` in the PR body so the skip is explicit.

### Determine scope

Comprehensively cover the changed public surface: every `pub fn` in the diff that takes external input or produces external output needs a scenario unless a sibling scenario already exercises the same path. For a small diff this is 1–3 use-cases; for a larger one, more — do not cap coverage at 3 to save effort. Ask: *"What would break silently if I wired this up wrong?"* and *"What would the user actually do with this feature on day one?"* — not *"what does a unit test already cover?"*

Derive scenarios from the public API: every `pub fn` that takes external input or produces external output is a scenario boundary. Two anchor examples:

- `event-log`: write N frames → simulate mid-frame crash via truncation → re-open → assert chain verifies up to the truncation point.
- `strategy-*`: feed a recorded sequence of leader trades → assert emitted `OrderIntent` fields match expected Kelly output for a frozen config.

### Where scenario tests live (single rule)

`crates/<x>/tests/scenario_<name>.rs`, gated by a `scenario` cargo feature on the crate. No `examples/`, no separate scenario crates, no ad-hoc binaries.

Scenarios run automatically as part of step 5 because `cargo nextest run --workspace --all-features` enables every declared feature, including `scenario`. The feature flag's purpose is to keep scenarios out of `cargo test` runs that omit `--all-features` (e.g. quick local iteration) and to make scenario membership explicit at the crate level.

For targeted single-scenario reruns:

```bash
cargo nextest run -p <crate> --features scenario <scenario_name>
```

This convention does not yet exist in the repo — establish it on the first crate that needs scenarios and reference that crate's `Cargo.toml` from then on.

### Determinism (mandatory)

Scenarios must be reproducible bit-for-bit:

- Fixed RNG seed (no `thread_rng()`).
- Frozen clock via injected `Clock` trait or fixture timestamps (no `SystemTime::now()`).
- Recorded fixtures only; no live network calls. Fixtures live under `crates/<x>/tests/fixtures/` and are committed.

A scenario that passes once and fails intermittently is a worse signal than no scenario at all — fix the determinism leak before re-running.

### Fixture and ephemeral state

Generate fixture data in-process where possible. If the scenario needs to write to disk (e.g. event-log on-disk format), write under `target/` so it stays untracked, and clean up at the end of the test via `tempfile::TempDir` or equivalent. Do not write under `target/scenario/` or any other shared path that could collide between parallel scenario runs.

Committed fixtures under `crates/<x>/tests/fixtures/` are intentional and reused across runs. New fixtures need a one-line comment explaining what they represent.

### Exercise and assert

Write the pass criterion **before** running. One assertion per scenario, not a compound:

```
Scenario: event-log truncation recovery
PASS: every frame written before the truncation point verifies under BLAKE3 chain check
FAIL: any pre-truncation frame fails verification, OR opener panics
```

Compound criteria (multiple `AND`s) split into separate scenarios.

Print the PASS/FAIL line for every scenario to stdout when it runs (a `println!` or a descriptive test name is enough) so the full result set is visible in the gate output, not merely inferred from the exit code. Fix any failure in step 4, re-run step 5, and re-run this step until every scenario prints PASS — all before the step-6 commit.

### On failure

Scenario failure is a **blocker** — do not proceed to step 6. Fix in step 4, re-run step 5, re-run this step. Never loosen the assertion to make it pass.

### Record in PR body

Add a **Scenario verification** subsection to the PR body listing:
- Scenarios exercised (one line each) and pass/fail.
- New fixtures added under `tests/fixtures/` and what they represent.
- Or `N/A — <reason>` per the skip rule above.

## 6. Code review (mandatory pre-merge)

Commit so the diff is stable. Commit messages: imperative mood, present tense, ≤72-char summary, optional scoped prefix (`Phase 0A:`, `core-types:`). Body explains *why*, not *what*.

```bash
git add <paths>
git commit -m "<imperative summary>"
```

**Pre-push sync (mandatory).** Steps 5, 5b, and PR-body drafting can take long enough for `main` to move, especially with parallel cycles in flight. Sync before pushing:

```bash
git fetch origin main
if ! git merge-base --is-ancestor origin/main HEAD; then
  git merge origin/main
  # Re-run step 5 acceptance gate AND step 5b scenario gate against merged state.
fi
```

**Apply the step 5 push gate before `git push`.** Do not proceed if any test fails, any gate command exits non-zero, or a required re-run after merge has not completed green. Fix locally, re-run, then push.

```bash
git push -u origin feat/<short-name>
```

Open the PR with the body required by `AGENTS.md`:

```bash
gh pr create --base main --title "<scope>: <summary>" --body "$(cat <<'EOF'
## Summary
<1-3 bullets — why, not what>

Closes #<issue>   # omit this line if no issue was identified in step 2

## Scope tag
<rust | docs | tooling | mixed:rust+docs | …>

## Files changed
<list or table>

## Tests run
Full step 5 acceptance gate: <all green | failures noted in Gate exceptions>
Step 5b scenario gate: <pass list | N/A — reason>

## Gate exceptions
<none | command not runnable: <which> — <reason> | pre-existing failure: <test> — verified on origin/main>

## Replay impact
<none | what changes in replay output>

## Risk impact
<none | risk-engine input/output deltas>

## API docs checked
<none | source/venue doc + Last checked update in docs/15-SOURCES.md>

## Deployment impact
<none | config/migration notes>

## Rollback plan
<revert PR | feature flag | config rollback>
EOF
)"
```

Run the `code-review:code-review` skill against the PR via an **Opus subagent** so review reasoning runs on Opus regardless of the main session's model. The main session stays on its current model and resumes once the subagent returns.

Dispatch via the `Agent` tool:

```
Agent(
  description="PR review on Opus",
  subagent_type="general-purpose",
  model="opus",
  prompt="Invoke the code-review:code-review skill against PR #<PR#> by calling Skill(skill='code-review:code-review', args='<PR#>'). The skill spawns parallel reviewers that audit CLAUDE.md adherence, scan for bugs, read git blame, check prior PR comments on the same files, and review code-comment guidance; only findings ≥70 confidence are posted to the PR. When it returns, summarise: number of findings posted, file/line list, and severity distribution. Do not fix anything — fixes happen on the main session."
)
```

Do not invoke `code-review:code-review` directly via `Skill` from the main session — that runs the review on the main session's model, defeating the Opus split. Do not invoke as `/code-review` (slash-command form errors out). Do not substitute `superpowers:code-reviewer` (single-pass subagent with different output semantics).

Once the Agent returns, you are back on the main session model. Triage and fix-up of findings run there.

Before acting on any finding, **locate the cited code in your actual diff**:

```bash
git diff origin/main...HEAD -- '*.rs' '*.toml' '*.md' | less   # eyeball it
git diff origin/main...HEAD --stat                              # files actually touched
```

For each finding, confirm: (1) does it cite a file in the diff stat? (2) does the cited symbol/snippet appear in the diff? If a finding references code that isn't in your diff, it's likely a false positive — the reviewer reasoned from pre-PR baseline rather than the committed change. This is common on private repos where the reviewer can't fetch the diff.

Three categories:
- **Confirmed** (code is in the diff and the analysis is correct) → fix.
- **False positive** (code is not in the diff, or analysis describes the pre-PR baseline) → discard with a one-line note.
- **Adjacent** (code is in the diff but the criticism is wrong because of context the reviewer missed) → reply to the comment with the correction; do not change code.

Only commit fixes for the **Confirmed** category. Before pushing the fixes, **re-run the full step 5 acceptance gate and step 5b scenario gate** — code-review fixes can introduce new failures and the push gate applies here too:

```bash
git commit -am "Address code-review findings"
# Re-run the full step 5 gate (all 7 commands) and step 5b scenarios.
# Push gate (step 5) applies — do not push if any command exits non-zero.
git push
```

If no findings are posted (all scored <70), proceed directly to step 7.

Re-run `Skill(skill="code-review:code-review", args="<PR#>")` if fixes were structural enough to warrant a fresh pass.

## 7. Wait for CI

> **STOP. Cardinal rule: NEVER `gh pr merge` before `gh pr checks <PR#> --watch --fail-fast` exits 0.**
> Code review (step 6) passing ≠ ready to merge. Local gate green ≠ CI will pass (rustfmt version drift, cache state, transient flakes all happen). **CI is the gate.**
> Past failures from skipping this: PR #122 (2026-05-09) merged with red CI because the local fmt state didn't match what was committed; broke main for 15 minutes.

**Do not pause for human PR review.** Once code-review findings (if any) are addressed and pushed, proceed straight through CI wait → merge → post-merge CI → cleanup without stopping. The user reviews via standing authorization; they will interrupt if they want to halt.

GitHub Actions takes ~30s to register checks after push.

```bash
sleep 30
gh pr checks <PR#> --watch --fail-fast
```

Blocks until conclusive. Exit 0 = all green. Non-zero = get the run ID and inspect:

```bash
gh run list --branch main --limit 3
gh run view <run-id> --log-failed
```

Fix in the worktree, push, re-poll. Do not advance to step 8 until this exits 0.

## 8. Final sync check

`main` may have moved during code-review or CI. Re-sync and re-trigger CI if so:

```bash
git fetch origin main
if ! git merge-base --is-ancestor origin/main HEAD; then
  git merge origin/main
  # Re-run step 5 acceptance gate AND step 5b scenario gate against merged state.
  # Push gate (step 5) applies — do not push unless all commands exit 0.
  git push
  sleep 30
  gh pr checks <PR#> --watch --fail-fast
fi
```

For shared docs (`_GLOSSARY.md`, `19-`), keep both sets of changes on conflict. For code conflicts, integrate — do not blindly accept either side. If two PRs introduce the same numeric default in different places, escalate to user — only one canonical home is allowed.

## 9. Merge

Only after CI is green AND the branch contains the latest `main`:

```bash
gh pr merge <PR#> --squash --delete-branch
```

Verify the squash commit message reads as a single coherent change.

If merge is rejected (branch protection, required reviewer, etc.), STOP. Do not clean up. PR, remote branch, and worktree must remain intact. Report blocker to user and wait.

## 10. Verify post-merge CI

> **STOP. Cardinal rule: NEVER start the next task until main CI is confirmed green after this merge.**
> The PR's own CI passing does NOT prove the squash commit on main passes — squash collapses commits and replays against fresh main, which can surface new conflicts. Skipping this check + starting new work means the next worktree gets created from a broken `origin/main` and the breakage is invisible until the next CI run.
> Past failure from skipping this: PR #122 (2026-05-09) — never verified main after merge, went straight to PR #120's worktree, main was red the whole time and only self-healed by side effect of #120's fmt run.

The squash creates a new push event on `main` that runs the full gate again:

```bash
sleep 30
RUN_ID=$(gh run list --branch main --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN_ID" --exit-status
```

If main CI fails, open a hotfix branch from the new `origin/main` HEAD, fix, PR, repeat the cycle. **Do not advance to step 11 (cleanup) and do not start any new task** until this run exits 0.

## 11. Cleanup

Mandatory after confirmed merge:

```bash
git worktree remove ../pm-<short-name>
git branch -D feat/<short-name>                                  # -D required: squash rewrites commits, branch isn't "fully merged"
git push origin --delete feat/<short-name> 2>/dev/null || true   # belt + suspenders if --delete-branch didn't fire
```

Verify nothing stale remains:

```bash
git worktree list
git branch | grep feat/<short-name> && echo STALE || echo clean
```

Update the primary checkout's local `main` to the merge you just landed, so the next cycle branches from current `origin/main` rather than a stale base (the cleanup commands above already run from the primary checkout, since you cannot remove a worktree you are inside):

```bash
git checkout main
git fetch origin main
git merge --ff-only origin/main    # fast-forward only: all work happened in the worktree, so local main must not have diverged
```

If `--ff-only` is rejected, local `main` has unexpected commits — stop and report; do not force or rebase without understanding why.

## 12. Update the issue and summarise

If a `#<issue>` was identified in step 2, comment the outcome on it (the `Closes #<issue>` line in the PR body already auto-closed it on merge; the comment records *what* was done):

```bash
gh issue comment <issue> --body "Done in #<PR#> (merged to main, CI green). <1-2 line summary of what shipped>."
```

Then return to the user:

- Merged PR URL.
- Phase/roadmap item closed.
- `Shortcuts / hacks taken: <list or "none">`.
- Any pre-existing failures noted in PR body.
- Whether `main` CI confirmed green post-merge.
- Confirmation the primary checkout's local `main` was fast-forwarded to the merge.

## Failure modes

- **`cargo fmt --all --check` fails:** run `cargo fmt --all`, commit, re-push.
- **`Cargo.toml` shows unexpected modifications after `git checkout`:** working tree from another branch bled in. `git checkout -- Cargo.toml Cargo.lock` and use a worktree. Never create branches off a dirty tree.
- **Clippy fails on a workspace lint:** check `[workspace.lints]` in root `Cargo.toml` first; the rule may be intentional. Fix the code, do not weaken the lint.
- **`cargo deny check` fails on a new dep:** add the license/source/advisory exception to `deny.toml` only with explicit user approval and a written reason. Default is to find an alternative crate.
- **`cargo audit` flags a transitive:** check if upgrade is available via `cargo update -p <crate>`. If not, document in PR body and open a tracking issue.
- **A test that previously passed now fails on this branch:** regression — fix in step 4. Do not classify as pre-existing; pre-existing means the failure exists on unmodified `origin/main` (verify by switching to `origin/main` independently).
- **A test fails non-deterministically across runs:** flaky test — likely determinism leak (`thread_rng()`, `SystemTime::now()`, parallel state). Fix the test per step 5b's determinism rules before pushing. Do not classify as pre-existing.
- **CI passes locally but fails in GH Actions:** check `rustc --version` mismatch, `--locked` violation (uncommitted `Cargo.lock`), or feature-flag drift between `--all-features` and the default set.
- **`gh pr merge` errors with "branch is checked out":** `gh` tries to switch the local checkout. Merge succeeded remotely; `--delete-branch` did not fire. Run the manual remote-delete in step 11.
- **Merge conflict on `_GLOSSARY.md` or `19-`:** keep both sets of changes; values from different sections rarely truly conflict.

## Architectural rules to honour during implementation

Step 4 implementation must respect the canonical rules in `CLAUDE.md` — specifically the "Things that are easy to get wrong" and "Hard rules" sections, plus dependency direction in `docs/16-RUST-WORKSPACE-ARCHITECTURE.md`. Do not duplicate those rules here; reference the canonical home.
