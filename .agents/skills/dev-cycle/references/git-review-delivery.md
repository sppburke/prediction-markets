# Git, Review, and Delivery Contract

Read this reference for every `dev-cycle`. It supplies exact repository-neutral host mechanics
specialized to prediction-markets' `main` branch, `feat/*` worktrees, GitHub CI, and squash merges.

## Contents

- Analyse remote and local state
- Create the isolated worktree
- Sync and verification binding
- Commit, review, and push guard
- Create one PR safely
- CI and review findings
- Final sync and expected-head merge
- Verify post-merge main
- Cleanup after confirmed merge and green main

## Analyse remote and local state

Run from the primary checkout before creating a worktree:

```bash
git fetch origin main
gh pr list --base main --state open --json number,title,headRefName,headRefOid,isDraft
gh pr list --base main --state merged --limit 10 --json number,title,mergedAt,files
git worktree list
git branch -a
```

Inspect the full body/diff of relevant PRs. Use `clash status` when installed as supplemental overlap
evidence; do not treat filename overlap as a substitute for semantic contract analysis.

Resolve and record:

```bash
base_head=$(git rev-parse origin/main)
```

## Create the isolated worktree

Use an explicit short name and exact recorded base:

```bash
git worktree add ../pm-<short-name> -b feat/<short-name> "$base_head"
cd ../pm-<short-name>
test "$(git merge-base HEAD "$base_head")" = "$base_head"
test -z "$(git status --porcelain)"
```

Do not switch or update the primary checkout's branch as part of setup. The worktree is the only write
location for the feature branch.

## Sync and verification binding

Before the final local gate, before push, and immediately before merge:

```bash
git fetch origin main
if ! git merge-base --is-ancestor origin/main HEAD; then
  git merge origin/main
  # Inspect the incoming range, then rerun affected verification and review.
fi
```

Required evidence is bound to a clean exact HEAD:

```bash
candidate_head=$(git rev-parse HEAD)
test -z "$(git status --porcelain)"
```

Record the SHA for each required full gate and review. If HEAD or relevant uncommitted bytes change,
the affected evidence is stale. Patch-identical reuse is allowed only when byte identity and affected
integration invariants are explicitly proven; otherwise rerun.

## Commit, review, and push guard

Commit messages use imperative mood, present tense, and a concise summary; an optional body explains
why. Stage only intended paths.

The first final review covers `origin/main...HEAD` once. Each accepted localized review-fix commit is
reviewed as the exact prior-reviewed-HEAD-to-current-HEAD delta plus affected integration invariants.
Restart whole-branch review only after a material scope, architecture, shared contract, or canonical
base-diff change.

Before every substantive push, enforce:

```bash
set -euo pipefail
candidate_head=$(git rev-parse HEAD)
test -z "$(git status --porcelain)"
test "$verified_head" = "$candidate_head"
test "$reviewed_head" = "$candidate_head"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push -u origin feat/<short-name>
```

`verified_head` and `reviewed_head` are recorded workflow state from successful required gates; do not
recompute them as a way to satisfy the guard.

## Create one PR safely

Check for an existing PR for the exact head before creating one:

```bash
gh pr list --head feat/<short-name> --state all --json number,state,url
```

Create a temporary body file outside the repository with restrictive permissions, install cleanup,
write the reviewed PR body without shell interpolation, then:

```bash
gh pr create --base main --head feat/<short-name> --title "<scope>: <summary>" --body-file "$pr_body_file"
```

Never create a second PR after an ambiguous result; query the exact head/base pair first. Verify the
returned PR number, URL, base, head SHA, and draft state.

## CI and review findings

GitHub Actions may take time to register checks. Wait for the exact PR head:

```bash
sleep 30
gh pr checks <PR#> --watch --fail-fast
```

After the command exits 0, verify the PR's remote head still equals local `HEAD`. Inspect failed logs
when nonzero, fix in the worktree, rerun affected local gates/review, push under the guard, and wait
again.

Read every available PR review and inline finding. Each finding gets `fixed`, `declined`, or
`deferred` with evidence. Do not silently drop low-severity findings. Declines are allowed only when
the finding is wrong, intended behavior, outside authorized scope, or genuine over-engineering; post
the reason where the review channel supports it.

After one combined automated-review fix batch, only a new fully evidenced material correctness,
financial-integrity, security, replay/contract, data-loss, or deploy-safety finding reopens broad
implementation. Record other repeated/non-blocking findings without creating an unbounded review loop.

## Final sync and expected-head merge

Immediately before merge:

```bash
git fetch origin main
if ! git merge-base --is-ancestor origin/main HEAD; then
  git merge origin/main
  # Rerun affected verification/review, push, and wait for CI on the new HEAD.
fi
merge_head=$(git rev-parse HEAD)
remote_head=$(gh pr view <PR#> --json headRefOid --jq .headRefOid)
test "$remote_head" = "$merge_head"
gh pr checks <PR#> --watch --fail-fast
gh pr merge <PR#> --squash --delete-branch --match-head-commit "$merge_head"
```

If merge returns nonzero, inspect remote truth:

```bash
gh pr view <PR#> --json state,mergedAt,mergeCommit,url,headRefOid,baseRefName
```

Proceed to cleanup only when `state` is `MERGED`. If still open or ambiguous, preserve the PR,
branch, and worktree.

## Verify post-merge main

Fetch `main`, identify the merge commit and its workflow rather than assuming the newest run belongs
to it, then wait for that run:

```bash
git fetch origin main
merge_sha=$(gh pr view <PR#> --json mergeCommit --jq .mergeCommit.oid)
test "$(git rev-parse origin/main)" = "$merge_sha" || git merge-base --is-ancestor "$merge_sha" origin/main
gh run list --branch main --commit "$merge_sha" --limit 10 --json databaseId,headSha,status,conclusion,name
gh run watch <RUN_ID> --exit-status
```

If no run has registered, poll with short bounded waits while continuing to update the user. Do not
start cleanup or another cycle until the merge's required `main` workflow is green.

## Cleanup after confirmed merge and green main

Use the exact primary checkout and task worktree paths recorded during isolation. Do not use
`checkout` as a cleanup precondition: verify the primary checkout first, and stop without removing
the worktree or branch when it is dirty, detached, or not already on `main`.

```bash
set -euo pipefail
primary_checkout=/absolute/path/to/prediction-markets
task_worktree="/absolute/path/to/pm-<short-name>"
task_branch="feat/<short-name>"

test "$(git -C "$primary_checkout" branch --show-current)" = main
test -z "$(git -C "$primary_checkout" status --porcelain)"
git -C "$primary_checkout" fetch origin main
git -C "$primary_checkout" merge --ff-only origin/main
git -C "$primary_checkout" worktree remove "$task_worktree"
git -C "$primary_checkout" branch -D "$task_branch"
git -C "$primary_checkout" push origin --delete "$task_branch" 2>/dev/null || true
git -C "$primary_checkout" worktree list
git -C "$primary_checkout" branch --list "$task_branch"
```

`-D` is expected after a squash merge because the feature commit graph is not an ancestor of the
squash commit. If either primary-checkout assertion or the fast-forward fails, stop: do not run any
later cleanup command or merge another branch. Never reset, force-push, or rebase local `main`
without separate evidence and authorization.

If an issue was linked, comment the merged PR and outcome after merge/main verification. Confirm all
temporary files and worktrees created by the cycle are removed; preserve anything not owned by this
cycle.
