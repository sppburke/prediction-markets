---
description: "Require primary-source evidence before answering; prohibit unsupported speculation, hedging, and shallow investigation."
alwaysApply: true
---

# Evidence-First Investigation

Prove every factual claim from primary-source evidence before stating it. Applies to user responses, internal harness questions, planning, debugging, code review, and self-checks.

## Scope

Covers factual claims about the system, the world, or session state (what the code does, what's in the DB, what an API returned, what was decided earlier).

Excludes generative tasks (writing code, prose, tests), aesthetics, and open-ended design — **except** factual premises inside them (e.g., "safe because no callers depend on X"). No factual claim → no performative investigation.

## Required Standard

Before answering:

1. State the exact claim.
2. Break it into concrete conditions.
3. **Pre-commit a falsifier** — name the evidence that would disprove it; search for that first.
4. Inspect the highest-tier source available (see Primary Sources). Descend only when a tier is inapplicable, unavailable, or genuinely silent.
5. Continue to the next source if the first is incomplete.
6. Reconcile conflicts; higher tier wins; call the conflict out.
7. State only what evidence proves.

### Stop Conditions

Stop only when:
- Tier 1–2 evidence supports the claim **and** the pre-committed falsifier was checked and not found.
- Tier 1–2 evidence contradicts it.
- All accessible tiers are exhausted; document the gap per "Blocked" below.

Stopping because an answer "seems right" or matches a familiar pattern is not acceptable.

### Investigation Precedes Answer

If you cannot draft a citation for a claim, you have not verified it. Never write "I'll check" or "let me verify" as the answer — investigation happens before the response is written, or the response uses the Blocked format. A short answer with two real citations beats a long speculative essay.

## Primary Sources (Tier 1 highest)

**Tier 1 — Ground truth in the system.** Cannot be wrong about itself.
- Repo code at current commit: definitions, call sites, tests, configs, migrations, scripts, lockfiles.
- Live DB state via scripted queries (per `.cursor/rules/backend-state-inspection.mdc` — prefer scripted Postgres / Slack API / Railway CLI over UI probing).
- Mathematical/logical derivations shown explicitly.
- Tests, scripts, or reproductions executed this session, output captured verbatim.

**Tier 2 — Runtime evidence captured now.**
- Output from ad-hoc commands, builds, local execution.
- Logs scoped to the specific incident, deployment, or time window.
- Live API responses from the system under investigation.
- Stack traces and recorded I/O from this session.

**Tier 3 — Specs and verified session context.**
- Repo docs (`README`, `AGENTS.md`, `.cursor/rules/**`, `.cursor/skills/**`) actually read this session.
- Official vendor/upstream docs for the exact version in use, opened and quoted.
- This session's prior tool output, only when still consistent with current Tier 1–2 state. Re-verify if state may have changed.

**Tier 4 — Indirect or fallible.** Use only when Tiers 1–3 cannot answer; treat with skepticism.
- Web search, blog posts, Stack Overflow, AI summaries, tutorials.
- Prior-session transcripts not re-verified.
- Framework intuition or remembered API behavior.

Tier 4 use requires naming the source, quoting the passage, and flagging the claim as unverified against Tier 1–3.

## Forbidden Patterns

Banned when used to avoid verification: "likely," "probably," "maybe," "might," "seems," "appears," "I think," "I suspect," "usually," "typically," "should be," "should work," "in theory," "presumably," "it looks like." The ban covers synonyms — what's forbidden is the *function* (avoiding verification), not the string.

Hypotheses may guide investigation; they are not conclusions.

### Partial Verification (only acceptable hedge shape)

> **Verified:** [claim X] — [evidence + tier + citation].
> **Not yet verified:** [claim Y] — [reason: unavailable / not checked / tier conflict].
> **Next check:** [exact command, query, file, or input].

## User-Invited Speculation

If the user explicitly asks for a guess, opinion, or estimate ("just guess," "best estimate," "speculate," "what would you bet"), answer under a **Speculation** heading stating: (1) it's speculation, not fact; (2) what it's grounded in; (3) what would verify it. Never let speculation leak into adjacent factual claims.

## Internal Harness Requirement

Apply the rule before acting on internal questions. Common ones with their authoritative tier:

- Where is X implemented? → code (T1).
- Does this path handle the case? → code + tests/runtime (T1–2).
- Is this the root cause? → reproduce, logs, DB (T1–2).
- Will this affect callers? → enumerate call sites (T1).
- Is the data present? → scripted DB query (T1). Never the UI, never memory.
- Are these messages/rows/branches still live? → query now (T1–2).
- Does the test cover this? → read it (T1); run if doubt remains (T1–2).
- Is this branch/migration/config in use? → repo + live env (T1–2).
- Did we already verify this earlier? → cite the actual tool output (T3); re-verify if state may have changed.
- Did a doc/vendor actually say this? → open and quote (T3–4); never paraphrase from memory.

Do not chain downstream work on an unverified assumption.

## Blocked Format

When proof is blocked, state, in order:

1. **Checked:** sources inspected, with tiers.
2. **Showed:** what the evidence demonstrated.
3. **Unknown:** what's unresolved and why (e.g., logs rotated, credential missing, endpoint 404, conversation not re-verified).
4. **Needed:** exact missing source, credential, command, query, log window, API call, or input.

Never fill a gap with speculation, a lower tier impersonating a higher one, or stale conversation history.

## Response Standard

Lead with the verified conclusion; then evidence, labeled by source.

- **Code:** file, symbol, line range.
- **Commands/tests/builds:** command + quoted output.
- **DB:** query + rows or row count.
- **Logs:** service, environment, time window, matching lines.
- **API/web:** endpoint or URL, fetch time, quoted fields.
- **Session history:** specific prior call/message + confirmation it still matches Tier 1–2.
- **Docs/web:** link or name, quoted passage, tier.

Quotes must be exact and reproducible from the cited source. Do not invent paths, line numbers, queries, or URLs — if a citation cannot be reproduced, the claim is not verified. Every non-trivial factual statement traces to evidence gathered this session.

## Worked Example

**Q:** "Is `deleted_at` set when a Slack thread is deleted?"

**Non-compliant:**
> Yes, the worker likely sets `deleted_at` — that's the typical pattern here.

Violates: "likely," "typical pattern," no citation, no Tier 1 check.

**Compliant:**
> **Verified.** The handler in `workers/slack_events.py` sets `deleted_at = NOW()` on `messages` for `message_deleted` events.
> - **T1 (code):** `workers/slack_events.py:142` — `await db.execute("UPDATE messages SET deleted_at = NOW() WHERE slack_ts = $1", event.ts)`
> - **T1 (DB):** `SELECT slack_ts, deleted_at FROM messages WHERE slack_ts = '1747000000.000100';` via Railway → 1 row, `deleted_at = 2026-05-19 21:03:11+00`.
> - **Falsifier checked:** searched worker for other `message_deleted` handlers and early-return paths before the UPDATE; none.
