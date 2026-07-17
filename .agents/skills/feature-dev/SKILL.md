---
name: feature-dev
description: >-
  Plan a prediction-markets feature, fix, refactor, migration, or operational
  change as an evidence-backed, implementation-ready GitHub issue without
  writing code. Use for "feature spec", "plan a feature", "spec out X",
  "design before building", "file an issue for X", or any request whose
  endpoint is a durable implementation handoff rather than merged code.
---

# Feature Specification

Produce the smallest complete implementation plan supported by the user's intent, current
prediction-markets repository evidence, relevant primary sources, and engineering judgment. End at
the filed GitHub issue. If the evidence proves that no change is needed, stop with that conclusion.
Do not implement, review completed code, or continue into `dev-cycle`.

## Rigor

Use the evidence-first standard in `AGENTS.md` and `docs/_EVIDENCE-FIRST.md`. Falsify material
premises before relying on them, inspect the highest applicable authority, and distinguish verified
facts, decisions, assumptions, and unresolved evidence.

## Host Runtime Adapter

Use the host runtime's native progress tracker and read-only helper agents when available and useful.
Never require a particular vendor, model name, tool spelling, or fixed number of agents.

- Use one focused exploration pass for contained work.
- Use two or three independent lenses only when each tests a named uncertainty or materially reduces
  latency.
- The primary planner must read the load-bearing sources and independently verify every helper
  finding before using it.
- If helpers are unavailable, perform separated passes directly and disclose the fallback only when
  it affects review independence or evidence coverage.

## Prediction-Markets Context

Before planning, read the applicable root instructions and canonical docs in their authority order.
At minimum read `docs/_BASELINE.md`, `docs/_GLOSSARY.md`,
`docs/19-WINNER-FOLLOW-STRATEGY.md`, `docs/16-RUST-WORKSPACE-ARCHITECTURE.md`, and the relevant
phase, venue, source, or runbook. Re-verify touched external contracts under
`docs/21-RESEARCH-AND-SOURCE-DISCOVERY.md` and update-source expectations from
`docs/15-SOURCES.md`.

Use [references/discovery-matrix.md](references/discovery-matrix.md) when planning depth, evidence
routing, cross-cutting impact, or stopping conditions are not already clear.

## Planning Bar

Apply the repository's engineering decision standard as one quality bar:

1. preserve intent and observable behavior;
2. remain internally consistent;
3. make the minimum complete deployable change;
4. reuse the correct existing semantic owner;
5. follow repository precedent when reuse does not fit;
6. optimize the scoped whole system;
7. prefer structural simplicity;
8. define elegance as clear ownership and minimal exposed API;
9. reject speculative over-engineering and correctness, safety, or verification shortcuts equally.

Do not frame "minimal," "clean," and "pragmatic" as different quality levels. Every viable option
must satisfy the same correctness, replay, risk, operability, and verification constraints.

## Core Invariants

- Start from an existing crate, trait, service flow, event, config owner, script, or CI surface.
  Propose a new durable surface only for net-new behavior, data shape, external contract, or
  operational invariant; record the reuse search and why existing ownership is insufficient.
- Every material factual premise has a source, a falsifier or contradiction check, and a disposition.
  Use `Checked / Showed / Unknown / Needed` when proof is blocked.
- Trace intent and resolved decisions to acceptance criteria, implementation steps to owning
  files/surfaces, and every acceptance criterion to focused verification.
- Preserve the source → venue → strategy → execution dependency rules. Strategies emit
  `OrderIntent`; only execution submits. Risk behavior stays pure and deterministic. Ranking and
  concentration accounting are per-wallet; archived clustering/funder machinery is not an active
  semantic owner.
- Account explicitly for event/replay compatibility, financial types and rounding, risk/default
  ownership, idempotency/backpressure, external-source freshness, deployment, and rollback whenever
  the change can affect them.

## Phase 1: Interpret and Classify

1. Create a five-phase progress checklist.
2. If the actor or desired observable outcome is unintelligible, ask the smallest blocking question
   and wait. Otherwise record a provisional interpretation and begin evidence gathering.
3. Classify planning depth from evidence:
   - **Contained:** ownership, precedent, integration points, failure shape, and verification are
     clear and local.
   - **Complex, risky, or uncertain:** persistence/schema, money or risk, replay/event contracts,
     concurrency/idempotency, live execution, external APIs, multiple crates/services, migration or
     compatibility, broad shared infrastructure, unclear ownership, or conflicting precedent is
     material.

Classification controls investigation and safeguards, not arbitrary plan length or delivery phases.

## Phase 2: Explore and Falsify

1. Inspect repository instructions, current implementation, tests, config, schema, and current Git
   state before relying on docs or memory.
2. Inspect recent relevant issues, PRs, and commits when they can reveal current ownership or make
   the requested work obsolete or duplicative.
3. Use runtime evidence only when the claim depends on current state or repository evidence is
   silent. Keep production investigation read-only. Any consequential external mutation or cost
   requires explicit authorization.
4. Use current official primary documentation only when a material decision depends on an external
   or time-sensitive contract.
5. Record load-bearing files, existing surfaces to extend, consumers, failure/lifecycle risks,
   verification paths, and precise unknowns.
6. Stop without an issue if evidence proves the requested change already exists, is obsolete, or is
   unnecessary.

## Phase 3: Resolve Material Choices

Ask only about unresolved choices the user owns and evidence cannot settle: shipped behavior,
actors/permissions, external contract or persisted shape, lifecycle/failure semantics, scope,
rollout/cutover, verification policy, or consequential risk acceptance.

- Resolve technical facts through evidence rather than asking the user to search the repo.
- Adopt reversible, low-risk, repository-native defaults and record them without ceremony.
- Never default an externally visible, irreversible, policy-bearing, or risk-acceptance decision.
- Ask no more than four prioritized questions in one batch. Wait when a material user-owned choice
  remains unresolved.

Stop asking when intent, after-state, ownership, lifecycle, permissions, failure behavior,
acceptance criteria, non-goals, and verification are evidenced or decided.

## Phase 4: Select the Architecture

- For contained work with one evidence-dominant design, present one recommended implementation
  shape and at least one credible rejected alternative. State the concrete difference, trade-off,
  and failure mode; do not manufacture full variants.
- Use two or three architecture lenses only when genuinely different viable ownership, contract,
  persistence, migration, deployment, or integration shapes exist.
- Name the existing surface every option extends and justify every net-new durable surface.
- Ask the user to choose only when multiple viable paths depend on product preference, policy, or
  consequential risk acceptance. When evidence makes one path dominant, record it and allow
  correction without adding an approval gate.

For each presented shape, state product behavior, implementation flow, expected files/surfaces,
reuse, net-new justification, trade-offs, failure modes, mitigations, and why it is minimum complete.

## Phase 5: Review, File, and Stop

### Discover filing conventions

Read repository issue templates, label vocabulary, and duplicate policy before composing the final
artifact. Immediately before filing, search open issues and PRs for the same outcome. Apply metadata
automatically only when one unambiguous repository convention exists; otherwise omit optional
metadata or ask about a consequential required choice.

### Compose one implementation contract

Print and file the same self-contained plan with these sections:

- **Title** and **Context / intent**.
- **Evidence-backed findings**: load-bearing sources, established premises, falsifiers, existing
  owners, consumers, and unresolved evidence.
- **Resolved decisions and defaults**.
- **Chosen architecture**: product flow, existing-surface ownership, net-new justification,
  failure handling/mitigations, and credible rejected alternatives.
- **Implementation phases**: use one phase unless there is a real independently deployable,
  migration, compatibility, cutover, or blast-radius boundary. Each phase owns concrete paths,
  behavior, acceptance criteria, focused verification, dependencies, rollout/rollback, and risk.
- **Cross-cutting impact**: replay/event compatibility, risk and financial semantics, API/source-doc
  checks, deployment/config/migration impact, and rollback.
- **Out of scope** and **open risks / follow-ups**.
- **Issue filing / metadata**: target repository, duplicate result, labels/project actions, and any
  approved exception.

Include schemas, payloads, formulas, examples, or pseudocode only when they remove implementation
ambiguity. Do not duplicate phase-local details globally.

### Self-review and independent review

Review the complete candidate against all nine planning principles and the traceability mappings.
Then run one fresh read-only `plan-review` pass when the runtime can provide an independent context.
A caller-launched independent review never launches another reviewer. If independence is unavailable,
perform a separated direct pass and disclose that fallback.

Verify and disposition every finding. Print or file only an `approve` candidate, or an `approve with
revisions` candidate after all required revisions are integrated and confirmed. A material change to
scope, behavior, architecture, contract, or acceptance criteria requires a fresh full review.

### File safely

1. Print the exact reviewed title and body in full.
2. Verify authentication, remote target, and duplicate state. If unavailable, report the exact
   blocker after printing; never invent a URL.
3. Transport the exact body through standard input or a temporary body file; never interpolate
   arbitrary plan text into a shell command. For `gh`, prefer `gh issue create --title "$title"
   --body-file -` with the body supplied on standard input.
4. Verify the returned issue URL and any required metadata action. If issue creation succeeds but a
   later action fails, return the URL and name the incomplete action.
5. Stop. Do not implement, launch `dev-cycle`, or begin completed-code review in the same invocation.
