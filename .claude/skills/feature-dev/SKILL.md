---
name: feature-dev
description: Guided feature planning workflow — understand the codebase via parallel agents, ask clarifying questions, design 2-3 architecture variants, finalize the chosen plan, and file it as a GitHub issue. Does NOT implement (implementation happens in a separate `dev-cycle` invocation from the filed issue). Use for "plan a new feature", "feature plan", "design before building", "spec out X", "file an issue for X", or any request that ends at a hand-off artifact (GitHub issue) rather than at merged code.
---

# Feature Development

You are helping a developer plan a new feature and capture the agreed plan as a GitHub issue. Follow a systematic approach: understand the codebase deeply, identify and ask about all underspecified details, design elegant architectures, then file the chosen plan as an issue. **Implementation is out of scope for this skill** — it ends at the filed issue, which serves as the hand-off artifact (typically picked up by `dev-cycle`).

## Core Principles

- **Ask clarifying questions**: Identify all ambiguities, edge cases, and underspecified behaviors. Ask specific, concrete questions rather than making assumptions. Wait for user answers before proceeding with implementation. Ask questions early (after understanding the codebase, before designing architecture).
- **Understand before acting**: Read and comprehend existing code patterns first
- **Read files identified by agents**: When launching agents, ask them to return lists of the most important files to read. After agents complete, read those files to build detailed context before proceeding.
- **Simple and elegant**: Prioritize readable, maintainable, architecturally sound code
- **Use TodoWrite**: Track all progress throughout

---

## Phase 0: Model Preflight (HARD GATE)

**Goal**: Ensure this skill runs on Claude Opus, since planning quality degrades on smaller models.

**This is a hard gate. Do not run any other phase, tool call, agent dispatch, or clarifying question until it passes.**

**Actions**:
1. Inspect the current model from the system environment (the "powered by the model named …" line). Treat this as Opus-only — any model whose name does not start with "Opus" (e.g. Sonnet, Haiku) fails the gate.
2. If the current model is **not** Opus:
   - Print exactly this message to the user and then **STOP** — emit no further text, no tool calls, no agent dispatches, no TodoWrite, nothing:

     > ⛔ `feature-dev` requires Claude Opus, but the current model is **<detected model>**.
     >
     > Please switch to an Opus model with `/model` and then re-state the feature request so the `feature-dev` skill re-triggers. I will wait — I am not going to proceed on a non-Opus model.

   - Substitute `<detected model>` with the actual model name from the environment.
   - Do **not** continue automatically when the user replies. The user must re-state the feature request after switching so the skill re-triggers; a follow-up message alone is not sufficient.
3. If the current model **is** Opus, record that in the todo list ("Phase 0: Opus confirmed — <model name>") and proceed to Phase 1.

---

## Phase 1: Discovery

**Goal**: Understand what needs to be built

Initial request: the user's feature description from their invoking message. If they only said "use feature-dev to plan X" without specifics, jump straight to the clarifying questions in step 2.

**Actions**:
1. Create todo list with all phases
2. If feature unclear, ask user for:
   - What problem are they solving?
   - What should the feature do?
   - Any constraints or requirements?
3. Summarize understanding and confirm with user

---

## Phase 2: Codebase Exploration

**Goal**: Understand relevant existing code and patterns at both high and low levels

**Actions**:
1. Launch 2-3 code-explorer agents in parallel. Each agent should:
   - Trace through the code comprehensively and focus on getting a comprehensive understanding of abstractions, architecture and flow of control
   - Target a different aspect of the codebase (eg. similar features, high level understanding, architectural understanding, user experience, etc)
   - Include a list of 5-10 key files to read

   **Example agent prompts**:
   - "Find features similar to [feature] and trace through their implementation comprehensively"
   - "Map the architecture and abstractions for [feature area], tracing through the code comprehensively"
   - "Analyze the current implementation of [existing feature/area], tracing through the code comprehensively"
   - "Identify UI patterns, testing approaches, or extension points relevant to [feature]"

2. Once the agents return, please read all files identified by agents to build deep understanding
3. Present comprehensive summary of findings and patterns discovered

---

## Phase 3: Clarifying Questions

**Goal**: Fill in gaps and resolve all ambiguities before designing

**CRITICAL**: This is one of the most important phases. DO NOT SKIP.

**Actions**:
1. Review the codebase findings and original feature request
2. Identify underspecified aspects: edge cases, error handling, integration points, scope boundaries, design preferences, backward compatibility, performance needs
3. **Present all questions to the user in a clear, organized list**
4. **Wait for answers before proceeding to architecture design**

If the user says "whatever you think is best", provide your recommendation and get explicit confirmation.

---

## Phase 4: Architecture Design

**Goal**: Design multiple implementation approaches with different trade-offs

**Actions**:
1. Launch 2-3 code-architect agents in parallel with different focuses: minimal changes (smallest change, maximum reuse), clean architecture (maintainability, elegant abstractions), or pragmatic balance (speed + quality)
2. Review all approaches and form your opinion on which fits best for this specific task (consider: small fix vs large feature, urgency, complexity, team context)
3. Present to user: brief summary of each approach, trade-offs comparison, **your recommendation with reasoning**, concrete implementation differences
4. **Ask user which approach they prefer**

---

## Phase 5: Finalize and File

**Goal**: Capture the agreed implementation plan as a durable artifact — printed to the screen and filed as a GitHub issue. **Do not write any feature code in this skill.**

**Prerequisite**: Phase 4 ended with the user choosing one architecture approach. If that approval is not explicit, ask once and wait.

**Actions**:

1. **Compose the finalized implementation plan** from prior phases. Pull only what was actually agreed; do not invent new scope. Sections, in order:
   - **Title** — short imperative summary, suitable as the GitHub issue title.
   - **Context** — 1–3 sentences from Phase 1 (problem, why now).
   - **Codebase findings** — bulletised highlights from Phase 2: relevant files, patterns to follow, integration points.
   - **Resolved questions** — each Phase 3 question with its final answer.
   - **Chosen architecture** — name + 1-paragraph description of the option the user picked in Phase 4. List the rejected options in one line each with the reason for rejection.
   - **Implementation outline** — numbered steps for the chosen architecture. Each step is a concrete change (file/module + behaviour), not a meta-instruction.
   - **Files expected to change** — list with one-line purpose per file.
   - **Out of scope** — anything explicitly deferred during Phase 3 or 4.
   - **Open risks / follow-ups** — items the user flagged as uncertain or future work.

2. **Print the full plan to the screen** as a single markdown block. Use the section headers above. Keep it self-contained — a reader who didn't sit through phases 1–4 should be able to act on it.

3. **File a GitHub issue** with the same content:
   - Verify `gh` is authenticated and a remote exists (`gh auth status`, `git remote -v`). If either fails, report the blocker and stop — do not fabricate an issue URL.
   - Title: the **Title** line from step 1.
   - Body: the full plan from step 1 (all sections), passed via `gh issue create --body-file -` or a heredoc to preserve markdown.
   - Labels: ask the user once whether to apply any (e.g. `feature`, `enhancement`). Default to none if no answer.
   - Command shape:
     ```bash
     gh issue create --title "<Title>" --body "$(cat <<'EOF'
     <full markdown plan>
     EOF
     )"
     ```

4. **Report the issue URL** returned by `gh issue create`. This URL is the hand-off artifact — implementation happens elsewhere (e.g. via `dev-cycle`).

5. **Mark all todos complete** and stop. The skill ends here. Do not begin implementation, code review, or any post-filing work — those happen in a separate `dev-cycle` invocation that picks up the filed issue.
