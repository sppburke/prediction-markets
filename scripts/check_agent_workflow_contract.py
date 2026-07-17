#!/usr/bin/env python3
"""Validate the prediction-markets cross-host workflow skill contract."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL_ROOT = ROOT / ".agents" / "skills"
EXPECTED_SKILLS = {"feature-dev", "plan-review", "dev-cycle"}
SKILL_NAME_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
MAX_SKILL_NAME_CHARS = 64
MAX_DESCRIPTION_CHARS = 1024
MAX_SKILL_LINES = 500


class ContractError(ValueError):
    """A workflow skill violates the repository contract."""


def parse_frontmatter(path: Path) -> tuple[str, str, str]:
    text = path.read_text(encoding="utf-8")
    if text.startswith("\ufeff"):
        raise ContractError(f"{path}: UTF-8 BOM is unsupported")
    lines = text.replace("\r\n", "\n").split("\n")
    if not lines or lines[0] != "---":
        raise ContractError(f"{path}: must start with exact --- frontmatter")
    try:
        closing = lines.index("---", 1)
    except ValueError as exc:
        raise ContractError(f"{path}: missing closing frontmatter delimiter") from exc

    header = lines[1:closing]
    if (
        len(header) < 3
        or not header[0].startswith("name: ")
        or header[1] != "description: >-"
    ):
        raise ContractError(
            f"{path}: frontmatter must contain only name and folded description"
        )
    if any(not re.fullmatch(r"  \S(?:.*\S)?", line) for line in header[2:]):
        raise ContractError(f"{path}: invalid folded description line")
    name = header[0].removeprefix("name: ")
    description = " ".join(line[2:] for line in header[2:])
    if not description or len(description) > MAX_DESCRIPTION_CHARS:
        raise ContractError(
            f"{path}: description must be 1..{MAX_DESCRIPTION_CHARS} characters"
        )
    body = "\n".join(lines[closing + 1 :])
    return name, description, body


def require_all(
    label: str, text: str, needles: tuple[str, ...], failures: list[str]
) -> None:
    normalized_text = " ".join(text.split())
    for needle in needles:
        if needle not in text and " ".join(needle.split()) not in normalized_text:
            failures.append(f"{label}: missing required text {needle!r}")


def validate_catalog() -> list[str]:
    failures: list[str] = []
    actual = {path.name for path in SKILL_ROOT.iterdir() if path.is_dir()}
    missing_skills = EXPECTED_SKILLS - actual
    if missing_skills:
        failures.append(
            f"catalog is missing required workflows: {sorted(missing_skills)}"
        )

    parsed: dict[str, tuple[str, str]] = {}
    all_workflow_text = ""
    for skill_name in sorted(actual):
        if (
            len(skill_name) > MAX_SKILL_NAME_CHARS
            or SKILL_NAME_PATTERN.fullmatch(skill_name) is None
        ):
            failures.append(f"invalid portable skill name: {skill_name!r}")
        skill_path = SKILL_ROOT / skill_name / "SKILL.md"
        try:
            name, description, body = parse_frontmatter(skill_path)
        except (ContractError, OSError, UnicodeError) as exc:
            failures.append(str(exc))
            continue
        if name != skill_name:
            failures.append(f"{skill_path}: name {name!r} does not match directory")
        line_count = len(skill_path.read_text(encoding="utf-8").splitlines())
        if line_count > MAX_SKILL_LINES:
            failures.append(
                f"{skill_path}: {line_count} lines exceeds {MAX_SKILL_LINES}"
            )
        parsed[skill_name] = (description, body)
        all_workflow_text += skill_path.read_text(encoding="utf-8")

        for relative in re.findall(r"\]\((references/[^)]+)\)", body):
            reference = skill_path.parent / relative
            if not reference.is_file():
                failures.append(f"{skill_path}: missing referenced file {relative}")
                continue
            try:
                all_workflow_text += reference.read_text(encoding="utf-8")
            except (OSError, UnicodeError) as exc:
                failures.append(f"{skill_path}: cannot read {relative}: {exc}")

        require_all(
            skill_name,
            body,
            (
                "## Rigor",
                "Host Runtime",
                "Never require a particular vendor",
                "evidence-first",
            ),
            failures,
        )

    forbidden_workflow_text = (
        "Claude",
        "Codex",
        "Opus",
        "Sonnet",
        "Haiku",
        "TodoWrite",
        "Agent(",
        "Skill(",
        "Bugbot",
        "Greptile",
        "Railway",
        "agent-coordination:v1",
        "origin/dev",
        "dashboard-<short-name>",
        "operator-graph",
        "funding/operator identity requires",
        "first-sender shortcut",
    )
    for phrase in forbidden_workflow_text:
        if phrase.lower() in all_workflow_text.lower():
            failures.append(f"catalog contains forbidden workflow text: {phrase!r}")

    if "feature-dev" in parsed:
        description, body = parsed["feature-dev"]
        require_all(
            "feature-dev description",
            description,
            (
                "prediction-markets",
                "without writing code",
                "feature spec",
                "durable implementation handoff",
            ),
            failures,
        )
        require_all(
            "feature-dev",
            body,
            (
                "Contained:",
                "Complex, risky, or uncertain:",
                "two or three independent lenses only when each tests a named uncertainty",
                "Ask no more than four prioritized questions",
                "one evidence-dominant design",
                "credible rejected alternative",
                "Stop without an issue if evidence proves",
                "run one fresh read-only `plan-review` pass",
                "A caller-launched independent review never launches another reviewer",
                "search open issues and PRs for the same outcome",
                "Print and file the same self-contained plan",
                "replay/event compatibility",
                "risk/default ownership",
                "Ranking and concentration accounting are per-wallet",
                "--body-file -",
                "Do not implement",
            ),
            failures,
        )

    if "plan-review" in parsed:
        description, body = parsed["plan-review"]
        require_all(
            "plan-review description",
            description,
            (
                "not-yet-implemented",
                "approve-with-revisions",
                "Do not author a plan from scratch",
                "pull request",
            ),
            failures,
        )
        guidelines = (
            "accurate to the intent of the change",
            "internally consistent",
            "the minimum viable code change required for deployment",
            "re-uses as much as possible of the current code base",
            "leverages repo precedence when re-use is not possible",
            "is as close to a global optimum as possible while being the minimum viable code change",
            "simplicity is important and essential",
            "code elegance is the highest form of beauty",
            "over-engineering and shortcuts are equally bad and undesirable",
        )
        for number, guideline in enumerate(guidelines, start=1):
            if body.count(guideline) != 1:
                failures.append(
                    f"plan-review: guideline {number} must appear exactly once"
                )
        require_all(
            "plan-review",
            body,
            (
                "A caller-launched independent review never launches another",
                "Never recurse",
                "The submitted plan is the **Artifact**, not an evidence tier",
                "**Material-claim ledger:**",
                "`supported`",
                "`contradicted`",
                "`evidence-gap`",
                "**Named-surface/citation inventory:**",
                "strongest lower-complexity",
                "| Events and replay |",
                "| Financial and risk semantics |",
                "operation: `add | replace | delete | move`",
                "1. Any `contradicted` material claim → `reject`.",
                "2. Otherwise, any exhausted material `evidence-gap` → `blocked`.",
                "material claims enumerated: N | supported: S | contradicted: C | evidence gaps: G",
            ),
            failures,
        )

    if "dev-cycle" in parsed:
        description, body = parsed["dev-cycle"]
        require_all(
            "dev-cycle description",
            description,
            ("through merge to main", "isolated worktree", "wait for CI", "Do not use"),
            failures,
        )
        require_all(
            "dev-cycle",
            body,
            (
                "plan-review",
                "origin/main",
                "feat/<short-name>",
                "classify the planned diff as `rust`, `docs`, `tooling`, or a mixed union",
                "Any later code or test edit invalidates that evidence",
                "Bind every verification record to the exact commit SHA",
                "Review the complete `origin/main...HEAD` diff once",
                "review only the exact unreviewed delta",
                "Wait for every required GitHub check",
                "expected-head guard",
                "post-merge `main` workflow",
                "Never clean up unless remote state is confirmed `MERGED`",
                "Shortcuts / hacks taken: none",
            ),
            failures,
        )
        for reference in ("repository-gates.md", "git-review-delivery.md"):
            reference_path = SKILL_ROOT / "dev-cycle" / "references" / reference
            if not reference_path.is_file():
                failures.append(f"dev-cycle: missing required reference {reference}")

        repository_gates_path = (
            SKILL_ROOT / "dev-cycle" / "references" / "repository-gates.md"
        )
        if repository_gates_path.is_file():
            try:
                repository_gates_text = repository_gates_path.read_text(
                    encoding="utf-8"
                )
            except (OSError, UnicodeError) as exc:
                failures.append(f"dev-cycle: cannot read repository-gates.md: {exc}")
            else:
                require_all(
                    "dev-cycle repository gates",
                    repository_gates_text,
                    (
                        "Ranking and concentration accounting are per-wallet",
                        "Wallet-to-operator clustering and on-chain funder discovery were removed",
                    ),
                    failures,
                )

    agents_path = ROOT / "AGENTS.md"
    try:
        agents_text = agents_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        failures.append(f"cannot read AGENTS.md: {exc}")
    else:
        require_all(
            "AGENTS.md",
            agents_text,
            (
                "`AGENTS.md` is the canonical shared instruction file",
                "The canonical, host-neutral workflow catalog is `.agents/skills/*/SKILL.md`.",
                "bash scripts/check_skill_parity.sh --sync",
                "docs/_EVIDENCE-FIRST.md",
                "Make the minimum complete deployable change",
                "Source crates cannot depend on venue crates",
                "Strategies emit `OrderIntent`",
                "Ranking and concentration accounting are per-wallet",
                "Wallet-to-operator clustering and on-chain funder discovery were removed",
                "rustc --version",
                "cargo nextest run --workspace --all-features",
            ),
            failures,
        )
        for phrase in ("operator-graph", "first-USDC-sender shortcut"):
            if phrase in agents_text:
                failures.append(
                    f"AGENTS.md contains retired architecture text: {phrase!r}"
                )

    ci_path = ROOT / ".github" / "workflows" / "ci.yml"
    try:
        ci_text = ci_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        failures.append(f"cannot read CI workflow: {exc}")
    else:
        require_all(
            "CI workflow",
            ci_text,
            (
                "bash scripts/check_skill_parity.sh",
                "python3 scripts/check_agent_workflow_contract.py",
            ),
            failures,
        )

    return failures


def main() -> int:
    failures = validate_catalog()
    if failures:
        print("ERROR: agent workflow contract failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("OK: agent workflow contract checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
