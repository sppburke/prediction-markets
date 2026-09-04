#!/usr/bin/env python3
"""Reject direct unfiltered DELETE/UPDATE statements in always-applied schemas."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCHEMA_OWNERS = (
    ROOT / "scripts/supabase_schema.sql",
    ROOT / "scripts/supabase_paper_state_schema.sql",
)
# Reviewed reset SQL runs under an exclusive lock and is deliberately unconditional.
ALLOWLISTED_PATHS = frozenset(
    {
        (ROOT / "scripts/paper_reset/archive_paper_state.sql").resolve(),
        (ROOT / "scripts/paper_reset/restore_paper_state.sql").resolve(),
    }
)
CHECKED_PATHS = SCHEMA_OWNERS + tuple(sorted(ALLOWLISTED_PATHS))
DIRECT_DML = re.compile(
    r"(?i)\b(?:"
    r"delete[ \t\r\n]+from[ \t\r\n]+(?:only[ \t\r\n]+)?"
    r"(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*"
    r"|update[ \t\r\n]+(?:only[ \t\r\n]+)?"
    r"(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*"
    r"(?:[ \t\r\n]+(?:as[ \t\r\n]+)?[a-z_][a-z0-9_]*)?"
    r"[ \t\r\n]+set"
    r")\b"
)
SELF_TEST_FIXTURE = """\
create or replace function broken() returns void language plpgsql as $$
begin
  delete from service_watchlist;
end;
$$;
"""
ALIASED_UPDATE_FIXTURE = "update target_table as target set value = 1;"
NESTED_WHERE_FIXTURE = """\
update target_table
set value = (select source_value from source_table where source_id = 1);
"""


def mask_comments_and_literals(sql: str) -> str:
    """Mask comments and ordinary quoted literals while retaining line offsets."""

    chars = list(sql)
    i = 0
    state = "code"
    while i < len(chars):
        pair = sql[i : i + 2]
        char = sql[i]
        if state == "code":
            if pair == "--":
                chars[i] = chars[i + 1] = " "
                i += 2
                state = "line_comment"
                continue
            if pair == "/*":
                chars[i] = chars[i + 1] = " "
                i += 2
                state = "block_comment"
                continue
            if char == "'":
                chars[i] = " "
                i += 1
                state = "literal"
                continue
            if char == '"':
                chars[i] = " "
                i += 1
                state = "identifier"
                continue
        elif state == "line_comment":
            if char == "\n":
                state = "code"
            else:
                chars[i] = " "
        elif state == "block_comment":
            if pair == "*/":
                chars[i] = chars[i + 1] = " "
                i += 2
                state = "code"
                continue
            if char != "\n":
                chars[i] = " "
        else:
            delimiter = "'" if state == "literal" else '"'
            if char == delimiter and i + 1 < len(chars) and sql[i + 1] == delimiter:
                chars[i] = chars[i + 1] = " "
                i += 2
                continue
            if char == delimiter:
                chars[i] = " "
                state = "code"
            elif char != "\n":
                chars[i] = " "
        i += 1
    return "".join(chars)


def violations(sql: str) -> list[tuple[int, str]]:
    masked = mask_comments_and_literals(sql)
    found: list[tuple[int, str]] = []
    for match in DIRECT_DML.finditer(masked):
        statement_end = masked.find(";", match.start())
        if statement_end < 0:
            statement_end = len(masked)
        statement = masked[match.start() : statement_end]
        if not has_target_predicate(statement):
            line = masked.count("\n", 0, match.start()) + 1
            found.append((line, " ".join(match.group(0).split())))
    return found


def has_target_predicate(statement: str) -> bool:
    """Return whether the DML target has a top-level WHERE predicate."""

    depth = 0
    for token in re.finditer(r"[()]|[a-z_][a-z0-9_]*", statement, re.IGNORECASE):
        value = token.group(0).lower()
        if value == "(":
            depth += 1
        elif value == ")":
            depth = max(0, depth - 1)
        elif value == "where" and depth == 0:
            return True
    return False


def is_allowlisted(path: Path) -> bool:
    return path.resolve() in ALLOWLISTED_PATHS


def check_paths(paths: list[Path]) -> int:
    failed = False
    for path in paths:
        if is_allowlisted(path):
            continue
        try:
            sql = path.read_text(encoding="utf-8")
        except OSError as error:
            print(f"{path}: {error}", file=sys.stderr)
            failed = True
            continue
        for line, statement in violations(sql):
            display = path.resolve().relative_to(ROOT) if path.resolve().is_relative_to(ROOT) else path
            print(
                f"{display}:{line}: unfiltered SQL DML: {statement}",
                file=sys.stderr,
            )
            failed = True
    return 1 if failed else 0


def self_test() -> None:
    found = violations(SELF_TEST_FIXTURE)
    if found != [(3, "delete from service_watchlist")]:
        raise RuntimeError(f"pre-fix fixture was not rejected exactly: {found!r}")
    aliased = violations(ALIASED_UPDATE_FIXTURE)
    if aliased != [(1, "update target_table as target set")]:
        raise RuntimeError(f"aliased unfiltered UPDATE was not rejected exactly: {aliased!r}")
    nested = violations(NESTED_WHERE_FIXTURE)
    if nested != [(1, "update target_table set")]:
        raise RuntimeError(f"nested WHERE incorrectly filtered the target: {nested!r}")
    if is_allowlisted(ROOT / "scripts/paper_reset/not_reviewed.sql"):
        raise RuntimeError("paper-reset directory was allowlisted beyond the reviewed exact paths")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", type=Path)
    args = parser.parse_args()
    self_test()
    paths = [path.resolve() for path in args.paths] if args.paths else list(CHECKED_PATHS)
    return check_paths(paths)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"self-test failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
