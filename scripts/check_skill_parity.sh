#!/usr/bin/env bash
# Keep Claude's repository skill projection and CLAUDE.md aligned with the
# canonical host-neutral catalog in .agents/skills.
set -euo pipefail
cd "$(dirname "$0")/.."
shopt -s dotglob nullglob

canonical_root=".agents/skills"
claude_root=".claude/skills"
declare -a canonical_names=()

die() {
  echo "ERROR: $*" >&2
  exit 1
}

path_exists() {
  [[ -e "$1" || -L "$1" ]]
}

is_canonical_name() {
  local candidate=$1 name
  for name in "${canonical_names[@]}"; do
    [[ "$candidate" == "$name" ]] && return 0
  done
  return 1
}

reject_symlinks_under() {
  local root=$1 symlinks
  symlinks=$(find "$root" -type l -print) || die "failed to inspect $root"
  [[ -z "$symlinks" ]] || die "$root must contain physical canonical files; found $symlinks"
}

load_catalog() {
  [[ -d "$canonical_root" && ! -L "$canonical_root" ]] ||
    die "$canonical_root must be a physical directory"
  reject_symlinks_under "$canonical_root"
  canonical_names=()

  local path name
  for path in "$canonical_root"/*; do
    name=${path##*/}
    ((${#name} <= 64)) || die "skill name exceeds 64 characters: $name"
    [[ "$name" =~ ^[a-z0-9]+(-[a-z0-9]+)*$ ]] || die "unsupported skill name: $name"
    [[ -d "$path" && ! -L "$path" ]] || die "canonical skill must be a physical directory: $path"
    [[ -f "$path/SKILL.md" ]] || die "canonical skill lacks SKILL.md: $path"
    canonical_names+=("$name")
  done
  ((${#canonical_names[@]} > 0)) || die "$canonical_root contains no skills"
}

validate_managed_destinations() {
  [[ -f AGENTS.md && ! -L AGENTS.md ]] || die "AGENTS.md must be the physical canonical file"

  if path_exists CLAUDE.md; then
    [[ -L CLAUDE.md ]] || die "CLAUDE.md conflicts with the managed symlink"
    [[ "$(readlink CLAUDE.md)" == "AGENTS.md" ]] || die "CLAUDE.md must point exactly to AGENTS.md"
  fi

  if path_exists .claude; then
    [[ -d .claude && ! -L .claude ]] || die ".claude must be a physical directory"
  fi
  if path_exists "$claude_root"; then
    [[ -d "$claude_root" && ! -L "$claude_root" ]] || die "$claude_root must be a physical directory"
  fi

  local name path expected
  for name in "${canonical_names[@]}"; do
    path="$claude_root/$name"
    expected="../../.agents/skills/$name"
    if path_exists "$path"; then
      [[ -L "$path" ]] || die "$path conflicts with the managed skill symlink"
      [[ "$(readlink "$path")" == "$expected" ]] || die "$path must point exactly to $expected"
    fi
  done
}

sync_projections() {
  load_catalog
  validate_managed_destinations
  mkdir -p "$claude_root"

  if ! path_exists CLAUDE.md; then
    ln -s AGENTS.md CLAUDE.md
  fi

  local path name target expected
  for path in "$claude_root"/*; do
    name=${path##*/}
    is_canonical_name "$name" && continue
    if [[ -L "$path" ]]; then
      target=$(readlink "$path")
      [[ "$target" == "../../.agents/skills/$name" ]] && rm "$path"
    fi
  done

  for name in "${canonical_names[@]}"; do
    path="$claude_root/$name"
    expected="../../.agents/skills/$name"
    path_exists "$path" || ln -s "$expected" "$path"
  done

  check_parity
  echo "OK: synchronized CLAUDE.md and Claude skill projections"
}

check_parity() {
  load_catalog
  [[ -f AGENTS.md && ! -L AGENTS.md ]] || die "AGENTS.md must be the physical canonical file"
  [[ -L CLAUDE.md ]] || die "CLAUDE.md must be a symlink"
  [[ "$(readlink CLAUDE.md)" == "AGENTS.md" ]] || die "CLAUDE.md must point exactly to AGENTS.md"
  [[ -d .claude && ! -L .claude ]] || die ".claude must be a physical directory"
  [[ -d "$claude_root" && ! -L "$claude_root" ]] || die "$claude_root must be a physical directory"

  local path name expected
  for name in "${canonical_names[@]}"; do
    path="$claude_root/$name"
    expected="../../.agents/skills/$name"
    [[ -L "$path" ]] || die "$path must be a symlink"
    [[ "$(readlink "$path")" == "$expected" ]] || die "$path must point exactly to $expected"
    [[ -f "$path/SKILL.md" ]] || die "$path does not resolve to a skill"
  done

  for path in "$claude_root"/*; do
    name=${path##*/}
    is_canonical_name "$name" || die "unexpected Claude skill entry: $path"
  done

  echo "OK: AGENTS/CLAUDE and skill projections are in parity"
}

case "${1-}" in
  "") check_parity ;;
  --sync) sync_projections ;;
  *) die "usage: $0 [--sync]" ;;
esac
