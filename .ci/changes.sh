#!/usr/bin/env bash
#
# changes.sh — self-contained diff classifier for the `changes` job in ci.yml.
#
# Reads EVENT, BASE, HEAD, BEFORE, and SHA from the environment, diffs
# base..head NUL-safely, and writes EXACTLY four `<flag>=true|false` lines to
# STDOUT — the machine contract the `changes` job maps onto its outputs by
# redirecting this script's stdout to $GITHUB_OUTPUT. Nothing else is written
# to stdout (a job summary step renders the human summary separately).
#
# Failure taxonomy (setup-ci point 4):
#   - UNABLE to classify (unresolvable/zero base, tag/dispatch no-diff,
#     clean empty diff, git diff nonzero) -> emit `true` for EVERY concern and
#     exit 0 (fail-safe full run — the real checks must still run).
#   - UNEXPECTED infrastructure failure (anything outside the classification
#     logic, e.g. mktemp) -> write an error to stderr and exit nonzero, so the
#     `changes` job reports `failure` and every downstream job skips / the run
#     is red (fail closed).
#
# Per-concern scopes (each concern's OWN config/image closure). A path is
# matched against each scope BEFORE the inert set, so a path that matches a
# scope is never treated as inert, and a single path may enable several
# concerns (e.g. src/x.py -> lint+test+docker). A path matching no scope and
# not inert enables every selected code concern (lint+test+docker); workflow
# is scope-only.

set -euo pipefail

ZERO_SHA="0000000000000000000000000000000000000000"

# emit_all_true <note> -> write all-true flags to stdout, exit 0.
emit_all_true() {
  local note="${1:-unable to classify}"
  echo "workflow=true"
  echo "lint=true"
  echo "test=true"
  echo "docker=true"
  exit 0
}

# infra_failure <msg> -> error to stderr, exit 1 (changes job MUST report failure).
infra_failure() {
  echo "changes.sh: $1" >&2
  exit 1
}

# --- Event / base / head resolution ----------------------------------------
# Any inability to resolve a valid base..head range -> all-true + exit 0.
# tag/dispatch events skip diffing entirely (conservative full run).
case "${EVENT:-}" in
  pull_request)
    [ -n "${BASE:-}" ] && [ -n "${HEAD:-}" ] || emit_all_true "pull_request missing base/head"
    mb="$(git merge-base "$BASE" "$HEAD" 2>/dev/null)" || emit_all_true "merge-base failed"
    [ -n "$mb" ] || emit_all_true "merge-base empty"
    REPO_BASE="$mb"
    REPO_HEAD="$HEAD"
    ;;
  push)
    # Tag pushes skip diffing entirely (conservative full run).
    case "${REF:-}" in
      refs/tags/*) emit_all_true "tag push (conservative full run)" ;;
    esac
    [ -n "${BEFORE:-}" ] && [ -n "${SHA:-}" ] || emit_all_true "push missing before/sha"
    [ "$BEFORE" != "$ZERO_SHA" ] || emit_all_true "new branch (before is zero SHA)"
    git rev-parse --verify -q "${BEFORE}^{commit}" >/dev/null 2>&1 || emit_all_true "before SHA unresolvable"
    REPO_BASE="$BEFORE"
    REPO_HEAD="$SHA"
    ;;
  *)
    emit_all_true "event '${EVENT:-?}' has no diff (conservative full run)"
    ;;
esac

# --- NUL-safe diff to a write-once temp file -------------------------------
DIFF_FILE="$(mktemp)" || infra_failure "mktemp failed"
trap 'rm -f "$DIFF_FILE"' EXIT

# Capture git diff's status explicitly (set -e would otherwise abort before a
# nonzero could be treated as "unable to classify").
set +e
git diff --name-only -z --no-renames "$REPO_BASE" "$REPO_HEAD" > "$DIFF_FILE"
diff_status=$?
set -e
[ "$diff_status" -eq 0 ] && [ -s "$DIFF_FILE" ] || emit_all_true "clean/empty diff or git diff failure"

workflow=false
lint=false
test=false
docker=false
unknown=false

while IFS= read -r -d '' path; do
  matched=false
  case "$path" in
    .github/workflows/* | .forgejo/workflows/* | .ci/*) workflow=true; matched=true ;;
  esac
  case "$path" in
    src/*.py | src/**/*.py | tests/*.py | tests/**/*.py | pyproject.toml) lint=true; test=true; matched=true ;;
  esac
  case "$path" in
    Dockerfile | src/* | pyproject.toml | README.md) docker=true; matched=true ;;
  esac
  case "$path" in
    *.md | docs/* | renovate.json5 | .pre-commit-config.yaml | .gitignore | .editorconfig | LICENSE | .env.example | .github/*.md | Makefile | .coderabbit.yaml) matched=true ;;
  esac
  # A path matching no scope AND not inert enables every selected code concern.
  [ "$matched" = true ] || unknown=true
done < "$DIFF_FILE"

if [ "$unknown" = true ]; then
  lint=true
  test=true
  docker=true
fi

echo "workflow=$workflow"
echo "lint=$lint"
echo "test=$test"
echo "docker=$docker"
