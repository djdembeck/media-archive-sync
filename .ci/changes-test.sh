#!/usr/bin/env bash
#
# changes-test.sh — focused harness for .ci/changes.sh (the classifier).
#
# Builds a scratch git repo, then drives changes.sh (as a subprocess) against
# controlled BASE/HEAD pairs, asserting the exact flag set AND exit code per
# the point-4 taxonomy. A failing assertion (wrong flags OR wrong exit code)
# exits nonzero so the workflow-lint job that runs it fails.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLASSIFIER="$SCRIPT_DIR/changes.sh"
[ -f "$CLASSIFIER" ] || { echo "FAIL: classifier $CLASSIFIER missing" >&2; exit 1; }

WORKDIR="$(mktemp -d)"
REPO="$WORKDIR/repo"
FAILED=0

cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

new_repo() {
  rm -rf "$REPO"
  mkdir -p "$REPO"
  git init -q "$REPO"
  git -C "$REPO" config user.email t@t
  git -C "$REPO" config user.name t
}

commit() { # commit <file>...
  local f
  for f in "$@"; do
    mkdir -p "$REPO/$(dirname "$f")"
    printf 'x\n' > "$REPO/$f"
  done
  git -C "$REPO" add -A
  git -C "$REPO" commit -qm "add: $*"
}

sha() { git -C "$REPO" rev-parse "HEAD${1:-}"; }

# run_classifier: run changes.sh ONCE in a subprocess, cwd=REPO, env passed
# in. Sets RC (exit code), FLAGS (the FULL stdout, sorted — the machine
# contract is EXACTLY the four <flag>=true|false lines, so any extra stdout
# line fails the case), and ERR (stderr, for debugging).
run_classifier() {
  local err_file="$WORKDIR/classifier.err"
  out="$(cd "$REPO" && EVENT="$EVENT" REF="${REF:-}" BASE="${BASE:-}" HEAD="${HEAD:-}" \
    BEFORE="${BEFORE:-}" SHA="${SHA:-}" bash "$CLASSIFIER" 2>"$err_file")"
  rc=$?
  ERR="$(cat "$err_file" 2>/dev/null || true)"
  rm -f "$err_file"
  RC=$rc
  FLAGS="$(printf '%s\n' "$out" | sort | tr '\n' ' ' | sed 's/ $//')"
}

# run_case <name> <expected_flags...> <expected_exit>
run_case() {
  local name="$1" expected="$2" exp_exit="$3"
  run_classifier
  local exp_sorted
  exp_sorted="$(printf '%s\n' $expected | sort | tr '\n' ' ' | sed 's/ $//')"
  local ok=1
  [ "$RC" -eq "$exp_exit" ] || ok=0
  [ "$FLAGS" = "$exp_sorted" ] || ok=0
  if [ "$ok" -eq 1 ]; then
    echo "PASS $name (rc=$RC) :: $FLAGS"
  else
    echo "FAIL $name"
    echo "   expected rc=$exp_exit got rc=$RC"
    echo "   expected: $exp_sorted"
    echo "   got:      $FLAGS"
    [ -n "$ERR" ] && { echo "   --- stderr ---"; printf '%s\n' "$ERR" | sed 's/^/   | /'; }
    FAILED=1
  fi
}

# Event env setters (operate on the current REPO). sha <suffix> -> HEAD<suffix>.
pr_env()   { BASE="$(sha '~1')"; HEAD="$(sha '')"; EVENT=pull_request; REF=""; BEFORE=""; SHA=""; }
push_env() { BEFORE="$(sha '~1')"; SHA="$(sha '')"; EVENT=push; REF="refs/heads/main"; BASE=""; HEAD=""; }
zero_env() { BEFORE="0000000000000000000000000000000000000000"; SHA="$(sha '')"; EVENT=push; REF="refs/heads/newbranch"; BASE=""; HEAD=""; }
tag_env()  { EVENT=push; REF="refs/tags/v0.0.1"; BASE=""; HEAD=""; BEFORE=""; SHA=""; }
disp_env() { EVENT=workflow_dispatch; BASE=""; HEAD=""; BEFORE=""; SHA=""; REF=""; }

# seed: establish a baseline commit so HEAD~1 always resolves for pr_env/push_env.
seed() { commit "$1"; }

# ---------------------------------------------------------------------------
# 1. docs-only -> all flags false
new_repo
commit README.md
commit docs/guide.md
commit docs/sub/deep.md
pr_env
run_case "docs-only" "workflow=false lint=false test=false docker=false" 0

# 2. workflow-only -> workflow=true, rest false
new_repo
commit src/a.py tests/test_a.py
commit .github/workflows/ci.yml
pr_env
run_case "workflow-only" "workflow=true lint=false test=false docker=false" 0

# 3. classifier edit -> workflow=true (the .ci/ harness path)
new_repo
commit src/a.py
commit .ci/changes-test.sh
pr_env
run_case "classifier-only" "workflow=true lint=false test=false docker=false" 0

# 4. source-only -> lint+test+docker (src/* is in docker's closure)
new_repo
commit tests/test_a.py
commit src/media_archive_sync/cli.py
pr_env
run_case "source-only" "workflow=false lint=true test=true docker=true" 0

# 5. test-only -> lint+test, NOT docker
new_repo
commit src/a.py
commit tests/test_new.py
pr_env
run_case "test-only" "workflow=false lint=true test=true docker=false" 0

# 6. pyproject.toml -> lint+test+docker
new_repo
commit src/a.py
commit pyproject.toml
pr_env
run_case "manifest" "workflow=false lint=true test=true docker=true" 0

# 7. Dockerfile -> docker only
new_repo
commit src/a.py
commit Dockerfile
pr_env
run_case "dockerfile" "workflow=false lint=false test=false docker=true" 0

# 8. rename into docs (source file moved under docs/) -> NOT docs-only
#    --no-renames emits the delete (src/old.py) AND the add (docs/old.md).
new_repo
seed README.md
commit src/old.py
mkdir -p "$REPO/docs"
git -C "$REPO" mv src/old.py docs/old.md
git -C "$REPO" commit -qm "rename into docs"
pr_env
run_case "rename-into-docs" "workflow=false lint=true test=true docker=true" 0

# 9. path with spaces -> unknown -> all code concerns true
new_repo
seed README.md
commit "src/dir with space/file name.py"
pr_env
run_case "path-with-spaces" "workflow=false lint=true test=true docker=true" 0

# 10. path with a newline in the name -> unknown (NUL-safe) -> all code concerns
new_repo
seed README.md
commit $'src/line\nbreak.py'
pr_env
run_case "path-with-newline" "workflow=false lint=true test=true docker=true" 0

# 11. unknown path (examples/ has no scope and is not inert) -> all code concerns
new_repo
commit src/a.py
commit examples/basic_download.py
pr_env
run_case "unknown-path-fallback" "workflow=false lint=true test=true docker=true" 0

# 12. clean empty diff -> all flags true + exit 0
new_repo
commit README.md
BASE="$(sha '')"; HEAD="$(sha '')"; EVENT=pull_request; REF=""; BEFORE=""; SHA=""
# merge-base of HEAD..HEAD is HEAD -> empty diff
run_case "clean-empty-diff" "workflow=true lint=true test=true docker=true" 0

# 13. zero/absent base (new branch) -> all flags true + exit 0
new_repo
commit README.md
zero_env
run_case "zero-base" "workflow=true lint=true test=true docker=true" 0

# 14. unresolvable base -> all flags true + exit 0
new_repo
commit README.md
EVENT=pull_request; REF=""; BASE="deadbeef00000000000000000000000000000000"; HEAD="$(sha '')"; BEFORE=""; SHA=""
run_case "unresolvable-base" "workflow=true lint=true test=true docker=true" 0

# 15. tag event (no diff) -> all flags true + exit 0
new_repo
commit README.md
tag_env
run_case "tag-event" "workflow=true lint=true test=true docker=true" 0

# 15b. workflow_dispatch (no event) -> all flags true + exit 0
new_repo
commit README.md
disp_env
run_case "dispatch-event" "workflow=true lint=true test=true docker=true" 0

# 15c. push to a branch (normal before..sha diff) -> classified by diff
new_repo
seed README.md
commit src/a.py
push_env
run_case "push-branch" "workflow=false lint=true test=true docker=true" 0

# ---------------------------------------------------------------------------
# 16. corrupted output -> the classifier must never emit a non-boolean flag
#     (the corruption the downstream Gates defend against).
new_repo
seed README.md
commit src/a.py
pr_env
raw="$(cd "$REPO" && EVENT="$EVENT" REF="" BASE="$BASE" HEAD="$HEAD" BEFORE="" SHA="" bash "$CLASSIFIER" 2>/dev/null)"
corrupt="$(printf '%s\n' "$raw" | grep -E '^(workflow|lint|test|docker)=' | grep -vE '=(true|false)$' || true)"
if [ -z "$corrupt" ]; then
  echo "PASS corrupted-output (classifier emits only bare booleans)"
else
  echo "FAIL corrupted-output: non-boolean flag emitted"
  printf '%s\n' "$corrupt" | sed 's/^/   | /'
  FAILED=1
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "ALL CASES PASSED"
  exit 0
else
  echo "SOME CASES FAILED"
  exit 1
fi
