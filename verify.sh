#!/usr/bin/env bash
# Project level fixture gate for this repository.
#
# For every .rdb fixture in the repository the gate runs:
#   decode -> re-encode -> re-decode -> semantic compare
# plus a structural check of the AOF (RESP) conversion, all without a
# Redis process. Fixtures are discovered automatically; negative fixtures
# must be declared in verify/manifest.json with a reason and the exact
# expected failure phase.
#
# The script works from any working directory, never touches the network
# or the home directory for fixtures, keeps all temporary artifacts in an
# auto-collected directory and verifies the work tree is unchanged.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "verify.sh: must run inside a git work tree" >&2
  exit 1
fi

worktree_before="$(git status --porcelain)"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/rdb-verify.XXXXXXXX")"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# never resolve modules over the network; everything needed is in the
# local module cache (acceptance runs go test ./... first)
export GOPROXY=off
export VERIFY_WORK_DIR="$TMP_DIR"

echo "verify.sh: repository root: $SCRIPT_DIR"
echo "verify.sh: temporary directory: $TMP_DIR (auto-removed on exit)"

gate_status=0
go test ./verify/... -count=1 -v || gate_status=$?

worktree_after="$(git status --porcelain)"
if [[ "$worktree_before" != "$worktree_after" ]]; then
  echo "verify.sh: FAIL: work tree changed during verification:" >&2
  diff <(printf '%s\n' "$worktree_before") <(printf '%s\n' "$worktree_after") >&2 || true
  exit 1
fi

if [[ "$gate_status" -ne 0 ]]; then
  echo "verify.sh: FAIL: fixture gate reported blockers" >&2
  exit 1
fi
echo "verify.sh: OK"
