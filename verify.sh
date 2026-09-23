#!/usr/bin/env bash
# Repository level RDB fixture gate.
#
# Discovers every .rdb fixture in the repository and verifies, for each one:
# decode -> re-encode -> second decode -> semantic comparison, plus an AOF
# conversion structure check. No Redis process is required.
#
# The script can be started from any working directory. It never reads the
# home directory for fixture data and never touches the network. Temporary
# artifacts (re-encoded RDBs, AOFs, build cache) live in an auto-reclaimed
# mktemp directory. The git work tree is checked before and after the run:
# any new leftover file fails the gate.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
cd "$SCRIPT_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "verify.sh: must run inside a git work tree" >&2
  exit 1
fi
WORKTREE_BEFORE="$(git status --porcelain)"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/rdb-verify.XXXXXX")"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# hermetic build: no network, no module mutation, build cache inside tmp
export GOPROXY=off
export GOFLAGS=-mod=readonly
export GOCACHE="$TMP_DIR/gocache"

echo "== build verifier =="
go build -o "$TMP_DIR/verify" ./verify

echo "== run verifier =="
"$TMP_DIR/verify" -root "$SCRIPT_DIR" -tmp "$TMP_DIR"

WORKTREE_AFTER="$(git status --porcelain)"
if [[ "$WORKTREE_BEFORE" != "$WORKTREE_AFTER" ]]; then
  echo "verify.sh: FAIL work tree changed during verification (leftover artifacts?)" >&2
  diff <(printf '%s\n' "$WORKTREE_BEFORE") <(printf '%s\n' "$WORKTREE_AFTER") >&2 || true
  exit 1
fi

echo "verify.sh: OK"
