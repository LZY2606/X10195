#!/usr/bin/env bash
# Project level RDB fixture gate.
#
# Collects every .rdb fixture in the repository and verifies, per file:
# decode -> re-encode -> re-decode -> semantic compare, plus structural
# checks of AOF conversion for supported object types. No Redis process,
# no home directory reads, no network access.
#
# Must be runnable from any working directory; temporary artifacts live in
# an auto-reclaimed directory outside the source tree. The gate fails if
# the working tree changes during the run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

snapshot_worktree() {
	git -C "$SCRIPT_DIR" status --porcelain=v1 --untracked-files=all
}

BEFORE="$(snapshot_worktree)"

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/rdb-verify.XXXXXXXX")"
cleanup() {
	rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

# hermetic build: no network, no home-directory caches
export GOPROXY=off
export GOFLAGS=-mod=mod
export GONOSUMDB='*'
export GOSUMDB=off
export GOCACHE="$TMP_ROOT/gocache"

go build -o "$TMP_ROOT/verifyfixtures" "$SCRIPT_DIR/verify"

"$TMP_ROOT/verifyfixtures" \
	-root "$SCRIPT_DIR" \
	-tmp "$TMP_ROOT/work" \
	-manifest "$SCRIPT_DIR/verify/negative_fixtures.json"

AFTER="$(snapshot_worktree)"
if [[ "$BEFORE" != "$AFTER" ]]; then
	echo "verify.sh: FAIL - working tree changed during verification:" >&2
	diff <(printf '%s\n' "$BEFORE") <(printf '%s\n' "$AFTER") >&2 || true
	exit 1
fi

echo "verify.sh: OK"
