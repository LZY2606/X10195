#!/usr/bin/env bash
#
# verify.sh - repository-level RDB fixture verification gate.
#
# For every *.rdb fixture discovered under ./cases (recursive, sorted, no
# hard-coded file list) it runs, entirely in-process and without any Redis
# server or network access:
#
#	decode -> re-encode -> decode again -> strict semantic comparison
#
# plus a structural AOF conversion check. Files documented in
# cases/verify_manifest.json are allowed to fail only at their declared stage
# with the declared error; expected-loss fixtures pass only when the observed
# losses match their declaration exactly.
#
# The script can be started from any working directory, reads nothing from the
# caller's HOME and reaches no network. All generated artifacts live in an
# auto-cleaned temporary directory; the script additionally verifies that the
# source working tree gains no untracked/modified files from the run.
#
# Exit code is non-zero if (and only if) the gate fails.

set -euo pipefail

# --- Locate the repository root from this script's own path -----------------
script_path="${BASH_SOURCE[0]:-$0}"
script_dir="$(cd "$(dirname "${script_path}")" && pwd)"
repo_root="${script_dir}"

# --- Sealed, auto-recycled scratch space ------------------------------------
scratch="$(mktemp -d "${TMPDIR:-/tmp}/rdb-verify.XXXXXX")"
cleanup() {
	rm -rf "${scratch}"
}
trap cleanup EXIT INT TERM

# --- Isolate from HOME and the network --------------------------------------
# HOME points into the scratch dir so no global git/go/user config is read.
# GOMODCACHE is captured once below (read-only) because dependencies must
# remain resolvable without the network. GOPROXY=off forbids any module
# download, and GOSUMDB=off avoids network lookups.
export HOME="${scratch}/home"
mkdir -p "${HOME}"
export GIT_CONFIG_GLOBAL="${HOME}/.gitconfig"
export GIT_CONFIG_SYSTEM="${HOME}/.git-system-config"
: >"${GIT_CONFIG_GLOBAL}"
: >"${GIT_CONFIG_SYSTEM}"

module_cache="$(GOTOOLCHAIN=local GOPROXY=off go env GOMODCACHE 2>/dev/null || true)"
export GOPATH="${scratch}/gopath"
export GOCACHE="${scratch}/gocache"
export GOMODCACHE="${module_cache}"
export GOPROXY=off
export GOSUMDB=off
export GOFLAGS=-mod=readonly
mkdir -p "${GOPATH}" "${GOCACHE}"

# Artifacts (structured JSON report) live only in the scratch dir.
export VERIFY_ARTIFACTS_DIR="${scratch}/artifacts"
mkdir -p "${VERIFY_ARTIFACTS_DIR}"

cd "${repo_root}"

echo "==> repository root: ${repo_root}"
echo "==> scratch dir:     ${scratch} (auto-removed on exit)"
echo "==> module cache:   ${GOMODCACHE} (read-only, GOPROXY=off)"
echo

# --- Pre-flight: record the source-tree state -------------------------------
if [[ ! -d .git ]]; then
	echo "error: ${repo_root} is not a git repository; cannot guard the working tree" >&2
	exit 2
fi

snapshot="${scratch}/tree-before.txt"
git status --porcelain --untracked-files=all >"${snapshot}"

# --- Run the Go verification entry ------------------------------------------
set +e
go test ./verify/ -run TestAllFixturesRoundTrip -count=1 -v
test_status=$?
set -e

# --- Post-flight: the source tree must be unchanged --------------------------
git status --porcelain --untracked-files=all >"${scratch}/tree-after.txt"
if ! cmp -s "${snapshot}" "${scratch}/tree-after.txt"; then
	echo
	echo "error: verification run changed the source working tree:" >&2
	diff -u "${snapshot}" "${scratch}/tree-after.txt" >&2 || true
	echo "re-encoded files or logs must never be left in the source directory" >&2
	exit 2
fi

echo
if [[ ${test_status} -ne 0 ]]; then
	echo "==> VERIFY FAILED (go test exit ${test_status})"
	echo "    structured report: ${VERIFY_ARTIFACTS_DIR}/verify_report.json"
	exit "${test_status}"
fi

echo "==> VERIFY PASSED"
