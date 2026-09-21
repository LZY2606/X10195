package verify

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestAllFixturesRoundTrip is the Go verification entry point invoked by the
// project-level verify.sh. It auto-discovers every *.rdb fixture and runs:
//
//	decode -> re-encode -> decode again -> strict semantic comparison
//
// plus a structural AOF conversion check. All artifacts are emitted under
// VERIFY_ARTIFACTS_DIR (an auto-cleaned temp directory) when that variable is
// set; the test itself never writes into the source tree.
func TestAllFixturesRoundTrip(t *testing.T) {
	repoRoot := repoRootFromWd(t)
	casesDir := filepath.Join(repoRoot, "cases")

	// Fixed reference instant so "expired key" classification is deterministic
	// and does not depend on when the gate happens to run.
	now := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)

	report, err := Run(Options{RepoRoot: repoRoot, CasesDir: casesDir, Now: now})
	if err != nil {
		t.Fatalf("fixture collection failed: %v", err)
	}

	var buf bytes.Buffer
	PrintReport(&buf, report)
	t.Log("\n" + buf.String())

	if artifacts := os.Getenv("VERIFY_ARTIFACTS_DIR"); artifacts != "" {
		path := filepath.Join(artifacts, "verify_report.json")
		jsonData, jerr := json.MarshalIndent(report, "", "  ")
		if jerr != nil {
			t.Fatalf("marshal report: %v", jerr)
		}
		if werr := os.WriteFile(path, jsonData, 0o644); werr != nil {
			t.Fatalf("write structured report: %v", werr)
		}
		t.Logf("structured report written to %s", path)
	}

	for _, gateErr := range report.GateErrors {
		t.Errorf("gate: %s", gateErr)
	}
	for _, res := range report.FailedResults() {
		t.Errorf("fixture %s: %s", res.Fixture.RelPath, res.Error)
	}
}

// repoRootFromWd walks up from the package directory (verify/) until it finds
// the directory containing go.mod, so the test works no matter what CWD
// `go test` was started from.
func repoRootFromWd(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repository root (go.mod)")
		}
		dir = parent
	}
}
