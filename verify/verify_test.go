package verify

import (
	"os"
	"path/filepath"
	"testing"
)

// TestFixtureGate runs the project level fixture gate: collect every .rdb
// fixture in the repository, decode, re-encode, re-decode and compare
// semantically, and structurally check AOF conversion. Negative fixtures
// only pass when verify/manifest.json hits exactly.
func TestFixtureGate(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	workDir := os.Getenv("VERIFY_WORK_DIR")
	if workDir == "" {
		workDir = t.TempDir()
	} else {
		workDir = filepath.Join(workDir, "gate")
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	report, err := Run(Options{Root: root, WorkDir: workDir, Out: os.Stdout})
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK() {
		t.Errorf("fixture gate failed with %d blocker(s), see [blocker] lines above", len(report.Blockers))
	}
}
