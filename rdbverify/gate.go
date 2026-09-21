package rdbverify

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Verdict values for a single fixture.
const (
	VerdictLossless     = "lossless"
	VerdictExpectedLoss = "expected-loss"
	VerdictNegativeOK   = "negative-ok"
	VerdictBlocker      = "blocker"
)

// FixtureResult is the structured outcome for one fixture.
type FixtureResult struct {
	Path        string     `json:"path"`
	Magic       string     `json:"magic"`
	RDBVersion  int        `json:"rdbVersion"`
	DBs         []int      `json:"dbs"`
	Types       []string   `json:"types"`
	Encodings   []string   `json:"encodings"`
	Features    []string   `json:"features"`
	Verdict     string     `json:"verdict"`
	Failure     string     `json:"failure,omitempty"`
	FailedStage Stage      `json:"failedStage,omitempty"`
	Diffs       []Diff     `json:"diffs,omitempty"`
	AOF         *AOFReport `json:"aof,omitempty"`
}

// Report is the full gate result.
type Report struct {
	FixtureCount int             `json:"fixtureCount"`
	Passed       int             `json:"passed"`
	Blockers     []FixtureResult `json:"blockers"`
	Results      []FixtureResult `json:"results"`
}

// Config controls a gate run.
type Config struct {
	RepoRoot     string
	ManifestPath string
}

// Run discovers every fixture, verifies it and returns a structured report.
func Run(cfg Config) (*Report, error) {
	fixtures, err := DiscoverRDBs(cfg.RepoRoot)
	if err != nil {
		return nil, fmt.Errorf("discover fixtures: %w", err)
	}
	if len(fixtures) == 0 {
		return nil, fmt.Errorf("fixture collection is empty: no *.rdb files under %s", cfg.RepoRoot)
	}
	manifest, err := LoadManifest(cfg.ManifestPath)
	if err != nil {
		return nil, err
	}
	if err := assertManifestCoverage(manifest, fixtures); err != nil {
		return nil, err
	}

	report := &Report{FixtureCount: len(fixtures)}
	fmt.Printf("Collected %d fixture(s) from %s\n", len(fixtures), cfg.RepoRoot)
	for _, rel := range fixtures {
		res := verifyOne(cfg.RepoRoot, rel, manifest)
		report.Results = append(report.Results, res)
		printFixtureLine(res)
		if res.Verdict == VerdictBlocker {
			report.Blockers = append(report.Blockers, res)
		} else {
			report.Passed++
		}
	}
	return report, nil
}

func assertManifestCoverage(manifest *Manifest, fixtures []string) error {
	known := make(map[string]struct{}, len(fixtures))
	for _, f := range fixtures {
		known[f] = struct{}{}
	}
	for _, n := range manifest.Negatives {
		if _, ok := known[n.Path]; !ok {
			return fmt.Errorf("manifest negative %s does not match any collected fixture", n.Path)
		}
	}
	for _, e := range manifest.ExpectedLosses {
		if _, ok := known[e.Path]; !ok {
			return fmt.Errorf("manifest expected-loss %s does not match any collected fixture", e.Path)
		}
	}
	return nil
}

func verifyOne(root, rel string, manifest *Manifest) FixtureResult {
	abs := filepath.Join(root, rel)
	res := FixtureResult{Path: rel}

	encoded, first, second, pipelineErr := Reencode(abs)
	_ = encoded
	if first != nil {
		fillResultFromDecode(&res, first)
	}

	var diffs []Diff
	var aofReport *AOFReport
	if pipelineErr == nil {
		diffs = CompareSnapshots(first.Snapshot, second.Snapshot)
		aofReport = CheckAOF(first)
		res.Diffs = diffs
		res.AOF = aofReport
		res.Features = detectFeatures(first, diffs, aofReport)
	}

	// Negative fixtures: the pipeline must fail at exactly the declared
	// stage with an error carrying the declared substring; semantic-stage
	// entries instead require the exact diff code listed in errorContains.
	if neg := manifest.NegativeFor(rel); neg != nil {
		return evaluateNegative(res, pipelineErr, diffs, neg)
	}

	if pipelineErr != nil {
		se, _ := pipelineErr.(*StageError)
		res.Verdict = VerdictBlocker
		if se != nil {
			res.FailedStage = se.Stage
			res.Failure = se.Err.Error()
		} else {
			res.Failure = pipelineErr.Error()
		}
		return res
	}

	loss := manifest.LossFor(rel)
	if loss == nil {
		if len(diffs) > 0 {
			res.Verdict = VerdictBlocker
			res.Failure = fmt.Sprintf("unexpected semantic loss: %s", joinDiffCodes(diffs))
			return res
		}
		if aofReport != nil && aofReport.Finding != nil && aofReport.Finding.Level == "error" {
			res.Verdict = VerdictBlocker
			res.Failure = fmt.Sprintf("AOF conversion defect: [%s] %s",
				aofReport.Finding.Code, aofReport.Finding.Detail)
			return res
		}
		res.Verdict = VerdictLossless
		return res
	}

	// Expected loss: observed diff-code multiset must match the manifest exactly.
	if !sameCodeSet(diffs, loss.LossCodes) {
		res.Verdict = VerdictBlocker
		res.Failure = fmt.Sprintf("expected-loss mismatch: manifest %v, observed %s",
			loss.LossCodes, joinDiffCodes(diffs))
		return res
	}
	if loss.AOFLossCode != "" {
		if aofReport == nil || aofReport.Finding == nil || aofReport.Finding.Code != loss.AOFLossCode {
			res.Verdict = VerdictBlocker
			res.Failure = fmt.Sprintf("expected AOF loss code %q not observed", loss.AOFLossCode)
			return res
		}
	} else if aofReport != nil && aofReport.Finding != nil && aofReport.Finding.Level == "error" {
		res.Verdict = VerdictBlocker
		res.Failure = fmt.Sprintf("AOF conversion defect: [%s] %s",
			aofReport.Finding.Code, aofReport.Finding.Detail)
		return res
	}
	res.Verdict = VerdictExpectedLoss
	return res
}

func evaluateNegative(res FixtureResult, pipelineErr error, diffs []Diff, neg *NegativeEntry) FixtureResult {
	if neg.Stage == StageSemantic {
		if pipelineErr != nil {
			res.Verdict = VerdictBlocker
			res.Failure = fmt.Sprintf("negative expected semantic diff but pipeline failed earlier: %v", pipelineErr)
			return res
		}
		if !diffsContain(diffs, DiffCode(neg.ErrorContains)) {
			res.Verdict = VerdictBlocker
			res.Failure = fmt.Sprintf("negative expected semantic diff %q, observed %s",
				neg.ErrorContains, joinDiffCodes(diffs))
			return res
		}
		res.Verdict = VerdictNegativeOK
		return res
	}
	if pipelineErr == nil {
		res.Verdict = VerdictBlocker
		res.Failure = "negative fixture unexpectedly passed the pipeline"
		return res
	}
	se, ok := pipelineErr.(*StageError)
	if !ok || se.Stage != neg.Stage {
		res.Verdict = VerdictBlocker
		res.Failure = fmt.Sprintf("negative expected failure at %q, got: %v", neg.Stage, pipelineErr)
		return res
	}
	if !strings.Contains(se.Err.Error(), neg.ErrorContains) {
		res.Verdict = VerdictBlocker
		res.Failure = fmt.Sprintf("negative error %q does not contain %q", se.Err.Error(), neg.ErrorContains)
		return res
	}
	res.Verdict = VerdictNegativeOK
	res.FailedStage = neg.Stage
	return res
}

func diffsContain(diffs []Diff, code DiffCode) bool {
	for _, d := range diffs {
		if d.Code == code {
			return true
		}
	}
	return false
}

func fillResultFromDecode(res *FixtureResult, dec *DecodeResult) {
	res.Magic = dec.Magic
	res.RDBVersion = dec.Version
	res.DBs = dec.DBIndexes
	res.Types = dec.Types
	res.Encodings = dec.Encodings
}

func sameCodeSet(diffs []Diff, want []DiffCode) bool {
	seen := map[DiffCode]int{}
	for _, d := range diffs {
		seen[d.Code]++
	}
	wantMap := map[DiffCode]int{}
	for _, c := range want {
		wantMap[c]++
	}
	if len(seen) != len(wantMap) {
		return false
	}
	for c, n := range wantMap {
		if seen[c] != n {
			return false
		}
	}
	return true
}

func joinDiffCodes(diffs []Diff) string {
	codes := make([]string, 0, len(diffs))
	for _, d := range diffs {
		codes = append(codes, string(d.Code))
	}
	sort.Strings(codes)
	return strings.Join(codes, ",")
}

func printFixtureLine(r FixtureResult) {
	version := "-"
	if r.Magic != "" {
		version = fmt.Sprintf("%s-%04d", r.Magic, r.RDBVersion)
	}
	types := strings.Join(r.Types, ",")
	if types == "" {
		types = "-"
	}
	fmt.Printf("  %-55s %-10s types=%-30s verdict=%s\n", r.Path, version, types, r.Verdict)
	if r.Verdict == VerdictBlocker {
		fmt.Printf("      BLOCKER: %s\n", r.Failure)
		for _, d := range r.Diffs {
			fmt.Printf("      - %s db=%d key=%q %s\n", d.Code, d.DB, d.Key, d.Detail)
		}
	}
}

// WriteReport writes the structured JSON report into path.
func WriteReport(path string, report *Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
