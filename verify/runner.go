package verify

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RequiredCoverageTags lists the scenarios the gate must observe somewhere in
// the fixture corpus. Each one is an "independent result" in the printed
// report; a missing tag means the corpus no longer exercises that scenario and
// the gate fails instead of silently narrowing its guarantees.
var RequiredCoverageTags = []string{
	"listpack",
	"stream-v1",
	"stream-v2",
	"stream-v3",
	"hfe",
	"empty-database",
	"expired-key",
	"unknown-opcode", // supplied by a negative fixture
}

// Report is the full structured result of a verification run.
type Report struct {
	FixtureCount int                 `json:"fixture_count"`
	Results      []FixtureResult     `json:"results"`
	Coverage     map[string][]string `json:"coverage"`
	// ManifestPath is the manifest actually loaded.
	ManifestPath string `json:"manifest_path"`
	// GateErrors are repository-level problems (zero collection, missing
	// coverage, manifest referencing unknown files, ...).
	GateErrors []string `json:"gate_errors"`
}

// Run executes the full gate and returns the structured report. It never
// writes into RepoRoot or CasesDir; callers control all output destinations.
func Run(opts Options) (*Report, error) {
	if opts.RepoRoot == "" || opts.CasesDir == "" {
		return nil, fmt.Errorf("RepoRoot and CasesDir are required")
	}
	report := &Report{Coverage: map[string][]string{}}

	manifest, manifestPath, merr := loadManifest(opts.RepoRoot)
	report.ManifestPath = manifestPath
	if merr != nil {
		report.GateErrors = append(report.GateErrors, merr.Error())
	}
	negative, expectedLoss := indexManifest(manifest)

	infos, decodeErrors := collect(opts)
	report.FixtureCount = len(infos)
	if len(infos) == 0 {
		return report, ErrNoFixtures
	}

	discovered := map[string]struct{}{}
	for _, info := range infos {
		discovered[info.RelPath] = struct{}{}
	}
	// Every manifest entry must hit an actually discovered file: stale entries
	// are gate errors so a removed fixture cannot leave a stale "pass".
	for _, e := range manifest.Entries {
		if _, ok := discovered[e.Path]; !ok {
			report.GateErrors = append(report.GateErrors,
				fmt.Sprintf("manifest references fixture not discovered: %s", e.Path))
		}
		if e.Reason == "" {
			report.GateErrors = append(report.GateErrors,
				fmt.Sprintf("manifest entry %s is missing a reason", e.Path))
		}
		if (e.ExpectedStage != "") == (len(e.ExpectedLosses) > 0) {
			report.GateErrors = append(report.GateErrors,
				fmt.Sprintf("manifest entry %s must set exactly one of expected_stage or expected_losses", e.Path))
		}
	}

	for _, info := range infos {
		result := FixtureResult{Fixture: info}
		negEntry, isNeg := negative[info.RelPath]
		lossEntry, isLoss := expectedLoss[info.RelPath]
		if isNeg && isLoss {
			result.Status = "failed"
			result.Error = "fixture declared as both negative and expected-loss in manifest"
			report.Results = append(report.Results, result)
			continue
		}
		runFixture(info, decodeErrors[info.RelPath], isNeg, negEntry, isLoss, lossEntry, opts, &result)
		report.Results = append(report.Results, result)
		for _, tag := range result.Tags {
			report.Coverage[tag] = append(report.Coverage[tag], info.RelPath)
		}
	}

	for _, tag := range RequiredCoverageTags {
		if len(report.Coverage[tag]) == 0 {
			report.GateErrors = append(report.GateErrors,
				fmt.Sprintf("required coverage scenario has no fixture result: %s", tag))
		}
	}
	return report, nil
}

func runFixture(info FixtureInfo, decodeErr error,
	isNeg bool, neg ManifestEntry, isLoss bool, loss ManifestEntry,
	opts Options, result *FixtureResult) {

	data, err := os.ReadFile(info.AbsPath)
	if err != nil {
		failGeneric(result, StageDecode, "read fixture: "+err.Error(), isNeg, neg)
		return
	}

	original, dErr := decodeAll(data)
	if dErr != nil {
		// A decode-stage negative fixture documented as an unknown opcode
		// provides the gate's independent "unknown opcode" coverage result.
		if isNeg && neg.ExpectedStage == StageDecode &&
			(strings.Contains(neg.ErrorContains, "opcode") ||
				strings.Contains(neg.Reason, "opcode") ||
				strings.Contains(dErr.Error(), "unknown type flag")) {
			result.Tags = append(result.Tags, "unknown-opcode")
		}
		finishNegative(result, StageDecode, dErr, isNeg, neg)
		return
	}
	result.Tags = tagForFixture(original, opts.Now)
	result.AOFStatus = "skipped"

	if isNeg {
		// negative fixture decoded fine: manifest mismatch
		result.Status = "failed"
		result.Error = fmt.Sprintf("manifest expected failure at stage %q but fixture decoded successfully", neg.ExpectedStage)
		return
	}

	encoded, eErr := reencode(original, info.Flavor, info.RDBVersion)
	if eErr != nil {
		finishNegative(result, StageEncode, eErr, false, neg)
		return
	}

	redecoded, rErr := decodeAll(encoded)
	if rErr != nil {
		finishNegative(result, StageRedeCode, rErr, false, neg)
		return
	}

	observedLosses, diffs := CompareObjects(original, redecoded, opts.Now)
	result.Losses = observedLosses
	result.Diffs = diffs

	aof := checkAOF(redecoded)
	result.AOFStatus = aof.status
	result.AOFIssue = aof.issue

	if aof.status == "checked" && aof.issue != "" {
		result.Status = "failed"
		result.Stage = StageAOF
		result.Error = aof.issue
		return
	}

	if len(diffs) > 0 {
		result.Status = "failed"
		result.Stage = StageSemantic
		var msgs []string
		for _, d := range diffs {
			msgs = append(msgs, d.Detail)
		}
		result.Error = "semantic mismatch: " + strings.Join(msgs, "; ")
		return
	}

	// Accept losses only when they exactly equal the manifest declaration.
	lossReasons := map[LossReason]string{}
	for _, l := range observedLosses {
		lossReasons[l.Reason] = l.Detail
	}
	declared := map[LossReason]struct{}{}
	if isLoss {
		for _, r := range loss.ExpectedLosses {
			declared[r] = struct{}{}
		}
	}
	var undeclared, stale []string
	for reason := range lossReasons {
		if _, ok := declared[reason]; !ok {
			undeclared = append(undeclared, string(reason))
		}
	}
	for reason := range declared {
		if _, ok := lossReasons[reason]; !ok {
			stale = append(stale, string(reason))
		}
	}
	sort.Strings(undeclared)
	sort.Strings(stale)
	if len(undeclared) > 0 || len(stale) > 0 {
		var problems []string
		if len(undeclared) > 0 {
			problems = append(problems, "undeclared losses: "+strings.Join(undeclared, ","))
		}
		if len(stale) > 0 {
			problems = append(problems, "declared but not observed: "+strings.Join(stale, ","))
		}
		if !isLoss {
			result.Status = "failed"
			result.Stage = StageSemantic
			result.Error = strings.Join(problems, "; ")
			return
		}
		result.Status = "failed"
		result.Error = "expected-loss manifest mismatch: " + strings.Join(problems, "; ")
		return
	}
	if len(observedLosses) > 0 {
		result.Status = "expected-loss"
		return
	}
	result.Status = "lossless"
}

// finishNegative handles fixtures whose observed failure stage is known: it
// passes only when a manifest entry exactly matches the stage (and, when
// declared, the error substring).
func finishNegative(result *FixtureResult, stage FailureStage, observed error, isNeg bool, neg ManifestEntry) {
	if !isNeg {
		result.Status = "failed"
		result.Stage = stage
		result.Error = observed.Error()
		return
	}
	if neg.ExpectedStage != stage {
		result.Status = "failed"
		result.Stage = stage
		result.Error = fmt.Sprintf("manifest expected stage %q but failed at %q: %v", neg.ExpectedStage, stage, observed)
		return
	}
	if neg.ErrorContains != "" && !strings.Contains(observed.Error(), neg.ErrorContains) {
		result.Status = "failed"
		result.Stage = stage
		result.Error = fmt.Sprintf("error %q does not contain expected substring %q", observed.Error(), neg.ErrorContains)
		return
	}
	result.Status = "negative-ok"
	result.Stage = stage
	result.Error = observed.Error()
}

// failGeneric is for failures before the file can even be read.
func failGeneric(result *FixtureResult, stage FailureStage, msg string, isNeg bool, neg ManifestEntry) {
	finishNegative(result, stage, fmt.Errorf("%s", msg), isNeg, neg)
}

func loadManifest(repoRoot string) (*Manifest, string, error) {
	path := filepath.Join(repoRoot, "cases", "verify_manifest.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{}, path, nil
		}
		return nil, path, fmt.Errorf("read manifest %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, path, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	seen := map[string]struct{}{}
	for _, e := range m.Entries {
		if _, dup := seen[e.Path]; dup {
			return nil, path, fmt.Errorf("duplicate manifest entry: %s", e.Path)
		}
		seen[e.Path] = struct{}{}
	}
	return &m, path, nil
}

func indexManifest(m *Manifest) (map[string]ManifestEntry, map[string]ManifestEntry) {
	neg := map[string]ManifestEntry{}
	loss := map[string]ManifestEntry{}
	for _, e := range m.Entries {
		if e.ExpectedStage != "" {
			neg[e.Path] = e
		} else {
			loss[e.Path] = e
		}
	}
	return neg, loss
}
