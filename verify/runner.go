package verify

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// Result is the gate verdict for one fixture.
type Result struct {
	Fixture *Fixture
	// Status is "pass", "expected-loss", "negative-match" or "fail".
	Status string
	// Stage is the pipeline stage that failed, if any.
	Stage Stage
	// Error is the observed failure message, if any.
	Error string
	// Losses are the structured loss codes observed (accepted or unexpected).
	Losses []string
	// Features are the independent semantic feature tags of the fixture.
	Features []string
	// Types is the sorted "type/encoding" set observed on first decode.
	Types []string
	// ObjectCount is the number of data objects (excluding aux/dbsize metadata).
	ObjectCount int
	// AOFCommands summarizes command-name -> count from the AOF conversion.
	AOFCommands map[string]int
}

const (
	statusPass          = "pass"
	statusExpectedLoss  = "expected-loss"
	statusNegativeMatch = "negative-match"
	statusFail          = "fail"
)

// Report is the aggregate gate result.
type Report struct {
	Results []*Result
}

// Run executes the full verification gate and returns the structured report.
// Discovery is automatic; only manifest entries may excuse a failure/loss.
func Run(repoRoot string, w io.Writer) (*Report, error) {
	manifest, err := LoadManifest(repoRoot + "/verify/manifest.json")
	if err != nil {
		return nil, err
	}
	fixtures, err := DiscoverFixtures(repoRoot)
	if err != nil {
	return nil, err
	}
	fmt.Fprintf(w, "discovered %d .rdb fixture(s) under %s\n", len(fixtures), repoRoot)
	if len(fixtures) == 0 {
		return nil, fmt.Errorf("fixture collection is empty: zero .rdb files discovered under %s", repoRoot)
	}

	report := &Report{}
	for _, fixture := range fixtures {
		result := runOne(fixture, manifest)
		report.Results = append(report.Results, result)
		printResult(w, result)
	}

	if err := validateManifestCoverage(fixtures, manifest); err != nil {
		return report, err
	}
	return report, nil
}

// runOne runs the decode/re-encode/re-decode/compare/aof pipeline for a file.
func runOne(fixture *Fixture, manifest *Manifest) *Result {
	result := &Result{
		Fixture:     fixture,
		Status:      statusPass,
		AOFCommands: map[string]int{},
	}

	// Stage 1: decode.
	first, err := decodeFile(fixture.AbsPath)
	if err != nil {
		return finishNegativeOrFail(result, manifest, StageDecode, err)
	}
	result.ObjectCount = countDataObjects(first.objects)
	result.Types = observedTypes(first.objects)
	result.Features = detectFeatures(first, nowMillis())

	// Determine unavoidable structural losses before attempting re-encode.
	snap1 := canonicalize(first)
	expectedStatic := staticLosses(fixture, first, snap1)

	// Stage 2: re-encode.
	encoded, err := encodeRDB(first, fixture.Flavor == "valkey")
	if err != nil {
		// Function-library loss is a declared blocker, not a crash: stop here.
		if strings.Contains(err.Error(), LossFunctionLibrary) {
			result.Stage = StageEncode
			result.Error = err.Error()
			result.Losses = mergeLosses(expectedStatic, []string{LossFunctionLibrary})
			return evaluateLosses(result, manifest)
		}
		return finishNegativeOrFail(result, manifest, StageEncode, err)
	}

	// Stage 3: second decode of the re-encoded bytes.
	second, err := decodeRDB(bytesReader(encoded))
	if err != nil {
		return finishNegativeOrFail(result, manifest, StageRedeCode, err)
	}

	// Stage 4: semantic comparison.
	snap2 := canonicalize(second)
	diffs := compareSnapshots(snap1, snap2)
	if len(diffs) > 0 {
		err := fmt.Errorf("semantic diff:\n  - %s", strings.Join(diffs, "\n  - "))
		return finishNegativeOrFail(result, manifest, StageCompare, err)
	}

	// Stage 5: AOF conversion structure check.
	stats, aofDiffs := checkAOF(first)
	for k, v := range stats.commands {
		result.AOFCommands[k] = v
	}
	result.Losses = expectedStatic
	if stats.streamGroupMetadata {
		result.Losses = mergeLosses(result.Losses, []string{LossAOFStreamMetadata})
	}
	if len(aofDiffs) > 0 {
		// The only tolerated AOF "diff" is the explicitly modeled
		// stream-metadata loss, which becomes a structured loss code.
		unexpected := filterAOFErrors(aofDiffs)
		if len(unexpected) > 0 {
			err := fmt.Errorf("aof structure:\n  - %s", strings.Join(unexpected, "\n  - "))
			return finishNegativeOrFail(result, manifest, StageAOF, err)
		}
	}
	return evaluateLosses(result, manifest)
}

func finishNegativeOrFail(result *Result, manifest *Manifest, stage Stage, err error) *Result {
	result.Stage = stage
	result.Error = err.Error()
	if neg, ok := manifest.NegativeLookup(result.Fixture.RelPath); ok {
		if neg.ExpectedStage == stage && strings.Contains(result.Error, neg.ExpectedError) {
			result.Status = statusNegativeMatch
			return result
		}
		result.Status = statusFail
		result.Error = fmt.Sprintf("negative manifest mismatch: expected stage %q and error containing %q, got stage %q and error %q",
			neg.ExpectedStage, neg.ExpectedError, stage, result.Error)
		return result
	}
	result.Status = statusFail
	return result
}

// evaluateLosses decides pass / expected-loss / fail from the observed loss set.
func evaluateLosses(result *Result, manifest *Manifest) *Result {
	if len(result.Losses) == 0 {
		result.Status = statusPass
		return result
	}
	sort.Strings(result.Losses)
	declared, ok := manifest.ExpectedLossLookup(result.Fixture.RelPath)
	if !ok {
		result.Status = statusFail
		result.Error = fmt.Sprintf("unexpected semantic loss(es) not declared in manifest: %s", strings.Join(result.Losses, ", "))
		return result
	}
	if !sameStringSet(declared.Losses, result.Losses) {
		result.Status = statusFail
		result.Error = fmt.Sprintf("expected-loss manifest mismatch for %s: declared %v but observed %v",
			result.Fixture.RelPath, declared.Losses, result.Losses)
		return result
	}
	result.Status = statusExpectedLoss
	return result
}

func filterAOFErrors(diffs []string) []string {
	const tolerated = "aof: stream groups/consumers/PEL are not expressible by XADD conversion"
	var out []string
	for _, d := range diffs {
		if d != tolerated {
			out = append(out, d)
		}
	}
	return out
}

func mergeLosses(a, b []string) []string {
	set := map[string]bool{}
	for _, v := range a {
		set[v] = true
	}
	for _, v := range b {
		set[v] = true
	}
	return sortedKeys(set)
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ac := append([]string(nil), a...)
	bc := append([]string(nil), b...)
	sort.Strings(ac)
	sort.Strings(bc)
	for i := range ac {
		if ac[i] != bc[i] {
			return false
		}
	}
	return true
}
