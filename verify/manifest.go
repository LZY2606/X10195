package verify

// This file defines the explicit negative-fixture and expected-loss manifest.
//
// Automatic discovery (see collector.go) is the only mechanism that decides
// which .rdb files participate in the gate. There is intentionally no
// allow-list of file names. The only thing a fixture may opt out of via this
// manifest is the default "must decode and survive a lossless round-trip"
// expectation, and even that requires an explicit, reviewed reason plus the
// precise stage at which the failure is expected.

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// FailureStage identifies a phase of the verify pipeline.
type FailureStage string

const (
	// StageDecode is the first parse of the on-disk fixture.
	StageDecode FailureStage = "decode"
	// StageEncode is re-serializing the decoded model back to rdb bytes.
	StageEncode FailureStage = "encode"
	// StageRedeocde is parsing the freshly re-encoded bytes.
	StageRedecode FailureStage = "redecode"
	// StageSemantic is the normalized semantic comparison.
	StageSemantic FailureStage = "semantic"
	// StageAOF is conversion of the decoded model to an AOF/RESP stream.
	StageAOF FailureStage = "aof"
)

// NegativeFixture declares a fixture that the pipeline is expected to reject
// at exactly one stage. A negative fixture only passes when:
//
//   - the discovered relative path matches Path exactly,
//   - the pipeline really fails,
//   - the failure happens at Stage,
//   - the error message contains ErrorContains (when non-empty).
//
// Success of any other stage, or failure at a different stage, fails the gate.
type NegativeFixture struct {
	// Path is the repository-relative path of the fixture, e.g. "cases/bad.rdb".
	Path string `json:"path"`
	// Stage is the pipeline stage at which the error is expected.
	Stage FailureStage `json:"stage"`
	// ErrorContains optionally pins down part of the expected error text.
	ErrorContains string `json:"errorContains,omitempty"`
	// Reason is a mandatory human-readable justification (must be non-empty).
	Reason string `json:"reason"`
}

// ExpectedLoss declares semantic information that the encoder is known to drop
// for a particular fixture, expressed as concrete difference codes (see the
// DIFF_* constants in semantic.go). Unlike negative fixtures, the pipeline
// must still decode, re-encode and re-decode the fixture; the comparator only
// tolerates exactly the declared differences. Any undeclared difference still
// fails the gate, so "second decode does not error" can never masquerade as a
// passing round-trip.
type ExpectedLoss struct {
	// Path is the repository-relative path of the fixture.
	Path string `json:"path"`
	// Reason is a mandatory human-readable justification (must be non-empty).
	Reason string `json:"reason"`
	// Diffs lists the exact semantic-difference codes that are tolerated.
	Diffs []string `json:"diffs"`
}

// Manifest is the on-disk layout of negative-fixtures.json.
type Manifest struct {
	// NegativeFixtures lists files expected to fail at a precise stage.
	NegativeFixtures []NegativeFixture `json:"negativeFixtures"`
	// ExpectedLosses lists accepted, specifically-scoped encoder losses.
	ExpectedLosses []ExpectedLoss `json:"expectedLosses"`
}

// LoadManifest reads and validates the manifest file. A missing manifest is
// legal and yields an empty manifest (all discovered fixtures must then be
// fully lossless).
func LoadManifest(path string) (*Manifest, error) {
	m := &Manifest{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	seen := make(map[string]bool)
	for i, nf := range m.NegativeFixtures {
		if nf.Path == "" {
			return nil, fmt.Errorf("manifest negativeFixtures[%d] has empty path", i)
		}
		if seen[nf.Path] {
			return nil, fmt.Errorf("manifest negativeFixtures contains duplicate path %q", nf.Path)
		}
		seen[nf.Path] = true
		switch nf.Stage {
		case StageDecode, StageEncode, StageRedecode, StageSemantic, StageAOF:
		default:
			return nil, fmt.Errorf("manifest negativeFixtures[%d] (%s) has invalid stage %q", i, nf.Path, nf.Stage)
		}
		if nf.Reason == "" {
			return nil, fmt.Errorf("manifest negativeFixtures[%d] (%s) must state a reason", i, nf.Path)
		}
	}
	seenLoss := make(map[string]bool)
	for i, el := range m.ExpectedLosses {
		if el.Path == "" {
			return nil, fmt.Errorf("manifest expectedLosses[%d] has empty path", i)
		}
		if seenLoss[el.Path] {
			return nil, fmt.Errorf("manifest expectedLosses contains duplicate path %q", el.Path)
		}
		seenLoss[el.Path] = true
		if el.Reason == "" {
			return nil, fmt.Errorf("manifest expectedLosses[%d] (%s) must state a reason", i, el.Path)
		}
		if len(el.Diffs) == 0 {
			return nil, fmt.Errorf("manifest expectedLosses[%d] (%s) must list at least one diff code", i, el.Path)
		}
	}
	return m, nil
}

// Negative returns the negative entry for a repository-relative path, or nil.
func (m *Manifest) Negative(relPath string) *NegativeFixture {
	for i := range m.NegativeFixtures {
		if m.NegativeFixtures[i].Path == relPath {
			return &m.NegativeFixtures[i]
		}
	}
	return nil
}

// Loss returns the expected-loss entry for a repository-relative path, or nil.
func (m *Manifest) Loss(relPath string) *ExpectedLoss {
	for i := range m.ExpectedLosses {
		if m.ExpectedLosses[i].Path == relPath {
			return &m.ExpectedLosses[i]
		}
	}
	return nil
}

// sortedKeys returns keys of m in sorted order (test helper).
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
