package rdbverify

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Verification stages, in execution order. Expected-failure entries reference
// these constants so that an error occurring in a different stage fails the
// gate instead of being silently accepted.
const (
	// StageHeader reads the 9-byte RDB magic/version header.
	StageHeader = "header"
	// StageDecode is the first full parse of the fixture.
	StageDecode = "decode"
	// StageReencode serialises decoded objects back into RDB bytes.
	StageReencode = "reencode"
	// StageRedecode parses the re-encoded bytes.
	StageRedecode = "redecode"
	// StageSemantic compares first and second decodes.
	StageSemantic = "semantic"
	// StageAof converts decoded objects to an AOF command stream.
	StageAof = "aof"
)

// allStages lists stages in execution order, used for manifest validation.
var allStages = map[string]struct{}{
	StageHeader:    {},
	StageDecode:    {},
	StageReencode:  {},
	StageRedecode:  {},
	StageSemantic:  {},
	StageAof:       {},
}

// NegativeEntry is one explicit negative-fixture expectation.
//
// Every entry must state why the fixture is expected to fail (Reason) and
// during which stage (ExpectStage). For semantic-stage expectations LossCode
// must name the structured expected-loss category.
type NegativeEntry struct {
	// Path is the repository-relative path of the fixture.
	Path string `json:"path"`
	// ExpectStage is the stage where the failure is expected.
	ExpectStage string `json:"expectStage"`
	// Reason is a mandatory human-readable justification.
	Reason string `json:"reason"`
	// LossCode is required when ExpectStage == semantic; it must match the
	// structured expected-loss code produced by the comparator.
	LossCode string `json:"lossCode,omitempty"`
}

// Manifest is the on-disk negative-fixture file.
type Manifest struct {
	Entries []NegativeEntry `json:"negativeFixtures"`
}

// index is the normalised lookup built from a manifest.
type manifestIndex struct {
	byPath map[string]*NegativeEntry
}

// DefaultManifestPath returns the canonical manifest location under root.
func DefaultManifestPath(root string) string {
	return filepath.Join(root, "verify", "negative-fixtures.json")
}

// LoadManifest reads and validates the manifest file.
//
// A missing file is valid (it means "no negative fixtures"). Duplicate paths,
// unknown stages, empty reasons and semantic entries without a loss code are
// rejected so that a sloppy manifest cannot widen the gate.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Manifest{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read negative manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse negative manifest %s: %w", path, err)
	}
	seen := make(map[string]struct{}, len(m.Entries))
	for i := range m.Entries {
		e := &m.Entries[i]
		if e.Path == "" {
			return nil, fmt.Errorf("negative manifest entry #%d: path is empty", i)
		}
		if filepath.IsAbs(e.Path) {
			return nil, fmt.Errorf("negative manifest entry %s: path must be repository-relative", e.Path)
		}
		e.Path = filepath.ToSlash(e.Path)
		if e.Reason == "" {
			return nil, fmt.Errorf("negative manifest entry %s: reason is required", e.Path)
		}
		if _, ok := allStages[e.ExpectStage]; !ok {
			return nil, fmt.Errorf("negative manifest entry %s: unknown expectStage %q", e.Path, e.ExpectStage)
		}
		if e.ExpectStage == StageSemantic && e.LossCode == "" {
			return nil, fmt.Errorf("negative manifest entry %s: semantic expectation requires lossCode", e.Path)
		}
		if _, dup := seen[e.Path]; dup {
			return nil, fmt.Errorf("negative manifest entry %s: duplicated", e.Path)
		}
		seen[e.Path] = struct{}{}
	}
	sort.Slice(m.Entries, func(i, j int) bool { return m.Entries[i].Path < m.Entries[j].Path })
	return &m, nil
}

// index builds a path lookup and verifies that every entry references a
// collected fixture.
func (m *Manifest) index(known map[string]*Fixture) (*manifestIndex, error) {
	idx := &manifestIndex{byPath: make(map[string]*NegativeEntry, len(m.Entries))}
	for i := range m.Entries {
		e := &m.Entries[i]
		if _, ok := known[e.Path]; !ok {
			return nil, fmt.Errorf("negative manifest entry %s: no such collected .rdb fixture", e.Path)
		}
		idx.byPath[e.Path] = e
	}
	return idx, nil
}

// lookup returns the expectation for path, or nil.
func (idx *manifestIndex) lookup(path string) *NegativeEntry {
	return idx.byPath[path]
}
