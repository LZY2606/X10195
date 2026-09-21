package verify

import (
	"encoding/json"
	"fmt"
	"os"
)

// stages of the verification pipeline. A negative fixture declares the
// stage at which it is expected to fail.
const (
	StageDecode   = "decode"
	StageEncode   = "encode"
	StageRedecode = "redecode"
	StageCompare  = "compare"
	StageAOF      = "aof"
)

var knownStages = map[string]bool{
	StageDecode:   true,
	StageEncode:   true,
	StageRedecode: true,
	StageCompare:  true,
	StageAOF:      true,
}

// manifestEntry describes one fixture that is expected to fail.
type manifestEntry struct {
	// Path is the fixture path relative to the repository root.
	Path string `json:"path"`
	// Reason documents why this fixture cannot pass.
	Reason string `json:"reason"`
	// ExpectStage is the pipeline stage that must fail:
	// decode, encode, redecode, compare or aof.
	ExpectStage string `json:"expectStage"`
	// ExpectError is an optional substring of the expected error.
	ExpectError string `json:"expectError,omitempty"`
}

// manifest is the explicit negative-fixture list. Fixtures not listed
// here must pass every stage.
type manifest struct {
	NegativeFixtures []manifestEntry `json:"negativeFixtures"`
}

func loadManifest(path string) (*manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s failed: %v", path, err)
	}
	m := &manifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("parse manifest %s failed: %v", path, err)
	}
	seen := make(map[string]bool)
	for i, e := range m.NegativeFixtures {
		if e.Path == "" {
			return nil, fmt.Errorf("manifest entry %d: path is required", i)
		}
		if e.Reason == "" {
			return nil, fmt.Errorf("manifest entry %s: reason is required", e.Path)
		}
		if !knownStages[e.ExpectStage] {
			return nil, fmt.Errorf("manifest entry %s: expectStage %q is not a known stage", e.Path, e.ExpectStage)
		}
		if seen[e.Path] {
			return nil, fmt.Errorf("manifest entry %s: duplicate path", e.Path)
		}
		seen[e.Path] = true
	}
	return m, nil
}
