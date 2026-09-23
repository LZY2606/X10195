package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Failure stages in the per-fixture pipeline, in execution order.
const (
	stageDecode   = "decode"
	stageAOF      = "aof"
	stageEncode   = "encode"
	stageRedecode = "redecode"
	stageCompare  = "compare"
)

var validStages = map[string]bool{
	stageDecode:   true,
	stageAOF:      true,
	stageEncode:   true,
	stageRedecode: true,
	stageCompare:  true,
}

// negativeEntry declares one fixture that is expected to fail at an exact
// pipeline stage, together with the reason why.
type negativeEntry struct {
	Path   string `json:"path"`
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
}

// negativeManifest is the on-disk explicit negative-fixture manifest.
type negativeManifest struct {
	NegativeFixtures []negativeEntry `json:"negativeFixtures"`
}

// loadManifest reads the manifest; a missing file means an empty manifest.
func loadManifest(path string) (*negativeManifest, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &negativeManifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	m := &negativeManifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("parse manifest %s failed: %v", path, err)
	}
	seen := make(map[string]bool)
	for i, e := range m.NegativeFixtures {
		if e.Path == "" {
			return nil, fmt.Errorf("manifest entry %d: path is required", i)
		}
		if !validStages[e.Stage] {
			return nil, fmt.Errorf("manifest entry %s: invalid stage %q", e.Path, e.Stage)
		}
		if e.Reason == "" {
			return nil, fmt.Errorf("manifest entry %s: reason is required", e.Path)
		}
		if seen[e.Path] {
			return nil, fmt.Errorf("manifest entry %s: duplicate path", e.Path)
		}
		seen[e.Path] = true
	}
	return m, nil
}

// find returns the entry exactly matching path, or nil.
func (m *negativeManifest) find(path string) *negativeEntry {
	for i := range m.NegativeFixtures {
		if m.NegativeFixtures[i].Path == path {
			return &m.NegativeFixtures[i]
		}
	}
	return nil
}
