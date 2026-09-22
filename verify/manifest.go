package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// stages at which a fixture may legitimately fail
var knownStages = map[string]bool{
	"decode":   true,
	"encode":   true,
	"redecode": true,
	"compare":  true,
	"aof":      true,
}

// negativeEntry declares a fixture that is expected to fail at a given stage.
type negativeEntry struct {
	Path   string `json:"path"`             // exact slash-separated path relative to repo root
	Reason string `json:"reason"`           // why this fixture cannot round-trip
	Stage  string `json:"stage"`            // expected failing stage: decode/encode/redecode/compare/aof
	Expect string `json:"expect,omitempty"` // optional substring the error message must contain
}

type negativeManifest struct {
	Fixtures []negativeEntry `json:"fixtures"`
}

// loadManifest reads the negative-fixture manifest. A missing file yields an
// empty manifest; a malformed file is an error.
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
		return nil, fmt.Errorf("parse manifest %s: %v", path, err)
	}
	seen := map[string]bool{}
	for i, e := range m.Fixtures {
		if e.Path == "" {
			return nil, fmt.Errorf("manifest entry %d: empty path", i)
		}
		if e.Reason == "" {
			return nil, fmt.Errorf("manifest entry %s: reason is required", e.Path)
		}
		if !knownStages[e.Stage] {
			return nil, fmt.Errorf("manifest entry %s: unknown stage %q", e.Path, e.Stage)
		}
		if seen[e.Path] {
			return nil, fmt.Errorf("manifest entry %s: duplicate path", e.Path)
		}
		seen[e.Path] = true
	}
	return m, nil
}

func (m *negativeManifest) lookup(relPath string) *negativeEntry {
	for i := range m.Fixtures {
		if m.Fixtures[i].Path == relPath {
			return &m.Fixtures[i]
		}
	}
	return nil
}
