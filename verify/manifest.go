package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// manifestPath is the explicit negative-fixture manifest. Every entry must
// name an existing fixture, the stage that is expected to fail and the
// reason. An entry only matches when the fixture fails at exactly that
// stage (and, when given, with a matching error substring).
const manifestPath = "verify/negative_fixtures.json"

type manifestEntry struct {
	Path          string `json:"path"`
	Stage         string `json:"stage"`
	Reason        string `json:"reason"`
	ErrorContains string `json:"error_contains,omitempty"`
}

type negativeManifest struct {
	entries []manifestEntry
	byPath  map[string]manifestEntry
}

func loadManifest(root string) (*negativeManifest, error) {
	m := &negativeManifest{byPath: map[string]manifestEntry{}}
	path := filepath.Join(root, filepath.FromSlash(manifestPath))
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	var doc struct {
		Fixtures []manifestEntry `json:"fixtures"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse %s failed: %v", manifestPath, err)
	}
	validStage := map[string]bool{}
	for _, s := range knownStages {
		validStage[s] = true
	}
	for _, entry := range doc.Fixtures {
		if entry.Path == "" || entry.Reason == "" {
			return nil, fmt.Errorf("manifest entry in %s misses path or reason", manifestPath)
		}
		if !validStage[entry.Stage] {
			return nil, fmt.Errorf("manifest entry %s has invalid stage %q", entry.Path, entry.Stage)
		}
		if _, dup := m.byPath[entry.Path]; dup {
			return nil, fmt.Errorf("duplicate manifest entry for %s", entry.Path)
		}
		m.entries = append(m.entries, entry)
		m.byPath[entry.Path] = entry
	}
	return m, nil
}
