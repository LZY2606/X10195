package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// manifestEntry declares a fixture that is expected to fail at a specific
// pipeline stage, together with the reason why.
type manifestEntry struct {
	Path        string `json:"path"`
	Reason      string `json:"reason"`
	ExpectStage string `json:"expect_stage"`
}

// loadManifest reads the negative-fixture manifest. A missing manifest file
// means no negative fixtures are declared.
func loadManifest(path string) (map[string]manifestEntry, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]manifestEntry{}, nil
	}
	if err != nil {
		return nil, err
	}
	var entries []manifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %v", path, err)
	}
	validStages := make(map[string]bool)
	for _, stage := range knownStages {
		validStages[stage] = true
	}
	out := make(map[string]manifestEntry)
	for _, entry := range entries {
		if entry.Path == "" || entry.Reason == "" {
			return nil, fmt.Errorf("manifest entry requires non-empty path and reason: %+v", entry)
		}
		if !validStages[entry.ExpectStage] {
			return nil, fmt.Errorf("manifest entry %s has invalid expect_stage %q (valid: %v)",
				entry.Path, entry.ExpectStage, knownStages)
		}
		if _, dup := out[entry.Path]; dup {
			return nil, fmt.Errorf("duplicate manifest entry for %s", entry.Path)
		}
		out[entry.Path] = entry
	}
	return out, nil
}
