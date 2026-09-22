package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// negativeFixture declares a fixture that is expected to fail the gate.
// The failure must land in exactly the declared stage and match the error
// substring; otherwise the fixture is treated as a blocker (so a broken or a
// accidentally-fixed file cannot pass under a stale manifest entry).
type negativeFixture struct {
	Path     string `json:"path"`
	Reason   string `json:"reason"`
	Stage    string `json:"stage"`
	ErrorSub string `json:"errorContains"`
}

// negativeManifest is the on-disk shape of verify/negative-fixtures.json.
type negativeManifest struct {
	Fixtures []negativeFixture `json:"negativeFixtures"`
}

func loadManifest(path string) (*negativeManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &negativeManifest{}, nil
		}
		return nil, fmt.Errorf("read negative manifest %s: %w", path, err)
	}
	var m negativeManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse negative manifest %s: %w", path, err)
	}
	seen := make(map[string]struct{}, len(m.Fixtures))
	for i, f := range m.Fixtures {
		if f.Path == "" || f.Stage == "" || f.ErrorSub == "" || f.Reason == "" {
			return nil, fmt.Errorf("manifest entry #%d is missing path/reason/stage/errorContains", i+1)
		}
		if !validStage(f.Stage) {
			return nil, fmt.Errorf("manifest entry %s has unknown stage %q", f.Path, f.Stage)
		}
		if _, dup := seen[f.Path]; dup {
			return nil, fmt.Errorf("manifest contains duplicate path %q", f.Path)
		}
		seen[f.Path] = struct{}{}
	}
	sort.Slice(m.Fixtures, func(i, j int) bool { return m.Fixtures[i].Path < m.Fixtures[j].Path })
	return &m, nil
}

func validStage(stage string) bool {
	switch stage {
	case stageCollect, stageDecode, stageReEncode, stageSecondDecode, stageSemantic, stageAOF:
		return true
	}
	return false
}
