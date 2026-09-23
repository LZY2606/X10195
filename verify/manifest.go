package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// finding kinds
const (
	kindBlocker      = "blocker"       // pipeline cannot continue or hard failure at a stage
	kindError        = "error"         // unexpected semantic difference
	kindExpectedLoss = "expected-loss" // loss attributed to a known encoder limitation
)

// pipeline stages
const (
	stageCollect  = "collect"
	stageDecode   = "decode"
	stageReencode = "reencode"
	stageRedecode = "redecode"
	stageCompare  = "compare"
	stageAOF      = "aof"
)

// finding is a single structured verification result that prevents a fixture
// from being considered clean, unless it is acknowledged by the manifest.
type finding struct {
	Fixture string `json:"fixture"`
	Stage   string `json:"stage"`
	Kind    string `json:"kind"`
	Check   string `json:"check"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (f finding) key() manifestKey {
	return manifestKey{Fixture: f.Fixture, Stage: f.Stage, Kind: f.Kind, Code: f.Code}
}

func (f finding) String() string {
	return fmt.Sprintf("%s/%s/%s: %s", f.Stage, f.Kind, f.Code, f.Message)
}

// manifestKey identifies a finding class for exact manifest matching.
type manifestKey struct {
	Fixture string
	Stage   string
	Kind    string
	Code    string
}

// manifestEntry acknowledges one expected negative result of a fixture.
type manifestEntry struct {
	Fixture string `json:"fixture"`
	Stage   string `json:"stage"`
	Kind    string `json:"kind"`
	Code    string `json:"code"`
	Reason  string `json:"reason"`
}

func (e manifestEntry) key() manifestKey {
	return manifestKey{Fixture: e.Fixture, Stage: e.Stage, Kind: e.Kind, Code: e.Code}
}

// manifest is the explicit negative-fixture manifest.
type manifest struct {
	Version int             `json:"version"`
	Entries []manifestEntry `json:"negatives"`
}

func loadManifest(path string) (*manifest, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &manifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	m := &manifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("parse manifest %s failed: %v", path, err)
	}
	for i, e := range m.Entries {
		if e.Fixture == "" || e.Stage == "" || e.Kind == "" || e.Code == "" || e.Reason == "" {
			return nil, fmt.Errorf("manifest entry %d is incomplete: fixture/stage/kind/code/reason are all required", i)
		}
	}
	return m, nil
}

// matchFindings splits findings into unmatched ones and manifest entries that
// never matched any finding (stale entries). Matching is exact on
// (fixture, stage, kind, code).
func matchFindings(findings []finding, m *manifest) (unmatched []finding, stale []manifestEntry) {
	entryUsed := make([]bool, len(m.Entries))
	for _, f := range findings {
		matched := false
		for i, e := range m.Entries {
			if e.key() == f.key() {
				entryUsed[i] = true
				matched = true
			}
		}
		if !matched {
			unmatched = append(unmatched, f)
		}
	}
	for i, e := range m.Entries {
		if !entryUsed[i] {
			stale = append(stale, e)
		}
	}
	return unmatched, stale
}

func sortFindings(findings []finding) {
	sort.Slice(findings, func(i, j int) bool {
		a, b := findings[i], findings[j]
		if a.Fixture != b.Fixture {
			return a.Fixture < b.Fixture
		}
		if a.Stage != b.Stage {
			return a.Stage < b.Stage
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return a.Message < b.Message
	})
}
