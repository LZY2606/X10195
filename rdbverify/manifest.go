package rdbverify

import (
	"encoding/json"
	"fmt"
	"os"
)

// Manifest is the explicit, reviewable list of fixtures that are expected
// to fail or lose semantics, with a mandatory human-readable reason.
type Manifest struct {
	// Negatives are fixtures expected to fail at a specific stage.
	Negatives []NegativeEntry `json:"negatives"`
	// ExpectedLosses are fixtures that decode and re-decode but whose
	// roundtrip is known to lose a specific, enumerated set of semantics.
	ExpectedLosses []ExpectedLossEntry `json:"expectedLosses"`
}

// NegativeEntry declares one expected failure.
type NegativeEntry struct {
	// Path is the repo-relative fixture path.
	Path string `json:"path"`
	// Stage is the expected failing stage: header/decode/encode/second-decode/semantic.
	Stage Stage `json:"stage"`
	// ErrorContains must be a substring of the observed error.
	ErrorContains string `json:"errorContains"`
	// Reason is mandatory and explains why this failure is legitimate.
	Reason string `json:"reason"`
}

// ExpectedLossEntry declares a fixture with known semantic losses.
type ExpectedLossEntry struct {
	Path      string     `json:"path"`
	Reason    string     `json:"reason"`
	LossCodes []DiffCode `json:"lossCodes"`
	// AOFLossCode, when set, marks the expected AOF structural-loss warning
	// code (e.g. "aof-lossy-construct"). Empty means AOF must be exact.
	AOFLossCode string `json:"aofLossCode,omitempty"`
}

// LoadManifest parses the JSON manifest; a missing file means an empty one.
func LoadManifest(path string) (*Manifest, error) {
	m := &Manifest{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manifest) validate() error {
	seen := map[string]struct{}{}
	for _, n := range m.Negatives {
		if n.Path == "" {
			return fmt.Errorf("negative entry without path")
		}
		if n.Reason == "" {
			return fmt.Errorf("negative %s requires a reason", n.Path)
		}
		switch n.Stage {
		case StageHeader, StageDecode, StageEncode, StageSecondDecode, StageSemantic:
		default:
			return fmt.Errorf("negative %s has invalid stage %q", n.Path, n.Stage)
		}
		if n.ErrorContains == "" {
			return fmt.Errorf("negative %s requires errorContains", n.Path)
		}
		if _, dup := seen[n.Path]; dup {
			return fmt.Errorf("duplicate manifest entry for %s", n.Path)
		}
		seen[n.Path] = struct{}{}
	}
	for _, e := range m.ExpectedLosses {
		if e.Path == "" {
			return fmt.Errorf("expected-loss entry without path")
		}
		if e.Reason == "" {
			return fmt.Errorf("expected-loss %s requires a reason", e.Path)
		}
		if len(e.LossCodes) == 0 {
			return fmt.Errorf("expected-loss %s requires at least one loss code", e.Path)
		}
		if _, dup := seen[e.Path]; dup {
			return fmt.Errorf("duplicate manifest entry for %s", e.Path)
		}
		seen[e.Path] = struct{}{}
	}
	return nil
}

// NegativeFor returns the negative entry for path, if any.
func (m *Manifest) NegativeFor(path string) *NegativeEntry {
	for i := range m.Negatives {
		if m.Negatives[i].Path == path {
			return &m.Negatives[i]
		}
	}
	return nil
}

// LossFor returns the expected-loss entry for path, if any.
func (m *Manifest) LossFor(path string) *ExpectedLossEntry {
	for i := range m.ExpectedLosses {
		if m.ExpectedLosses[i].Path == path {
			return &m.ExpectedLosses[i]
		}
	}
	return nil
}
