package verify

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// Manifest is the explicit gate configuration. Nothing in this file is used to
// discover fixtures: discovery is always performed by walking the repository.
// The manifest only records deviations that cannot be silently ignored:
//   - negatives: fixtures that must fail, together with the exact pipeline
//     stage at which the failure is expected and the reason why.
//   - expectedLosses: fixtures that survive decode -> re-encode -> decode but
//     are known to undergo a semantically equivalent normalization (for
//     example RDB version or on-disk encoding downgrade).
type Manifest struct {
	Negatives      []NegativeEntry     `json:"negatives"`
	ExpectedLosses []ExpectedLossEntry `json:"expected_losses"`
}

// NegativeEntry describes a fixture expected to fail at a given pipeline stage.
type NegativeEntry struct {
	Path   string `json:"path"`
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
}

// ExpectedLossEntry describes an accepted, structured loss for one fixture.
type ExpectedLossEntry struct {
	Path   string   `json:"path"`
	Losses []string `json:"losses"`
	Reason string   `json:"reason"`
}

// Pipeline stages referenced by the manifest.
const (
	StageHeader   = "header"
	StageDecode   = "decode"
	StageReencode = "reencode"
	StageDecode2  = "decode2"
	StageSemantic = "semantic"
	StageAOF      = "aof"
)

// Structured loss kinds emitted by the gate.
const (
	LossRDBVersionNormalized  = "rdb-version-normalized"
	LossEncodingNormalized    = "encoding-normalized"
	LossRDBChecksumAltered    = "rdb-checksum-altered"
	LossAuxMetadataDropped    = "aux-metadata-dropped"
	LossFunctionLibraryDropped = "function-library-dropped"
	LossLRUMetadataDropped    = "lru-metadata-dropped"
)

func loadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	for i, n := range m.Negatives {
		if n.Path == "" || n.Stage == "" || n.Reason == "" {
			return nil, fmt.Errorf("manifest negatives[%d] must set path, stage and reason", i)
		}
		if !validStage(n.Stage) {
			return nil, fmt.Errorf("manifest negatives[%d] has invalid stage %q", i, n.Stage)
		}
	}
	for i, l := range m.ExpectedLosses {
		if l.Path == "" || l.Reason == "" || len(l.Losses) == 0 {
			return nil, fmt.Errorf("manifest expected_losses[%d] must set path, reason and losses", i)
		}
		for _, kind := range l.Losses {
			if !validLossKind(kind) {
				return nil, fmt.Errorf("manifest expected_losses[%d] has unknown loss kind %q", i, kind)
			}
		}
	}
	return &m, nil
}

func validStage(stage string) bool {
	switch stage {
	case StageHeader, StageDecode, StageReencode, StageDecode2, StageSemantic, StageAOF:
		return true
	}
	return false
}

func validLossKind(kind string) bool {
	switch kind {
	case LossRDBVersionNormalized, LossEncodingNormalized, LossRDBChecksumAltered,
		LossAuxMetadataDropped, LossFunctionLibraryDropped, LossLRUMetadataDropped:
		return true
	}
	return false
}

func (m *Manifest) negative(path string) (NegativeEntry, bool) {
	for _, n := range m.Negatives {
		if n.Path == path {
			return n, true
		}
	}
	return NegativeEntry{}, false
}

func (m *Manifest) expectedLoss(path string) (ExpectedLossEntry, bool) {
	for _, l := range m.ExpectedLosses {
		if l.Path == path {
			return l, true
		}
	}
	return ExpectedLossEntry{}, false
}

// manifestDiagnostics returns sorted manifest paths so stale entries can be
// detected against the discovered set.
func manifestPaths(m *Manifest) []string {
	seen := map[string]struct{}{}
	for _, n := range m.Negatives {
		seen[n.Path] = struct{}{}
	}
	for _, l := range m.ExpectedLosses {
		seen[l.Path] = struct{}{}
	}
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}
