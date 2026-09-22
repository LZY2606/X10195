package verify

import (
	"encoding/json"
	"fmt"
	"os"
)

// Stage identifies a phase of the decode/re-encode pipeline.
type Stage string

const (
	// StageDecode is the first decode of the on-disk fixture.
	StageDecode Stage = "decode"
	// StageEncode is the re-encoding of the decoded object set.
	StageEncode Stage = "encode"
	// StageRedeCode is the second decode, performed on the re-encoded bytes.
	StageRedeCode Stage = "redecode"
	// StageCompare is the semantic comparison between the two decodes.
	StageCompare Stage = "compare"
	// StageAOF is the AOF/RESP conversion structure check.
	StageAOF Stage = "aof"
)

// Well-known loss/blocker codes. They describe semantic information that
// the current encoder/AOF converter cannot represent losslessly.
const (
	// LossEncodingVersion: the encoder always writes REDIS0011/VALKEY080,
	// so fixtures produced by other RDB versions lose their version field.
	LossEncodingVersion = "encoding-version"
	// LossFunctionLibrary: RDB_OPCODE_FUNCTION payloads cannot be re-encoded.
	LossFunctionLibrary = "function-library"
	// LossLRU: per-object LRU idle time (opcode 248) is not re-encoded.
	LossLRU = "lru-idle"
	// LossLFU: per-object LFU frequency (opcode 249) is not re-encoded.
	LossLFU = "lfu-freq"
	// LossAOFStreamMetadata: the AOF converter emits XADD only and cannot
	// express stream consumer groups, consumers or PEL ownership.
	LossAOFStreamMetadata = "aof-stream-metadata"
)

// NegativeFixture declares a fixture that is expected to fail the pipeline.
type NegativeFixture struct {
	// Path is the fixture path relative to the repository root.
	Path string `json:"path"`
	// Reason is a mandatory, human-readable explanation of why the file is invalid.
	Reason string `json:"reason"`
	// ExpectedStage is the pipeline stage at which the failure must occur.
	ExpectedStage Stage `json:"expected_stage"`
	// ExpectedError is a substring that must be present in the observed error.
	ExpectedError string `json:"expected_error"`
}

// ExpectedLoss declares a positive fixture whose round-trip is known to lose
// specific, explicitly enumerated semantic attributes.
type ExpectedLoss struct {
	// Path is the fixture path relative to the repository root.
	Path string `json:"path"`
	// Reason is a mandatory justification for accepting the loss.
	Reason string `json:"reason"`
	// Losses is the exact set of loss codes observed for the fixture.
	Losses []string `json:"losses"`
}

// Manifest is the explicit negative-fixture / expected-loss registry.
type Manifest struct {
	// Negative lists fixtures whose processing must fail at a precise stage.
	Negative []NegativeFixture `json:"negative"`
	// ExpectedLoss lists positive fixtures with a fully enumerated loss set.
	ExpectedLoss []ExpectedLoss `json:"expected_loss"`
}

// LoadManifest reads and validates the manifest file. A missing file is
// reported as an error because the gate relies on the registry to
// distinguish intentional failures from regressions.
func LoadManifest(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	for i, n := range m.Negative {
		if n.Path == "" {
			return nil, fmt.Errorf("manifest negative[%d]: path is empty", i)
		}
		if n.Reason == "" {
			return nil, fmt.Errorf("manifest negative[%d] (%s): reason is required", i, n.Path)
		}
		switch n.ExpectedStage {
		case StageDecode, StageEncode, StageRedeCode, StageCompare, StageAOF:
		default:
			return nil, fmt.Errorf("manifest negative[%d] (%s): unsupported expected_stage %q", i, n.Path, n.ExpectedStage)
		}
		if n.ExpectedError == "" {
			return nil, fmt.Errorf("manifest negative[%d] (%s): expected_error is required", i, n.Path)
		}
	}
	for i, l := range m.ExpectedLoss {
		if l.Path == "" {
			return nil, fmt.Errorf("manifest expected_loss[%d]: path is empty", i)
		}
		if l.Reason == "" {
			return nil, fmt.Errorf("manifest expected_loss[%d] (%s): reason is required", i, l.Path)
		}
		if len(l.Losses) == 0 {
			return nil, fmt.Errorf("manifest expected_loss[%d] (%s): losses must not be empty", i, l.Path)
		}
		seen := map[string]bool{}
		for _, code := range l.Losses {
			if code == "" {
				return nil, fmt.Errorf("manifest expected_loss[%d] (%s): empty loss code", i, l.Path)
			}
			if seen[code] {
				return nil, fmt.Errorf("manifest expected_loss[%d] (%s): duplicated loss code %q", i, l.Path, code)
			}
			seen[code] = true
		}
	}
	return &m, nil
}

// NegativeLookup returns the negative entry for the given relative path, if any.
func (m *Manifest) NegativeLookup(rel string) (NegativeFixture, bool) {
	for _, n := range m.Negative {
		if n.Path == rel {
			return n, true
		}
	}
	return NegativeFixture{}, false
}

// ExpectedLossLookup returns the expected-loss entry for the given relative path, if any.
func (m *Manifest) ExpectedLossLookup(rel string) (ExpectedLoss, bool) {
	for _, l := range m.ExpectedLoss {
		if l.Path == rel {
			return l, true
		}
	}
	return ExpectedLoss{}, false
}
