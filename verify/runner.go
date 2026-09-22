package verify

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/hdt3213/rdb/model"
)

// Result kinds.
const (
	KindLossless     = "lossless"
	KindExpectedLoss = "expected-loss"
	KindBlocker      = "blocker"
	KindNegative     = "negative"
)

// FixtureResult is the structured gate result for one fixture.
type FixtureResult struct {
	Path        string   `json:"path"`
	RDBVersion  string   `json:"rdbVersion"`
	Types       []string `json:"types"`
	Encodings   []string `json:"encodings"`
	Traits      []string `json:"traits"`
	Kind        string   `json:"kind"`
	FailedStage string   `json:"failedStage,omitempty"`
	Error       string   `json:"error,omitempty"`
	Losses      []string `json:"losses,omitempty"`
	Diffs       []Diff   `json:"diffs,omitempty"`
	AOFSkipped  []string `json:"aofSkipped,omitempty"`
}

// Report is the machine-readable gate output.
type Report struct {
	FixtureCount int             `json:"fixtureCount"`
	Results      []FixtureResult `json:"results"`
	Blockers     []string        `json:"blockers"`
}

func runFixture(f Fixture, manifest *Manifest) FixtureResult {
	res := FixtureResult{Path: f.RelPath, Kind: KindLossless}

	raw, err := os.ReadFile(f.AbsPath)
	if err != nil {
		res.Kind = KindBlocker
		res.FailedStage = StageHeader
		res.Error = err.Error()
		return res
	}

	// stage: decode
	version, valkey, objects, derr := decodeFile(bytesReader(raw))
	res.RDBVersion = formatVersion(version, valkey)
	res.Types, res.Encodings = summarizeObjects(objects)
	res.Traits = detectTraits(version, valkey, objects)
	if derr != nil {
		res.FailedStage = StageDecode
		res.Error = derr.Error()
		classifyNegative(&res, manifest)
		return res
	}
	if neg, isNeg := manifest.negative(f.RelPath); isNeg {
		res.Kind = KindBlocker
		res.FailedStage = neg.Stage
		res.Error = fmt.Sprintf("manifest expected failure at stage %s but fixture decoded successfully", neg.Stage)
		return res
	}

	// stage: reencode
	encoded, rerr := reencode(objects, valkey)
	if rerr != nil {
		res.Kind = KindBlocker
		res.FailedStage = StageReencode
		res.Error = rerr.Error()
		return res
	}

	// stage: second decode
	version2, valkey2, objects2, d2err := decodeFile(bytesReader(encoded))
	if d2err != nil {
		res.Kind = KindBlocker
		res.FailedStage = StageDecode2
		res.Error = d2err.Error()
		return res
	}

	// stage: semantic compare
	snap1 := buildSnapshot(version, valkey, objects)
	snap2 := buildSnapshot(version2, valkey2, objects2)
	diffs := compareSnapshots(snap1, snap2)
	res.Diffs = diffs

	// detect structured (non-semantic) losses
	losses := detectLosses(snap1, snap2)
	res.Losses = losses

	if len(diffs) > 0 {
		res.Kind = KindBlocker
		res.FailedStage = StageSemantic
		msgs := make([]string, 0, len(diffs))
		for _, d := range diffs {
			msgs = append(msgs, d.Msg)
		}
		res.Error = "semantic mismatch: " + strings.Join(msgs, "; ")
		return res
	}

	// stage: AOF structural inspection
	if _, aofDiffs := inspectAOF(objects); len(aofDiffs) > 0 {
		res.Kind = KindBlocker
		res.FailedStage = StageAOF
		msgs := make([]string, 0, len(aofDiffs))
		for _, d := range aofDiffs {
			msgs = append(msgs, d.Msg)
		}
		res.Error = "aof structure mismatch: " + strings.Join(msgs, "; ")
		res.AOFSkipped = nil
		return res
	}
	if _, aofDiffs := inspectAOF(objects2); len(aofDiffs) > 0 {
		res.Kind = KindBlocker
		res.FailedStage = StageAOF
		msgs := make([]string, 0, len(aofDiffs))
		for _, d := range aofDiffs {
			msgs = append(msgs, d.Msg)
		}
		res.Error = "aof structure mismatch after reencode: " + strings.Join(msgs, "; ")
		return res
	}
	insp, _ := inspectAOF(objects)
	res.AOFSkipped = insp.Skipped

	if len(losses) > 0 {
		expected, ok := manifest.expectedLoss(f.RelPath)
		if !ok {
			res.Kind = KindBlocker
			res.FailedStage = StageSemantic
			res.Error = fmt.Sprintf("fixture has unacknowledged losses %v; add an expected_losses manifest entry with reason", losses)
			return res
		}
		if !sameStringSet(expected.Losses, losses) {
			res.Kind = KindBlocker
			res.FailedStage = StageSemantic
			res.Error = fmt.Sprintf("manifest losses %v do not match detected losses %v", expected.Losses, losses)
			return res
		}
		res.Kind = KindExpectedLoss
	}
	return res
}

func classifyNegative(res *FixtureResult, manifest *Manifest) {
	neg, ok := manifest.negative(res.Path)
	if !ok {
		res.Kind = KindBlocker
		return
	}
	if neg.Stage != res.FailedStage {
		res.Kind = KindBlocker
		res.Error = fmt.Sprintf("manifest expected failure at stage %s but failed at %s: %s", neg.Stage, res.FailedStage, res.Error)
		return
	}
	res.Kind = KindNegative
}

func detectLosses(a, b *Snapshot) []string {
	losses := map[string]struct{}{}
	if a.Version != b.Version || a.Valkey != b.Valkey {
		losses[LossRDBVersionNormalized] = struct{}{}
	}
	for _, oa := range a.Objects {
		for _, ob := range b.Objects {
			if oa.DB == ob.DB && oa.Key == ob.Key {
				if oa.Encoding != ob.Encoding {
					losses[LossEncodingNormalized] = struct{}{}
				}
			}
		}
	}
	out := make([]string, 0, len(losses))
	for k := range losses {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func formatVersion(version int, valkey bool) string {
	if valkey {
		return fmt.Sprintf("VALKEY%03d", version)
	}
	return fmt.Sprintf("REDIS%04d", version)
}

func summarizeObjects(objects []model.RedisObject) (types []string, encodings []string) {
	typeSet := map[string]struct{}{}
	encSet := map[string]struct{}{}
	for _, o := range objects {
		switch o.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
			continue
		}
		typeSet[o.GetType()] = struct{}{}
		if e := o.GetEncoding(); e != "" {
			encSet[e] = struct{}{}
		}
	}
	for k := range typeSet {
		types = append(types, k)
	}
	for k := range encSet {
		encodings = append(encodings, k)
	}
	sort.Strings(types)
	sort.Strings(encodings)
	return
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]struct{}{}
	for _, x := range a {
		m[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := m[x]; !ok {
			return false
		}
	}
	return true
}
