// Package verify implements the repository wide RDB fixture gate.
// It discovers every .rdb fixture in the repository, decodes it,
// re-encodes it, decodes the re-encoded file again and compares the
// two decoding results semantically. Fixtures whose objects can be
// converted to AOF also get their AOF output structurally validated.
// The whole process never talks to a Redis server.
package verify

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// Stage names used by results and by the negative fixture manifest.
const (
	StageDecode   = "decode"
	StageEncode   = "encode"
	StageRedecode = "redecode"
	StageCompare  = "compare"
	StageAOF      = "aof"
)

// Stages lists all valid stage names in pipeline order.
var Stages = []string{StageDecode, StageEncode, StageRedecode, StageCompare, StageAOF}

// Loss kinds that the gate knows how to classify. Anything that is not
// one of these known, unavoidable losses is treated as a blocker.
const (
	LossFunctions  = "functions"   // encoder cannot emit function libraries yet
	LossEncoding   = "encoding"    // compact encoding rewritten to another encoding
	LossRDBVersion = "rdb-version" // header version normalised to the encoder version
	LossLRU        = "lru"         // LRU idle time metadata cannot be re-encoded
	LossLFU        = "lfu"         // LFU frequency metadata cannot be re-encoded
)

// Loss describes a single known, unavoidable information loss caused by
// re-encoding. Losses are reported in a structured way but do not fail
// the gate; any other difference is a blocker.
type Loss struct {
	Kind   string `json:"kind"`
	Key    string `json:"key,omitempty"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	Detail string `json:"detail,omitempty"`
}

func (l Loss) String() string {
	var sb strings.Builder
	sb.WriteString(l.Kind)
	if l.Key != "" {
		fmt.Fprintf(&sb, " key=%q", l.Key)
	}
	if l.From != "" || l.To != "" {
		fmt.Fprintf(&sb, " %s->%s", l.From, l.To)
	}
	if l.Detail != "" {
		sb.WriteString(" " + l.Detail)
	}
	return sb.String()
}

// FixtureResult is the outcome of running the whole pipeline on one fixture.
type FixtureResult struct {
	Path        string   // slash separated path relative to the repository root
	Magic       string   // REDIS or VALKEY
	Version     int      // rdb version read from the header
	Types       []string // sorted distinct redis object types found
	ObjectCount int
	Categories  []string
	Losses      []Loss
	FailedStage string // empty when the fixture passed every stage
	Err         error
	Expected    bool // true when the failure exactly matches a manifest entry
}

// Category names that always get an independent result section.
const (
	CatListpack      = "listpack"
	CatStreamV1      = "stream-v1"
	CatStreamV2      = "stream-v2"
	CatStreamV3      = "stream-v3"
	CatHFE           = "hfe"
	CatEmptySet      = "empty-set"
	CatExpiredKeys   = "expired-keys"
	CatUnknownOpcode = "unknown-opcode"
)

// Categories lists every category that gets an independent result section,
// in report order.
var Categories = []string{
	CatListpack, CatStreamV1, CatStreamV2, CatStreamV3,
	CatHFE, CatEmptySet, CatExpiredKeys, CatUnknownOpcode,
}

// Report is the aggregate result of a gate run.
type Report struct {
	Fixtures  []*FixtureResult
	Failures  []string // human readable gate failures
	now       time.Time
	manifest  *Manifest
	manifestP string
}

// Passed reports whether the gate passed.
func (r *Report) Passed() bool {
	return len(r.Failures) == 0
}

// Options controls a gate run.
type Options struct {
	Root         string    // repository root
	TmpDir       string    // directory for temporary artifacts, cleaned by caller
	ManifestPath string    // path of the negative fixture manifest
	Now          time.Time // reference time for expired key detection
	Out          io.Writer // log output
}

// Run executes the whole gate and returns the report.
func Run(opts Options) (*Report, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	if opts.Out == nil {
		opts.Out = io.Discard
	}
	report := &Report{now: opts.Now}

	manifest, err := LoadManifest(opts.ManifestPath)
	if err != nil {
		return nil, err
	}
	report.manifest = manifest
	report.manifestP = opts.ManifestPath

	fixtures, err := Collect(opts.Root)
	if err != nil {
		return nil, err
	}
	if len(fixtures) == 0 {
		report.Failures = append(report.Failures,
			"no .rdb fixtures collected from "+opts.Root)
		return report, nil
	}
	fmt.Fprintf(opts.Out, "collected %d fixture(s) under %s\n", len(fixtures), opts.Root)
	for i, f := range fixtures {
		fmt.Fprintf(opts.Out, "  [%02d/%02d] %s rdb=%s%04d types=%s objects=%d\n",
			i+1, len(fixtures), f.Path, f.Magic, f.Version,
			strings.Join(f.Types, ","), f.ObjectCount)
	}

	for _, f := range fixtures {
		res := runFixture(opts, f)
		report.Fixtures = append(report.Fixtures, res)
	}
	report.checkManifest()
	report.printDetail(opts.Out)
	return report, nil
}

func (r *Report) checkManifest() {
	byPath := make(map[string]*FixtureResult)
	for _, res := range r.Fixtures {
		byPath[res.Path] = res
	}
	matched := make(map[int]bool)
	for _, res := range r.Fixtures {
		if res.FailedStage == "" {
			continue
		}
		entry := r.manifest.Find(res.Path)
		if entry != nil && entry.Stage == res.FailedStage {
			res.Expected = true
			matched[entryIndex(r.manifest, entry)] = true
			continue
		}
		if entry != nil {
			r.Failures = append(r.Failures, fmt.Sprintf(
				"%s: failed at stage %q but manifest expects stage %q",
				res.Path, res.FailedStage, entry.Stage))
		} else {
			r.Failures = append(r.Failures, fmt.Sprintf(
				"%s: unexpected failure at stage %q: %v",
				res.Path, res.FailedStage, res.Err))
		}
	}
	for i, entry := range r.manifest.Entries {
		if matched[i] {
			continue
		}
		res, ok := byPath[entry.Path]
		if !ok {
			r.Failures = append(r.Failures, fmt.Sprintf(
				"manifest entry %s (stage %s) matches no collected fixture",
				entry.Path, entry.Stage))
			continue
		}
		if res.FailedStage == "" {
			r.Failures = append(r.Failures, fmt.Sprintf(
				"%s: manifest expects failure at stage %q but the fixture passed",
				entry.Path, entry.Stage))
		}
	}
}

func entryIndex(m *Manifest, target *ManifestEntry) int {
	for i := range m.Entries {
		if &m.Entries[i] == target {
			return i
		}
	}
	return -1
}

func (r *Report) printDetail(out io.Writer) {
	fmt.Fprintln(out, "---- fixture results ----")
	for _, res := range r.Fixtures {
		status := "PASS"
		switch {
		case res.FailedStage != "" && res.Expected:
			status = fmt.Sprintf("EXPECTED-FAILURE stage=%s err=%v", res.FailedStage, res.Err)
		case res.FailedStage != "":
			status = fmt.Sprintf("FAIL stage=%s err=%v", res.FailedStage, res.Err)
		}
		fmt.Fprintf(out, "%s: %s\n", res.Path, status)
		for _, loss := range res.Losses {
			fmt.Fprintf(out, "  expected-loss: %s\n", loss.String())
		}
	}
	fmt.Fprintln(out, "---- category results ----")
	for _, cat := range Categories {
		var lines []string
		for _, res := range r.Fixtures {
			if !containsString(res.Categories, cat) {
				continue
			}
			status := "pass"
			if res.FailedStage != "" {
				if res.Expected {
					status = "expected-failure"
				} else {
					status = "FAIL"
				}
			}
			lines = append(lines, res.Path+"="+status)
		}
		if len(lines) == 0 {
			fmt.Fprintf(out, "%s: no fixtures\n", cat)
			continue
		}
		sort.Strings(lines)
		fmt.Fprintf(out, "%s: %s\n", cat, strings.Join(lines, ", "))
	}
	if r.Passed() {
		fmt.Fprintf(out, "GATE PASS: %d fixture(s) verified\n", len(r.Fixtures))
	} else {
		fmt.Fprintf(out, "GATE FAIL: %d problem(s)\n", len(r.Failures))
		for _, f := range r.Failures {
			fmt.Fprintf(out, "  - %s\n", f)
		}
	}
}

func containsString(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}
