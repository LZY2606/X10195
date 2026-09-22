// Package verify implements the project level fixture gate. It collects
// every .rdb fixture in the repository, then for each fixture runs
// decode -> re-encode -> re-decode -> semantic compare, plus a structural
// check of the AOF (RESP) conversion, without requiring a Redis process.
package verify

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/model"
)

// Verification phases, also used as expectPhase values in the negative
// fixture manifest.
const (
	PhaseDecode   = "decode"
	PhaseReencode = "reencode"
	PhaseRedecode = "redecode"
	PhaseCompare  = "compare"
	PhaseAOF      = "aof"
)

// Phases lists all known phases in execution order.
var Phases = []string{PhaseDecode, PhaseReencode, PhaseRedecode, PhaseCompare, PhaseAOF}

// ManifestName is the negative-fixture manifest location relative to Root.
const ManifestName = "verify/manifest.json"

// Fixture is one collected .rdb file.
type Fixture struct {
	RelPath string `json:"path"`
	Format  string `json:"format"` // redis, valkey or unknown
	Version int    `json:"rdbVersion"`
	absPath string
}

// Loss is a structured expected-loss record: the encoder is not able to
// represent some aspect of the input losslessly.
type Loss struct {
	Fixture string `json:"fixture"`
	Phase   string `json:"phase"`
	Kind    string `json:"kind"`
	Object  string `json:"object,omitempty"`
	Detail  string `json:"detail"`
	Reason  string `json:"reason"`
	Count   int    `json:"count"`
}

// Blocker is an unexpected failure; any blocker fails the gate.
type Blocker struct {
	Fixture string `json:"fixture"`
	Phase   string `json:"phase"`
	Detail  string `json:"detail"`
}

// NegativeFixture declares a fixture that is expected to fail at an exact
// phase, with a human readable reason.
type NegativeFixture struct {
	Path        string `json:"path"`
	Reason      string `json:"reason"`
	ExpectPhase string `json:"expectPhase"`
}

// Manifest is the explicit negative-fixture manifest.
type Manifest struct {
	NegativeFixtures []NegativeFixture `json:"negativeFixtures"`
}

// FixtureResult is the per-fixture outcome.
type FixtureResult struct {
	Path    string   `json:"path"`
	Format  string   `json:"format"`
	Version int      `json:"rdbVersion"`
	Types   []string `json:"types"`
	Status  string   `json:"status"` // pass, expected-loss, expected-failure, failed
	Phase   string   `json:"failedPhase,omitempty"`
}

// Report is the full structured gate result.
type Report struct {
	Fixtures []FixtureResult `json:"fixtures"`
	Losses   []Loss          `json:"expectedLosses"`
	Blockers []Blocker       `json:"blockers"`
}

// OK reports whether the gate passed (no blockers).
func (r *Report) OK() bool {
	return len(r.Blockers) == 0
}

// Options configures a Run.
type Options struct {
	Root    string    // repository root used for fixture collection
	WorkDir string    // scratch directory for re-encoded files and the report
	Out     io.Writer // log destination
}

// RepoRoot returns the repository root derived from this source file, so
// the gate works regardless of the caller's working directory.
func RepoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("cannot locate source file")
	}
	return filepath.Dir(filepath.Dir(file)), nil
}

// Collect finds all .rdb fixtures under root, sorted by relative path.
// Collecting zero fixtures is an error.
func Collect(root string) ([]Fixture, error) {
	var fixtures []Fixture
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".rdb") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		f := Fixture{RelPath: filepath.ToSlash(rel), absPath: path}
		readHeader(&f)
		fixtures = append(fixtures, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].RelPath < fixtures[j].RelPath })
	if len(fixtures) == 0 {
		return nil, fmt.Errorf("collected 0 .rdb fixtures under %s", root)
	}
	return fixtures, nil
}

// readHeader fills Format and Version from the 9 byte RDB header. Files
// with an unreadable header stay collected with Format "unknown" so the
// decode phase reports them.
func readHeader(f *Fixture) {
	f.Format = "unknown"
	f.Version = 0
	data, err := os.ReadFile(f.absPath)
	if err != nil || len(data) < 9 {
		return
	}
	header := string(data[:9])
	var version string
	switch {
	case strings.HasPrefix(header, "REDIS"):
		f.Format = "redis"
		version = header[len("REDIS"):]
	case strings.HasPrefix(header, "VALKEY"):
		f.Format = "valkey"
		version = header[len("VALKEY"):]
	default:
		return
	}
	v, err := strconv.Atoi(version)
	if err != nil {
		f.Format = "unknown"
		return
	}
	f.Version = v
}

// LoadManifest reads the negative-fixture manifest. A missing file means
// an empty manifest; a malformed file is an error.
func LoadManifest(root string) (*Manifest, error) {
	path := filepath.Join(root, filepath.FromSlash(ManifestName))
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &Manifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse %s failed: %v", ManifestName, err)
	}
	seen := make(map[string]bool)
	validPhase := make(map[string]bool)
	for _, p := range Phases {
		validPhase[p] = true
	}
	for _, nf := range m.NegativeFixtures {
		if nf.Path == "" || nf.Reason == "" {
			return nil, fmt.Errorf("%s: every negative fixture needs path and reason", ManifestName)
		}
		if !validPhase[nf.ExpectPhase] {
			return nil, fmt.Errorf("%s: %s has invalid expectPhase %q", ManifestName, nf.Path, nf.ExpectPhase)
		}
		if seen[nf.Path] {
			return nil, fmt.Errorf("%s: duplicate entry for %s", ManifestName, nf.Path)
		}
		seen[nf.Path] = true
	}
	return &m, nil
}

// Run executes the whole gate and returns the structured report.
func Run(opts Options) (*Report, error) {
	if opts.Root == "" {
		return nil, errors.New("root is required")
	}
	if opts.WorkDir == "" {
		return nil, errors.New("work dir is required")
	}
	if opts.Out == nil {
		opts.Out = io.Discard
	}
	if err := os.MkdirAll(opts.WorkDir, 0o755); err != nil {
		return nil, err
	}
	fixtures, err := Collect(opts.Root)
	if err != nil {
		return nil, err
	}
	manifest, err := LoadManifest(opts.Root)
	if err != nil {
		return nil, err
	}

	out := opts.Out
	fmt.Fprintf(out, "[collect] %d fixture(s) collected under %s\n", len(fixtures), opts.Root)
	for _, f := range fixtures {
		fmt.Fprintf(out, "[collect] %s format=%s rdbVersion=%s\n", f.RelPath, f.Format, versionString(f.Version))
	}

	report := &Report{}
	for i := range fixtures {
		res := verifyFixture(opts, &fixtures[i], report)
		report.Fixtures = append(report.Fixtures, res)
	}
	matchManifest(manifest, report)
	sort.Slice(report.Losses, func(i, j int) bool {
		if report.Losses[i].Fixture != report.Losses[j].Fixture {
			return report.Losses[i].Fixture < report.Losses[j].Fixture
		}
		if report.Losses[i].Kind != report.Losses[j].Kind {
			return report.Losses[i].Kind < report.Losses[j].Kind
		}
		return report.Losses[i].Detail < report.Losses[j].Detail
	})
	for _, res := range report.Fixtures {
		fmt.Fprintf(out, "[fixture] %s format=%s rdbVersion=%s types=%s status=%s\n",
			res.Path, res.Format, versionString(res.Version), strings.Join(res.Types, ","), res.Status)
	}
	for _, l := range report.Losses {
		line, _ := json.Marshal(l)
		fmt.Fprintf(out, "[expected-loss] %s\n", line)
	}
	for _, b := range report.Blockers {
		line, _ := json.Marshal(b)
		fmt.Fprintf(out, "[blocker] %s\n", line)
	}
	pass, loss, expectedFailure, failed := 0, 0, 0, 0
	for _, res := range report.Fixtures {
		switch res.Status {
		case "pass":
			pass++
		case "expected-loss":
			loss++
		case "expected-failure":
			expectedFailure++
		default:
			failed++
		}
	}
	fmt.Fprintf(out, "[summary] fixtures=%d pass=%d expected-loss=%d expected-failure=%d failed=%d blockers=%d\n",
		len(report.Fixtures), pass, loss, expectedFailure, failed, len(report.Blockers))

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(opts.WorkDir, "report.json"), data, 0o644); err != nil {
		return nil, err
	}
	return report, nil
}

func versionString(v int) string {
	if v <= 0 {
		return "unknown"
	}
	return strconv.Itoa(v)
}

// verifyFixture runs all phases for one fixture and appends losses and
// blockers to the report.
func verifyFixture(opts Options, f *Fixture, report *Report) FixtureResult {
	res := FixtureResult{Path: f.RelPath, Format: f.Format, Version: f.Version, Types: []string{}}
	workDir := filepath.Join(opts.WorkDir, "reencoded", filepath.FromSlash(f.RelPath))

	objects, err := decodeAll(f.absPath)
	if err != nil {
		return failFixture(report, res, PhaseDecode, err)
	}
	res.Types = typeList(objects)

	encoded, err := reencode(objects, f.Format == "valkey")
	if err != nil {
		return failFixture(report, res, PhaseReencode, err)
	}
	if err := os.MkdirAll(filepath.Dir(workDir), 0o755); err == nil {
		_ = os.WriteFile(workDir, encoded, 0o644)
	}

	objects2, err := decodeAllBytes(encoded)
	if err != nil {
		return failFixture(report, res, PhaseRedecode, err)
	}

	losses, blockers := compareDecoded(f.RelPath, objects, objects2)
	report.Losses = append(report.Losses, losses...)
	if len(blockers) > 0 {
		report.Blockers = append(report.Blockers, blockers...)
		return failFixture(report, res, PhaseCompare, errors.New(blockers[0].Detail))
	}

	aofLosses, aofBlockers := checkAOF(f.RelPath, objects)
	report.Losses = append(report.Losses, aofLosses...)
	if len(aofBlockers) > 0 {
		report.Blockers = append(report.Blockers, aofBlockers...)
		return failFixture(report, res, PhaseAOF, errors.New(aofBlockers[0].Detail))
	}

	if len(losses)+len(aofLosses) > 0 {
		res.Status = "expected-loss"
	} else {
		res.Status = "pass"
	}
	return res
}

func failFixture(report *Report, res FixtureResult, phase string, err error) FixtureResult {
	res.Status = "failed"
	res.Phase = phase
	if phase != PhaseCompare && phase != PhaseAOF {
		report.Blockers = append(report.Blockers, Blocker{
			Fixture: res.Path,
			Phase:   phase,
			Detail:  err.Error(),
		})
	}
	return res
}

func typeList(objects []model.RedisObject) []string {
	set := make(map[string]bool)
	for _, o := range objects {
		set[o.GetType()] = true
	}
	types := make([]string, 0, len(set))
	for t := range set {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// matchManifest reconciles failures with the negative-fixture manifest. A
// negative fixture passes only when the manifest hits exactly: same path,
// same failure phase.
func matchManifest(manifest *Manifest, report *Report) {
	expected := make(map[string]NegativeFixture)
	for _, nf := range manifest.NegativeFixtures {
		expected[nf.Path] = nf
	}
	matched := make(map[string]bool)

	byPath := make(map[string]*FixtureResult)
	for i := range report.Fixtures {
		byPath[report.Fixtures[i].Path] = &report.Fixtures[i]
	}

	var kept []Blocker
	for _, b := range report.Blockers {
		nf, ok := expected[b.Fixture]
		if !ok {
			kept = append(kept, b)
			continue
		}
		matched[b.Fixture] = true
		if nf.ExpectPhase != b.Phase {
			kept = append(kept, Blocker{
				Fixture: b.Fixture,
				Phase:   b.Phase,
				Detail: fmt.Sprintf("failed at phase %q but manifest %q expects phase %q: %s",
					b.Phase, ManifestName, nf.ExpectPhase, b.Detail),
			})
			continue
		}
		// exact hit: drop the blocker, mark the fixture as expected failure
		if res, ok := byPath[b.Fixture]; ok {
			res.Status = "expected-failure"
		}
	}
	report.Blockers = kept

	for _, nf := range manifest.NegativeFixtures {
		if matched[nf.Path] {
			continue
		}
		res, ok := byPath[nf.Path]
		if !ok {
			report.Blockers = append(report.Blockers, Blocker{
				Fixture: nf.Path,
				Phase:   nf.ExpectPhase,
				Detail:  "manifest entry refers to a fixture that was not collected",
			})
			continue
		}
		if res.Status != "expected-failure" {
			report.Blockers = append(report.Blockers, Blocker{
				Fixture: nf.Path,
				Phase:   nf.ExpectPhase,
				Detail:  "manifest entry did not trigger: fixture passed all phases",
			})
		}
	}
}
