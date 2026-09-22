package verify

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/model"
)

func nowMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func bytesReader(b []byte) io.Reader {
	return bytes.NewReader(b)
}

func countDataObjects(objects []model.RedisObject) int {
	n := 0
	for _, o := range objects {
		switch o.(type) {
		case *model.AuxObject, *model.DBSizeObject:
			continue
		}
		n++
	}
	return n
}

func observedTypes(objects []model.RedisObject) []string {
	set := map[string]bool{}
	for _, o := range objects {
		switch o.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
			continue
		}
		enc := o.GetEncoding()
		if enc == "" {
			enc = "?"
		}
		set[o.GetType()+"/"+enc] = true
	}
	return sortedKeys(set)
}

func printResult(w io.Writer, r *Result) {
	version := "unknown"
	if r.Fixture.HeaderError == nil {
		version = fmt.Sprintf("%s-%d", r.Fixture.Flavor, r.Fixture.RDBVersion)
	}
	fmt.Fprintf(w, "  [%s] %s  version=%s objects=%d types=%s features=%s\n",
		r.Status, r.Fixture.RelPath, version, r.ObjectCount,
		strings.Join(r.Types, ","), strings.Join(r.Features, ","))
	if len(r.Losses) > 0 {
		fmt.Fprintf(w, "           losses: %s\n", strings.Join(r.Losses, ","))
	}
	if len(r.AOFCommands) > 0 {
		names := make([]string, 0, len(r.AOFCommands))
		for name := range r.AOFCommands {
			names = append(names, name)
		}
		sort.Strings(names)
		parts := make([]string, 0, len(names))
		for _, name := range names {
			parts = append(parts, fmt.Sprintf("%s=%d", name, r.AOFCommands[name]))
		}
		fmt.Fprintf(w, "           aof: %s\n", strings.Join(parts, " "))
	}
	if r.Status == statusFail || (r.Status == statusNegativeMatch && r.Error != "") {
		if r.Stage != "" {
			fmt.Fprintf(w, "           stage=%s error: %s\n", r.Stage, indentBlock(r.Error))
		} else if r.Error != "" {
			fmt.Fprintf(w, "           error: %s\n", indentBlock(r.Error))
		}
	}
}

func indentBlock(s string) string {
	return strings.ReplaceAll(s, "\n", "\n           ")
}

// validateManifestCoverage ensures every manifest entry references an
// existing discovered fixture. Negative fixtures must fail exactly as
// declared; expected-loss fixtures must actually observe their declared set.
// Mis-targeted or stale manifest entries fail the gate.
func validateManifestCoverage(fixtures []*Fixture, manifest *Manifest) error {
	discovered := map[string]*Fixture{}
	for _, f := range fixtures {
		discovered[f.RelPath] = f
	}
	var problems []string
	for _, n := range manifest.Negative {
		if _, ok := discovered[n.Path]; !ok {
			problems = append(problems, fmt.Sprintf("manifest negative entry references missing fixture: %s", n.Path))
		}
	}
	for _, l := range manifest.ExpectedLoss {
		if _, ok := discovered[l.Path]; !ok {
			problems = append(problems, fmt.Sprintf("manifest expected_loss entry references missing fixture: %s", l.Path))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("manifest validation failed:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return nil
}

// Summarize prints aggregate statistics and returns an error if any result failed.
func (r *Report) Summarize(w io.Writer) error {
	var pass, expectedLoss, negative, fail int
	var failures []string
	featureCoverage := map[string][]string{}
	for _, res := range r.Results {
		switch res.Status {
		case statusPass:
			pass++
		case statusExpectedLoss:
			expectedLoss++
		case statusNegativeMatch:
			negative++
		default:
			fail++
			failures = append(failures, res.Fixture.RelPath)
		}
		for _, f := range res.Features {
			featureCoverage[f] = append(featureCoverage[f], res.Fixture.RelPath)
		}
	}
	features := make([]string, 0, len(featureCoverage))
	for f := range featureCoverage {
		features = append(features, f)
	}
	sort.Strings(features)
	fmt.Fprintf(w, "\nfeature coverage:\n")
	for _, f := range features {
		paths := featureCoverage[f]
		sort.Strings(paths)
		fmt.Fprintf(w, "  %-20s %d fixture(s): %s\n", f, len(paths), strings.Join(paths, ", "))
	}
	fmt.Fprintf(w, "\nsummary: %d pass, %d expected-loss, %d negative-match, %d fail (total %d)\n",
		pass, expectedLoss, negative, fail, len(r.Results))
	if fail > 0 {
		sort.Strings(failures)
		return fmt.Errorf("gate failed for %d fixture(s): %s", fail, strings.Join(failures, ", "))
	}
	return nil
}

