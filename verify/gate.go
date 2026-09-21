package verify

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// StatusFailed marks a result that must fail the gate.
const (
	StatusLossless     = "lossless"
	StatusExpectedLoss = "expected-loss"
	StatusNegativeOK   = "negative-ok"
	StatusFailed       = "failed"
)

// Pass reports whether the gate is green: no repository-level errors and every
// fixture ended in an explicitly accepted state.
func (r *Report) Pass() bool {
	if len(r.GateErrors) > 0 {
		return false
	}
	for _, res := range r.Results {
		switch res.Status {
		case StatusLossless, StatusExpectedLoss, StatusNegativeOK:
		default:
			return false
		}
	}
	return true
}

// FailedResults returns only the failed per-fixture results.
func (r *Report) FailedResults() []FixtureResult {
	var out []FixtureResult
	for _, res := range r.Results {
		if res.Status == StatusFailed {
			out = append(out, res)
		}
	}
	return out
}

// PrintReport writes a deterministic, human-readable inventory and result log.
// The inventory prints fixture count, relative path, recognized RDB version
// and observed data types for every discovered fixture, before any verdict.
func PrintReport(w io.Writer, r *Report) {
	fmt.Fprintf(w, "RDB fixture verification\n")
	fmt.Fprintf(w, "discovered fixtures: %d\n", r.FixtureCount)
	fmt.Fprintf(w, "manifest: %s\n", r.ManifestPath)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "inventory:")
	for _, res := range r.Results {
		f := res.Fixture
		flavorVersion := fmt.Sprintf("%s-v%d", f.Flavor, f.RDBVersion)
		if f.RDBVersion == 0 {
			flavorVersion = "unknown-version"
		}
		types := strings.Join(f.Types, ",")
		if types == "" {
			types = "-"
		}
		encs := strings.Join(f.Encodings, ",")
		if encs == "" {
			encs = "-"
		}
		fmt.Fprintf(w, "  %-48s %-12s keys=%-3d types=[%s] encodings=[%s]\n",
			f.RelPath, flavorVersion, f.KeyCount, types, encs)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "results:")
	for _, res := range r.Results {
		line := fmt.Sprintf("  [%s] %s", res.Status, res.Fixture.RelPath)
		switch res.Status {
		case StatusNegativeOK:
			line += fmt.Sprintf(" stage=%s", res.Stage)
			if res.Error != "" {
				line += fmt.Sprintf(" error=%q", truncate(res.Error, 160))
			}
		case StatusExpectedLoss:
			var reasons []string
			for _, l := range res.Losses {
				reasons = append(reasons, string(l.Reason))
			}
			sort.Strings(reasons)
			line += " losses=" + strings.Join(reasons, ",")
		case StatusFailed:
			if res.Stage != "" {
				line += fmt.Sprintf(" stage=%s", res.Stage)
			}
			if res.Error != "" {
				line += fmt.Sprintf(" error=%q", truncate(res.Error, 240))
			}
		case StatusLossless:
			if res.AOFStatus == "checked" {
				line += " aof=checked"
			} else if res.AOFStatus == "not-supported" {
				line += " aof=not-supported"
			}
		}
		if len(res.Tags) > 0 {
			line += " tags=" + strings.Join(res.Tags, ",")
		}
		fmt.Fprintln(w, line)
		if res.Status == StatusFailed && len(res.Diffs) > 0 {
			for _, d := range res.Diffs {
				fmt.Fprintf(w, "      diff: %s\n", truncate(d.Detail, 300))
			}
		}
		if res.Status == StatusFailed && res.AOFStatus == "checked" && res.AOFIssue != "" {
			fmt.Fprintf(w, "      aof: %s\n", truncate(res.AOFIssue, 300))
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "coverage:")
	tags := make([]string, 0, len(r.Coverage))
	for t := range r.Coverage {
		tags = append(tags, t)
	}
	sort.Strings(tags)
	for _, t := range tags {
		paths := append([]string(nil), r.Coverage[t]...)
		sort.Strings(paths)
		fmt.Fprintf(w, "  %-22s %d fixture(s): %s\n", t, len(paths), strings.Join(paths, ","))
	}
	var missing []string
	seen := map[string]struct{}{}
	for _, t := range tags {
		seen[t] = struct{}{}
	}
	for _, t := range RequiredCoverageTags {
		if _, ok := seen[t]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		fmt.Fprintf(w, "  MISSING: %s\n", strings.Join(missing, ","))
	}
	if len(r.GateErrors) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "gate errors:")
		for _, e := range r.GateErrors {
			fmt.Fprintf(w, "  - %s\n", e)
		}
	}
	fmt.Fprintln(w)
	if r.Pass() {
		fmt.Fprintln(w, "VERIFY: PASS")
	} else {
		fmt.Fprintln(w, "VERIFY: FAIL")
	}
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
