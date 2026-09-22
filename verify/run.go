package verify

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Options controls a gate run.
type Options struct {
	// RepoRoot is the repository root; discovery and manifest paths are
	// resolved relative to it.
	RepoRoot string
	// ManifestPath is the explicit negative-fixture / expected-loss manifest.
	ManifestPath string
	// ReportPath, when set, receives the structured JSON report.
	ReportPath string
	// Log is the human-readable output sink.
	Log io.Writer
}

// Run discovers every .rdb fixture under the repository, executes the
// decode -> reencode -> decode -> semantic pipeline for each and returns nil
// when the gate passes.
func Run(opts Options) error {
	if opts.Log == nil {
		opts.Log = io.Discard
	}
	log := opts.Log

	manifestPath := opts.ManifestPath
	if !filepath.IsAbs(manifestPath) {
		manifestPath = filepath.Join(opts.RepoRoot, manifestPath)
	}
	manifest, err := loadManifest(manifestPath)
	if err != nil {
		return err
	}

	fixtures, err := discoverFixtures(opts.RepoRoot)
	if err != nil {
		return fmt.Errorf("discover fixtures: %w", err)
	}
	if len(fixtures) == 0 {
		return fmt.Errorf("fixture collection is empty under %s: refusing to gate zero files", opts.RepoRoot)
	}

	fmt.Fprintf(log, "collected %d fixture(s) from %s\n", len(fixtures), opts.RepoRoot)
	report := &Report{FixtureCount: len(fixtures)}

	for _, f := range fixtures {
		res := runFixture(f, manifest)
		report.Results = append(report.Results, res)
		printFixtureLine(log, res)
	}

	// Manifest entries must exactly match discovered fixtures.
	discovered := map[string]struct{}{}
	for _, f := range fixtures {
		discovered[f.RelPath] = struct{}{}
	}
	for _, p := range manifestPaths(manifest) {
		if _, ok := discovered[p]; !ok {
			report.Blockers = append(report.Blockers,
				fmt.Sprintf("manifest references %s which is not a discovered fixture", p))
		}
	}

	// Every required corpus trait must be covered.
	traitCoverage := map[string]string{}
	for _, r := range report.Results {
		for _, t := range r.Traits {
			if _, ok := traitCoverage[t]; !ok {
				traitCoverage[t] = r.Path
			}
		}
	}
	var missingTraits []string
	for _, t := range requiredTraits {
		if _, ok := traitCoverage[t]; !ok {
			missingTraits = append(missingTraits, t)
		}
	}
	sort.Strings(missingTraits)
	for _, t := range missingTraits {
		report.Blockers = append(report.Blockers, fmt.Sprintf("required trait not covered by any fixture: %s", t))
	}

	counts := map[string]int{}
	for _, r := range report.Results {
		counts[r.Kind]++
		if r.Kind == KindBlocker {
			detail := r.Error
			if detail == "" {
				detail = r.FailedStage
			}
			report.Blockers = append(report.Blockers, fmt.Sprintf("%s: %s", r.Path, detail))
		}
	}

	printTraitMatrix(log, traitCoverage, missingTraits)
	fmt.Fprintf(log, "summary: %d lossless, %d expected-loss, %d negative, %d blocker(s)\n",
		counts[KindLossless], counts[KindExpectedLoss], counts[KindNegative], counts[KindBlocker])

	if opts.ReportPath != "" {
		data, mErr := json.MarshalIndent(report, "", "  ")
		if mErr != nil {
			return mErr
		}
		if wErr := os.WriteFile(opts.ReportPath, append(data, '\n'), 0o644); wErr != nil {
			return wErr
		}
	}

	if len(report.Blockers) > 0 {
		sort.Strings(report.Blockers)
		return fmt.Errorf("gate failed with %d blocker(s):\n  - %s",
			len(report.Blockers), strings.Join(report.Blockers, "\n  - "))
	}
	return nil
}

func printFixtureLine(log io.Writer, res FixtureResult) {
	traits := ""
	if len(res.Traits) > 0 {
		traits = " traits=[" + strings.Join(res.Traits, ",") + "]"
	}
	encs := ""
	if len(res.Encodings) > 0 {
		encs = " enc=[" + strings.Join(res.Encodings, ",") + "]"
	}
	switch res.Kind {
	case KindBlocker:
		fmt.Fprintf(log, "BLOCKER   %-55s ver=%-9s types=%s%s%s\n  stage=%s error=%s\n",
			res.Path, res.RDBVersion, strings.Join(res.Types, "+"), encs, traits, res.FailedStage, res.Error)
	case KindNegative:
		fmt.Fprintf(log, "NEGATIVE  %-55s ver=%-9s types=%s%s%s (failed at %s as expected)\n",
			res.Path, res.RDBVersion, strings.Join(res.Types, "+"), encs, traits, res.FailedStage)
	case KindExpectedLoss:
		fmt.Fprintf(log, "EXP-LOSS  %-55s ver=%-9s types=%s%s%s losses=%s\n",
			res.Path, res.RDBVersion, strings.Join(res.Types, "+"), encs, traits, strings.Join(res.Losses, ","))
	default:
		fmt.Fprintf(log, "LOSSLESS  %-55s ver=%-9s types=%s%s%s\n",
			res.Path, res.RDBVersion, strings.Join(res.Types, "+"), encs, traits)
	}
}

func printTraitMatrix(log io.Writer, coverage map[string]string, missing []string) {
	fmt.Fprintln(log, "trait coverage:")
	uniq := append([]string{}, requiredTraits...)
	uniq = append(uniq, traitMultiDB, traitNonASCII)
	sort.Strings(uniq)
	printed := map[string]struct{}{}
	for _, t := range uniq {
		if _, ok := printed[t]; ok {
			continue
		}
		printed[t] = struct{}{}
		if p, ok := coverage[t]; ok {
			fmt.Fprintf(log, "  [x] %-18s %s\n", t, p)
		} else {
			fmt.Fprintf(log, "  [ ] %-18s MISSING\n", t)
		}
	}
	_ = missing
}
