package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	rootFlag := flag.String("root", ".", "repository root to scan for .rdb fixtures")
	manifestFlag := flag.String("manifest", "", "path to negative-fixture manifest JSON")
	reportFlag := flag.String("report", "", "write structured JSON report to this path")
	flag.Parse()

	root, err := filepath.Abs(*rootFlag)
	if err != nil {
		fail("resolve root: %v", err)
	}
	manifest, err := loadManifest(*manifestFlag)
	if err != nil {
		fail("%v", err)
	}
	manifestByPath := make(map[string]negativeFixture, len(manifest.Fixtures))
	for _, f := range manifest.Fixtures {
		manifestByPath[filepath.ToSlash(f.Path)] = f
	}

	paths, err := discoverFixtures(root)
	if err != nil {
		fail("[%s] %v", stageCollect, err)
	}
	if len(paths) == 0 {
		fail("[%s] zero .rdb fixtures discovered under %s", stageCollect, root)
	}
	fmt.Printf("fixture count: %d\n", len(paths))

	rep := &report{FixtureCount: len(paths)}
	featureCoverage := make(map[string][]string)
	for _, rel := range paths {
		res := processFixture(root, rel, manifestByPath[rel])
		for _, f := range res.Features {
			featureCoverage[f] = append(featureCoverage[f], rel)
		}
		rep.Results = append(rep.Results, res)
		switch res.Status {
		case statusPass:
			rep.Pass++
		case statusExpectedLoss:
			rep.ExpectedLoss++
		case statusBlocker:
			rep.Blocker++
		case statusNegativeOK:
			rep.NegativeOK++
		}
	}

	// stale manifest entries: declared path was not discovered at all
	var discovered = make(map[string]struct{}, len(paths))
	for _, p := range paths {
		discovered[p] = struct{}{}
	}
	var stale []string
	for _, f := range manifest.Fixtures {
		if _, ok := discovered[f.Path]; !ok {
			stale = append(stale, f.Path)
		}
	}

	printResults(rep)
	printFeatureCoverage(featureCoverage)

	if *reportFlag != "" {
		data, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*reportFlag, data, 0o644); err != nil {
			fail("write report: %v", err)
		}
	}

	var problems []string
	for _, r := range rep.Results {
		if r.Status == statusBlocker {
			problems = append(problems, fmt.Sprintf("BLOCKER %s stage=%s: %s", r.Path, r.Stage, r.Error))
		}
	}
	sort.Strings(stale)
	for _, p := range stale {
		problems = append(problems, fmt.Sprintf("stale manifest entry, fixture not found: %s", p))
	}
	for _, feature := range requiredFeatures {
		if len(featureCoverage[feature]) == 0 {
			problems = append(problems, fmt.Sprintf("required feature not covered by any fixture: %s", feature))
		}
	}

	fmt.Printf("summary: %d pass, %d expected-loss, %d negative-ok, %d blocker\n",
		rep.Pass, rep.ExpectedLoss, rep.NegativeOK, rep.Blocker)
	if len(problems) > 0 {
		fmt.Println("GATE FAILED:")
		for _, p := range problems {
			fmt.Println("  - " + p)
		}
		os.Exit(1)
	}
	fmt.Println("GATE OK")
}

func processFixture(root, rel string, neg negativeFixture) fixtureResult {
	res := fixtureResult{Path: rel}
	abs := filepath.Join(root, filepath.FromSlash(rel))
	data, err := os.ReadFile(abs)
	if err != nil {
		return failResult(res, stageCollect, err, neg)
	}
	snap, _, err := decodeSnapshot(data)
	if err != nil {
		res.Version = headerVersion(data)
		return failResult(res, stageDecode, err, neg)
	}
	res.Version = fmt.Sprintf("%s%04d", snap.flavor, snap.version)
	res.Features = classify(snap)
	res.Types = dataTypes(snap)

	reencoded, err := snap.reEncode()
	if err != nil {
		return failResult(res, stageReEncode, err, neg)
	}
	snap2, _, err := decodeSnapshot(reencoded)
	if err != nil {
		return failResult(res, stageSecondDecode, err, neg)
	}
	sem, enc := compareSnapshots(snap, snap2)

	aof := verifyAOF(snap)
	res.AOF = &aof

	if len(sem) > 0 {
		// semantic differences are never downgraded to expected-loss
		detail := make([]string, 0, len(sem))
		for _, l := range sem {
			detail = append(detail, l.Code+": "+l.Detail)
		}
		err := fmt.Errorf("%s", strings.Join(detail, "; "))
		return failResult(res, stageSemantic, err, neg)
	}
	if !aof.Supported {
		return failResult(res, stageAOF, fmt.Errorf("%s", aof.Detail), neg)
	}
	res.Losses = enc
	if aof.GroupOwned {
		// AOF cannot carry consumer groups/PENDing ownership: record as a
		// structured loss of the auxiliary representation, not of RDB semantics.
		res.Losses = append(res.Losses, Loss{Code: "aof-groups-unsupported", Detail: "stream groups/consumers/PEL ownership cannot be represented in AOF output"})
	}
	sort.Slice(res.Losses, func(i, j int) bool { return lossLess(res.Losses[i], res.Losses[j]) })

	if neg.Path != "" {
		// declared negative fixture actually passed: mismatch
		res.Status = statusBlocker
		res.Stage = neg.Stage
		res.Manifest = true
		res.Error = fmt.Sprintf("manifest expected failure at stage %s containing %q, but fixture passed", neg.Stage, neg.ErrorSub)
		return res
	}
	if len(enc) > 0 || aof.GroupOwned {
		res.Status = statusExpectedLoss
	} else {
		res.Status = statusPass
	}
	return res
}

func failResult(res fixtureResult, stage string, err error, neg negativeFixture) fixtureResult {
	res.Stage = stage
	res.Error = err.Error()
	if neg.Path != "" {
		if neg.Stage == stage && strings.Contains(err.Error(), neg.ErrorSub) {
			res.Status = statusNegativeOK
			res.Manifest = true
			res.Negative = "matched: " + neg.Reason
			return res
		}
		res.Status = statusBlocker
		res.Manifest = true
		res.Error = fmt.Sprintf("manifest mismatch: expected stage=%s error~%q, got stage=%s error=%q",
			neg.Stage, neg.ErrorSub, stage, err.Error())
		return res
	}
	res.Status = statusBlocker
	return res
}

func dataTypes(snap *snapshot) []string {
	set := make(map[string]struct{})
	for _, o := range snap.objects {
		set[o.GetType()] = struct{}{}
	}
	if len(snap.functions) > 0 {
		set["functions"] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// headerVersion extracts a printable version even from fixtures that fail to
// decode (used in negative-fixture output).
func headerVersion(data []byte) string {
	if len(data) < 9 {
		return "unknown"
	}
	prefix := string(data[:5])
	if prefix == "REDIS" || prefix == "VALKEY" {
		return prefix + string(data[5:9])
	}
	return "unknown"
}

func printResults(rep *report) {
	for _, r := range rep.Results {
		fmt.Printf("- %s\n", r.Path)
		fmt.Printf("    version=%s types=[%s] features=[%s] => %s\n",
			r.Version, strings.Join(r.Types, ","), strings.Join(r.Features, ","), r.Status)
		switch r.Status {
		case statusNegativeOK:
			fmt.Printf("    negative match @%s: %s\n", r.Stage, r.Negative)
		case statusBlocker:
			fmt.Printf("    BLOCKER @%s: %s\n", r.Stage, r.Error)
		case statusExpectedLoss:
			for _, l := range r.Losses {
				fmt.Printf("    expected-loss[%s] %s\n", l.Code, l.Detail)
			}
		}
		if r.AOF != nil {
			aofLine := fmt.Sprintf("    aof: supported=%t commands=%d", r.AOF.Supported, r.AOF.Commands)
			if r.AOF.Detail != "" {
				aofLine += " detail=" + r.AOF.Detail
			}
			if r.AOF.GroupOwned {
				aofLine += " (groups not representable)"
			}
			fmt.Println(aofLine)
		}
	}
}

func printFeatureCoverage(coverage map[string][]string) {
	all := make([]string, 0, len(coverage))
	for f := range coverage {
		all = append(all, f)
	}
	sort.Strings(all)
	fmt.Println("feature coverage:")
	for _, f := range all {
		fmt.Printf("    %s: %d fixture(s)\n", f, len(coverage[f]))
	}
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "verify: "+format+"\n", args...)
	os.Exit(1)
}
