// Command verify is the repository level RDB fixture gate.
//
// It discovers every .rdb fixture in the repository, then for each fixture
// runs a five stage pipeline without requiring a Redis process:
//
//	decode1  decode the fixture into memory (with special opcodes)
//	encode   re-encode the decoded objects into a new RDB in a temp dir
//	decode2  decode the re-encoded RDB
//	compare  semantic comparison of both decodings
//	aof      convert the fixture to AOF and validate the RESP structure
//
// Fixtures that are expected to fail must be listed explicitly in
// verify/negative_fixtures.json with a reason and the expected stage.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

const stageDecode1 = "decode1"
const stageEncode = "encode"
const stageDecode2 = "decode2"
const stageCompare = "compare"
const stageAOF = "aof"

var knownStages = []string{stageDecode1, stageEncode, stageDecode2, stageCompare, stageAOF}

// feature tags reported independently in the summary
var knownFeatures = []string{
	"listpack",
	"stream-v1",
	"stream-v2",
	"stream-v3",
	"hfe",
	"empty-collection",
	"empty-database",
	"expired-key",
	"unknown-opcode",
	"functions",
	"lru-lfu",
}

type fixtureResult struct {
	fx       *fixture
	stage    string // failed stage, empty when passed
	err      error
	losses   []lossEntry
	features []string
}

func (r *fixtureResult) passed() bool { return r.stage == "" }

func main() {
	root := flag.String("root", "", "repository root directory")
	tmp := flag.String("tmp", "", "temporary directory for re-encoded artifacts")
	flag.Parse()
	if *root == "" || *tmp == "" {
		fmt.Fprintln(os.Stderr, "usage: verify -root <repo root> -tmp <temp dir>")
		os.Exit(2)
	}
	code := run(*root, *tmp)
	os.Exit(code)
}

func run(root, tmpDir string) int {
	fmt.Println("== collect ==")
	fixtures, err := collectFixtures(root)
	if err != nil {
		fmt.Printf("FATAL collect fixtures failed: %v\n", err)
		return 1
	}
	if len(fixtures) == 0 {
		fmt.Println("FATAL no .rdb fixtures collected")
		return 1
	}
	fmt.Printf("fixtures: %d\n", len(fixtures))
	for _, fx := range fixtures {
		fx.decoded, fx.decodeErr = decodeFixture(fx.absPath)
		if fx.decoded != nil {
			fx.types = collectTypes(fx.decoded)
		}
		features := "none"
		if fx.decoded != nil && len(fx.decoded.features) > 0 {
			features = strings.Join(fx.decoded.sortedFeatures(), ",")
		}
		fmt.Printf("fixture=%s rdb=%s%d types=[%s] features=[%s]\n",
			fx.relPath, fx.magic, fx.version, strings.Join(fx.types, ","), features)
	}

	mfst, err := loadManifest(root)
	if err != nil {
		fmt.Printf("FATAL load negative manifest failed: %v\n", err)
		return 1
	}

	fmt.Println("== verify ==")
	var results []*fixtureResult
	for _, fx := range fixtures {
		res := runPipeline(fx, tmpDir)
		results = append(results, res)
		for _, l := range res.losses {
			fmt.Printf("EXPECTED-LOSS fixture=%s kind=%s subject=%s detail=%s\n",
				fx.relPath, l.kind, l.subject, l.detail)
		}
		if !res.passed() {
			fmt.Printf("stage-failure fixture=%s stage=%s err=%v\n", fx.relPath, res.stage, res.err)
		}
	}

	failed := false
	manifestUsed := map[string]bool{}
	fmt.Println("== results ==")
	for _, res := range results {
		entry, ok := mfst.byPath[res.fx.relPath]
		switch {
		case res.passed() && !ok:
			fmt.Printf("PASS %s\n", res.fx.relPath)
		case res.passed() && ok:
			manifestUsed[res.fx.relPath] = true
			failed = true
			fmt.Printf("FAIL %s: manifest expects failure at stage %s (%s) but fixture passed\n",
				res.fx.relPath, entry.Stage, entry.Reason)
		case !res.passed() && !ok:
			failed = true
			fmt.Printf("FAIL %s stage=%s err=%v\n", res.fx.relPath, res.stage, res.err)
		default:
			if entry.Stage == res.stage &&
				(entry.ErrorContains == "" || strings.Contains(res.err.Error(), entry.ErrorContains)) {
				manifestUsed[res.fx.relPath] = true
				fmt.Printf("EXPECTED-FAIL %s stage=%s reason=%s\n",
					res.fx.relPath, res.stage, entry.Reason)
			} else {
				failed = true
				fmt.Printf("FAIL %s stage=%s err=%v (manifest entry stage=%s does not match)\n",
					res.fx.relPath, res.stage, res.err, entry.Stage)
			}
		}
	}
	for _, entry := range mfst.entries {
		if !manifestUsed[entry.Path] {
			failed = true
			fmt.Printf("FAIL stale manifest entry: %s (no matching fixture outcome)\n", entry.Path)
		}
	}

	printFeatureSummary(results, mfst)

	passCount, expectedFailCount, failCount := 0, 0, 0
	for _, res := range results {
		_, negative := mfst.byPath[res.fx.relPath]
		switch {
		case res.passed() && !negative:
			passCount++
		case !res.passed() && negative:
			expectedFailCount++
		default:
			failCount++
		}
	}
	fmt.Println("== summary ==")
	fmt.Printf("total=%d pass=%d expected-fail=%d fail=%d\n",
		len(results), passCount, expectedFailCount, failCount)
	if failed || failCount > 0 {
		return 1
	}
	return 0
}

func printFeatureSummary(results []*fixtureResult, mfst *negativeManifest) {
	fmt.Println("== features ==")
	byFeature := map[string][]*fixtureResult{}
	for _, res := range results {
		for _, f := range res.features {
			byFeature[f] = append(byFeature[f], res)
		}
	}
	for _, feature := range knownFeatures {
		group := byFeature[feature]
		pass, expectedFail, fail := 0, 0, 0
		for _, res := range group {
			_, negative := mfst.byPath[res.fx.relPath]
			if res.passed() {
				pass++
			} else if negative {
				expectedFail++
			} else {
				fail++
			}
		}
		paths := make([]string, 0, len(group))
		for _, res := range group {
			paths = append(paths, res.fx.relPath)
		}
		sort.Strings(paths)
		fmt.Printf("feature=%s fixtures=%d pass=%d expected-fail=%d fail=%d files=[%s]\n",
			feature, len(group), pass, expectedFail, fail, strings.Join(paths, ","))
	}
}
