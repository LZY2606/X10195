// Command verifyfixtures collects every .rdb fixture in the repository and
// verifies, for each of them, a full decode -> re-encode -> re-decode ->
// semantic compare round trip without requiring a Redis process.
//
// The tool never reads the home directory and never touches the network; all
// temporary artifacts are written to the directory given by -tmp.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// knownStages lists the pipeline stages in execution order. A negative
// fixture manifest entry must reference one of these stages.
var knownStages = []string{stageDecode, stageEncode, stageRedecode, stageCompare, stageAOF}

const (
	stageDecode   = "decode"
	stageEncode   = "encode"
	stageRedecode = "redecode"
	stageCompare  = "compare"
	stageAOF      = "aof"
)

type stageError struct {
	stage string
	err   error
}

func (e *stageError) Error() string { return e.err.Error() }

type expectedLoss struct {
	kind   string
	detail string
}

type fixtureResult struct {
	path     string
	version  string
	types    []string
	features []string
	losses   []expectedLoss
	objects  int
	failure  *stageError
}

func main() {
	root := flag.String("root", ".", "repository root directory")
	tmp := flag.String("tmp", "", "directory for temporary artifacts; auto-created and removed when empty")
	manifestPath := flag.String("manifest", "", "path to negative fixture manifest (default <root>/verify/negative_fixtures.json)")
	flag.Parse()

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fatalf("resolve root: %v", err)
	}
	if *manifestPath == "" {
		*manifestPath = filepath.Join(absRoot, "verify", "negative_fixtures.json")
	}

	tmpDir := *tmp
	autoTmp := false
	if tmpDir == "" {
		tmpDir, err = os.MkdirTemp("", "rdb-verify-")
		if err != nil {
			fatalf("create temp dir: %v", err)
		}
		autoTmp = true
	} else {
		if err := os.MkdirAll(tmpDir, 0o755); err != nil {
			fatalf("create temp dir: %v", err)
		}
	}
	if autoTmp {
		defer func() { _ = os.RemoveAll(tmpDir) }()
	}

	fixtures, err := collectFixtures(absRoot)
	if err != nil {
		fatalf("collect fixtures: %v", err)
	}
	fmt.Printf("COLLECT count=%d root=%s\n", len(fixtures), absRoot)
	if len(fixtures) == 0 {
		fatalf("no .rdb fixtures collected under %s", absRoot)
	}

	manifest, err := loadManifest(*manifestPath)
	if err != nil {
		fatalf("load manifest: %v", err)
	}

	var (
		results       []*fixtureResult
		expectedFails int
		blockers      int
		lossCount     int
	)
	matched := make(map[string]bool)

	for _, fx := range fixtures {
		res := processFixture(absRoot, tmpDir, fx)
		results = append(results, res)
		fmt.Printf("FIXTURE path=%s rdb_version=%s types=%s features=%s objects=%d\n",
			res.path, res.version, strings.Join(res.types, ","),
			strings.Join(res.features, ","), res.objects)
		for _, loss := range res.losses {
			lossCount++
			fmt.Printf("EXPECTED-LOSS fixture=%s kind=%s detail=%q\n", res.path, loss.kind, loss.detail)
		}
		entry, negative := manifest[res.path]
		switch {
		case negative && res.failure == nil:
			blockers++
			fmt.Printf("BLOCKER fixture=%s stage=manifest error=%q\n", res.path,
				"manifest expects failure at stage "+entry.ExpectStage+" but fixture passed")
		case negative && res.failure.stage != entry.ExpectStage:
			blockers++
			fmt.Printf("BLOCKER fixture=%s stage=%s error=%q\n", res.path, res.failure.stage,
				fmt.Sprintf("manifest expects stage %s but failed at %s: %v",
					entry.ExpectStage, res.failure.stage, res.failure.err))
		case negative:
			matched[res.path] = true
			expectedFails++
			fmt.Printf("EXPECTED-FAIL fixture=%s stage=%s reason=%q error=%q\n",
				res.path, res.failure.stage, entry.Reason, res.failure.err.Error())
		case res.failure != nil:
			blockers++
			fmt.Printf("BLOCKER fixture=%s stage=%s error=%q\n", res.path, res.failure.stage, res.failure.err.Error())
		default:
			fmt.Printf("RESULT fixture=%s status=PASS\n", res.path)
		}
	}

	for path, entry := range manifest {
		if !matched[path] {
			blockers++
			fmt.Printf("BLOCKER fixture=%s stage=manifest error=%q\n", path,
				"stale manifest entry, fixture missing or did not fail: "+entry.Reason)
		}
	}

	printFeatureSummary(results)

	pass := 0
	for _, res := range results {
		if res.failure == nil {
			pass++
		}
	}
	fmt.Printf("SUMMARY fixtures=%d pass=%d expected_fail=%d blocker=%d expected_loss=%d\n",
		len(results), pass, expectedFails, blockers, lossCount)
	if blockers > 0 {
		fmt.Println("GATE status=FAIL")
		os.Exit(1)
	}
	fmt.Println("GATE status=PASS")
}

func fatalf(format string, args ...interface{}) {
	fmt.Printf("GATE status=FAIL error=%q\n", fmt.Sprintf(format, args...))
	os.Exit(1)
}

// printFeatureSummary prints an independent result line for every detected
// feature category (listpack, stream v1/v2/v3, hfe, empty-collection,
// expired-key, unknown-opcode, functions, ...).
func printFeatureSummary(results []*fixtureResult) {
	byFeature := make(map[string][]*fixtureResult)
	for _, res := range results {
		for _, feature := range res.features {
			byFeature[feature] = append(byFeature[feature], res)
		}
	}
	names := make([]string, 0, len(byFeature))
	for name := range byFeature {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		items := byFeature[name]
		pass, fail := 0, 0
		var failing []string
		for _, res := range items {
			if res.failure == nil {
				pass++
			} else {
				fail++
				failing = append(failing, res.path)
			}
		}
		sort.Strings(failing)
		fmt.Printf("FEATURE name=%s total=%d pass=%d fail=%d failing=%s\n",
			name, len(items), pass, fail, strings.Join(failing, ","))
	}
}
