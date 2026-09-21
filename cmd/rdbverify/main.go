// Command rdbverify runs the offline fixture verification gate.
//
// It discovers every *.rdb file in the repository, performs
// decode -> re-encode -> second-decode with strict semantic comparison,
// and structurally validates AOF conversion output. No Redis process or
// network access is required.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hdt3213/rdb/rdbverify"
)

func main() {
	var (
		repoFlag     = flag.String("root", "", "repository root (auto-detected if omitted)")
		manifestFlag = flag.String("manifest", "", "negative/expected-loss manifest path (defaults to <root>/verifydata/manifest.json)")
		reportFlag   = flag.String("report", "", "optional path for the structured JSON report")
	)
	flag.Parse()

	cwd, err := os.Getwd()
	if err != nil {
		fatal("cannot determine working directory: %v", err)
	}
	root := *repoFlag
	if root == "" {
		root, err = rdbverify.FindRepoRoot(cwd)
		if err != nil {
			fatal("%v", err)
		}
	}
	manifest := *manifestFlag
	if manifest == "" {
		manifest = filepath.Join(root, "verifydata", "manifest.json")
	}

	report, err := rdbverify.Run(rdbverify.Config{RepoRoot: root, ManifestPath: manifest})
	if err != nil {
		fatal("%v", err)
	}
	if *reportFlag != "" {
		if err := rdbverify.WriteReport(*reportFlag, report); err != nil {
			fatal("write report: %v", err)
		}
	}
	fmt.Printf("\nFixtures: %d, passed (lossless/expected-loss/negative): %d, blockers: %d\n",
		report.FixtureCount, report.Passed, len(report.Blockers))
	if len(report.Blockers) > 0 {
		fmt.Println("RESULT: FAIL")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS")
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "rdbverify: "+format+"\n", args...)
	os.Exit(2)
}
