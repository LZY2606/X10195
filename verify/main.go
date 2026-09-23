// Command rdb-verify is the project level fixture verification gate.
//
// It automatically collects every .rdb fixture in the repository and, for
// each file, runs decode -> re-encode -> re-decode -> semantic compare.
// Objects that support AOF conversion additionally get a structural check
// of the converted command lines. The whole process is library based and
// never spawns a Redis process.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	root := flag.String("root", ".", "repository root used for fixture discovery")
	manifest := flag.String("manifest", "", "negative fixture manifest path (default <root>/verify-negative.json)")
	tmpDir := flag.String("tmpdir", "", "directory for re-encoded artifacts (default: auto created, auto removed)")
	flag.Parse()

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve root: %v\n", err)
		os.Exit(2)
	}
	manifestPath := *manifest
	if manifestPath == "" {
		manifestPath = filepath.Join(absRoot, "verify-negative.json")
	}

	workDir := *tmpDir
	if workDir == "" {
		dir, err := os.MkdirTemp("", "rdb-verify-")
		if err != nil {
			fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
			os.Exit(2)
		}
		defer func() { _ = os.RemoveAll(dir) }()
		workDir = dir
	}

	if !runGate(absRoot, manifestPath, workDir, os.Stdout) {
		os.Exit(1)
	}
}
