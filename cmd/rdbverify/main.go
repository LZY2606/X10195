// Command rdbverify is the Go entry point for the repository RDB fixture
// gate. It is normally invoked through verify.sh but can be run directly.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hdt3213/rdb/verify"
)

func main() {
	root := flag.String("root", ".", "repository root containing .rdb fixtures")
	manifest := flag.String("manifest", "verify/manifest.json", "negative-fixture and expected-loss manifest")
	report := flag.String("report", "", "optional path for structured JSON report")
	flag.Parse()

	absRoot, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *root != "." && *root != "" {
		absRoot, err = resolveRoot(*root)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}

	err = verify.Run(verify.Options{
		RepoRoot:     absRoot,
		ManifestPath: *manifest,
		ReportPath:   *report,
		Log:          os.Stdout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %s\n", err)
		os.Exit(1)
	}
}

func resolveRoot(root string) (string, error) {
	if root == "" {
		return os.Getwd()
	}
	return filepath.Abs(root)
}
