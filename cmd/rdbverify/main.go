// Command rdbverify is the Go entry point for the project RDB fixture gate.
//
// It discovers every *.rdb fixture in the repository and, for each one,
// performs decode -> re-encode -> second decode -> semantic comparison, plus
// an AOF/RESP conversion structure check. No Redis process is required.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hdt3213/rdb/verify"
)

func main() {
	repoRoot := flag.String("root", "", "repository root (defaults to the parent of this command directory)")
	flag.Parse()

	root := *repoRoot
	if root == "" {
		// cmd/rdbverify lives two levels below the repository root.
		exec, err := os.Executable()
		if err == nil {
			root = filepath.Clean(filepath.Join(filepath.Dir(exec), "..", ".."))
		}
	}
	if root == "" {
		fmt.Fprintln(os.Stderr, "rdbverify: cannot determine repository root, pass -root")
		os.Exit(2)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rdbverify: %v\n", err)
		os.Exit(2)
	}

	report, err := verify.Run(abs, os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rdbverify: %v\n", err)
		os.Exit(1)
	}
	if err := report.Summarize(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "rdbverify: %v\n", err)
		os.Exit(1)
	}
}
