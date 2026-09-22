package main

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// discoverFixtures walks the repository root and returns every *.rdb file as a
// slash-separated path relative to root. The walk itself is lexical and the
// final list is sorted, so output is deterministic.
func discoverFixtures(root string) ([]string, error) {
	var fixtures []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".rdb") {
			rel, rerr := filepath.Rel(root, path)
			if rerr != nil {
				return rerr
			}
			fixtures = append(fixtures, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	sort.Strings(fixtures)
	return fixtures, nil
}
