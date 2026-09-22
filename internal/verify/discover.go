// Package verify implements the repository-wide RDB fixture gate.
//
// It discovers every *.rdb fixture in the repository, decodes each file,
// re-encodes the decoded objects, decodes the result a second time and
// compares the two decodings semantically. Fixtures that are known to be
// unreadable or that trigger a known encoder limitation must be listed in
// the explicit manifest with a reason.
package verify

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// skipDirs are repository directories that must never be scanned for
// fixtures. ".git" holds pack files, the others are tool/temp locations.
var skipDirs = map[string]struct{}{
	".git":         {},
	"node_modules": {},
}

// Fixture describes one discovered .rdb file.
type Fixture struct {
	// AbsPath is the absolute path on disk.
	AbsPath string
	// RelPath is the repository-relative slash-separated path.
	RelPath string
}

// DiscoverFixtures walks root recursively and returns every *.rdb file in
// deterministic (lexicographic, case-sensitive) order. The walk itself never
// follows symlinks for directories.
func DiscoverFixtures(root string) ([]Fixture, error) {
	var fixtures []Fixture
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if _, ok := skipDirs[d.Name()]; ok && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if !strings.HasSuffix(path, ".rdb") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fixtures = append(fixtures, Fixture{
			AbsPath: path,
			RelPath: filepath.ToSlash(rel),
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.Slice(fixtures, func(i, j int) bool {
		return fixtures[i].RelPath < fixtures[j].RelPath
	})
	return fixtures, nil
}
