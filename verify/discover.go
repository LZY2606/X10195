package verify

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Fixture describes one discovered .rdb file.
type Fixture struct {
	// AbsPath is the absolute filesystem path.
	AbsPath string
	// RelPath is the repository-root-relative slash-separated path.
	RelPath string
}

// skipDirs are directories that never hold repository fixtures.
var skipDirs = map[string]struct{}{
	".git":         {},
	"vendor":       {},
	"node_modules": {},
}

// discoverFixtures walks root (depth-first order is not relied upon; results
// are sorted) and collects every file ending in .rdb. It never follows
// symlinks for directories, so a stray temp symlink cannot leak files from
// outside the repository into the run.
func discoverFixtures(root string) ([]Fixture, error) {
	var fixtures []Fixture
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
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".rdb") {
			rel, rerr := filepath.Rel(root, path)
			if rerr != nil {
				return rerr
			}
			fixtures = append(fixtures, Fixture{
				AbsPath: path,
				RelPath: filepath.ToSlash(rel),
			})
		}
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

// fileExists reports whether path exists without following directory symlinks.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
