package rdbverify

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FindRepoRoot walks up from start until it reaches a directory containing
// go.mod, which anchors the repository regardless of the caller's CWD.
func FindRepoRoot(start string) (string, error) {
	dir := start
	var prev string
	for dir != prev {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		prev = dir
		dir = filepath.Dir(dir)
	}
	return "", fmt.Errorf("could not locate repository root (go.mod) from %s", start)
}

// DiscoverRDBs walks every directory under root and returns the
// repository-relative paths of all *.rdb files, sorted lexically so that
// output is deterministic. VCS metadata and hidden directories are skipped.
func DiscoverRDBs(root string) ([]string, error) {
	var found []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (name == ".git" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".rdb") {
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(found)
	return found, nil
}
