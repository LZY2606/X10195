package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// fixture is one collected .rdb file.
type fixture struct {
	path string // repository relative, slash separated
	abs  string
}

// collectFixtures walks root and returns every .rdb file, sorted by relative
// path for deterministic logs. It never leaves the repository tree.
func collectFixtures(root string) ([]fixture, error) {
	var fixtures []fixture
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".rdb") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fixtures = append(fixtures, fixture{path: filepath.ToSlash(rel), abs: path})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].path < fixtures[j].path })
	return fixtures, nil
}

// detectVersion reads the 9 byte RDB header and reports the magic and the
// numeric RDB version without decoding the file.
func detectVersion(abs string) (string, error) {
	f, err := os.Open(abs)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, 9)
	n, err := f.Read(header)
	if err != nil || n < len(header) {
		return "", fmt.Errorf("cannot read rdb header")
	}
	var versionString string
	switch {
	case strings.HasPrefix(string(header), "REDIS"):
		versionString = string(header[5:])
	case strings.HasPrefix(string(header), "VALKEY"):
		versionString = string(header[6:])
	default:
		return "", fmt.Errorf("unknown rdb magic %q", string(header[:5]))
	}
	version, err := strconv.Atoi(versionString)
	if err != nil {
		return "", fmt.Errorf("invalid rdb version %q", versionString)
	}
	return strconv.Itoa(version), nil
}
