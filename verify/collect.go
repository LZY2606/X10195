package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// fixture describes one collected .rdb file.
type fixture struct {
	relPath string // repository relative path, slash separated
	absPath string
	version int    // rdb version parsed from the 9 byte header
	flavor  string // "redis" or "valkey"
}

// collectFixtures discovers every .rdb file below root. The result is sorted
// by relative path so that logs are deterministic. Hidden directories (such
// as .git) are skipped. Collecting zero fixtures is an error.
func collectFixtures(root string) ([]fixture, error) {
	var relPaths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".rdb") {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relPaths = append(relPaths, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %v", root, err)
	}
	sort.Strings(relPaths)
	if len(relPaths) == 0 {
		return nil, fmt.Errorf("collected 0 .rdb fixtures under %s", root)
	}
	fixtures := make([]fixture, 0, len(relPaths))
	for _, rel := range relPaths {
		abs := filepath.Join(root, filepath.FromSlash(rel))
		version, flavor, err := readRDBHeader(abs)
		if err != nil {
			return nil, fmt.Errorf("read header of %s: %v", rel, err)
		}
		fixtures = append(fixtures, fixture{
			relPath: rel,
			absPath: abs,
			version: version,
			flavor:  flavor,
		})
	}
	return fixtures, nil
}

// readRDBHeader parses the 9 byte RDB magic header without any library help.
func readRDBHeader(path string) (version int, flavor string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, 9)
	if _, err = io.ReadFull(f, header); err != nil {
		return 0, "", fmt.Errorf("file too short for rdb header")
	}
	var digits string
	switch {
	case strings.HasPrefix(string(header), "REDIS"):
		flavor = "redis"
		digits = string(header[5:])
	case strings.HasPrefix(string(header), "VALKEY"):
		flavor = "valkey"
		digits = string(header[6:])
	default:
		return 0, "", fmt.Errorf("bad magic %q", string(header))
	}
	version, err = strconv.Atoi(digits)
	if err != nil {
		return 0, "", fmt.Errorf("bad version %q", digits)
	}
	return version, flavor, nil
}
