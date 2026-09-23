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

// fixture is one discovered .rdb file in the repository
type fixture struct {
	relPath   string // slash separated path relative to repo root
	absPath   string
	magic     string // REDIS or VALKEY, empty when unreadable
	version   int    // rdb version from header, -1 when unreadable
	types     []string
	decoded   *decodedFixture // result of the decode1 stage
	decodeErr error
}

// collectFixtures discovers every .rdb file below root. The result is sorted
// by relative path so logs are deterministic. It never follows network or
// home directory locations: only the tree below root is walked.
func collectFixtures(root string) ([]*fixture, error) {
	var relPaths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir // skip .git and other hidden dirs
			}
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".rdb") {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relPaths = append(relPaths, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s failed: %v", root, err)
	}
	sort.Strings(relPaths)
	fixtures := make([]*fixture, 0, len(relPaths))
	for _, rel := range relPaths {
		fx := &fixture{
			relPath: rel,
			absPath: filepath.Join(root, filepath.FromSlash(rel)),
			version: -1,
		}
		fx.magic, fx.version = readRDBHeader(fx.absPath)
		fixtures = append(fixtures, fx)
	}
	return fixtures, nil
}

// readRDBHeader reads the 9 byte RDB header and reports the magic and version.
// Invalid headers are reported as empty magic and version -1; the decode stage
// will produce the authoritative error for such fixtures.
func readRDBHeader(path string) (string, int) {
	f, err := os.Open(path)
	if err != nil {
		return "", -1
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, 9)
	n, err := f.Read(header)
	if err != nil || n < 9 {
		return "", -1
	}
	var magic string
	switch {
	case strings.HasPrefix(string(header), "REDIS"):
		magic = "REDIS"
	case strings.HasPrefix(string(header), "VALKEY"):
		magic = "VALKEY"
	default:
		return "", -1
	}
	version, err := strconv.Atoi(string(header[len(magic):9]))
	if err != nil {
		return magic, -1
	}
	return magic, version
}
