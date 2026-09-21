package main

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// fixture describes a single .rdb file discovered in the repository.
type fixture struct {
	Path    string // repository root relative path, slash separated
	Abs     string // absolute path
	Magic   string // REDIS, VALKEY or UNKNOWN
	Version int    // rdb version parsed from the header, 0 when unknown
}

// skippedDirs are directories never scanned for fixtures. They are build or
// test scratch directories, never fixture sources.
var skippedDirs = map[string]bool{
	"node_modules": true,
	"tmp":          true,
}

// collectFixtures walks root and returns every .rdb file below it, sorted by
// relative path so that logs are deterministic.
func collectFixtures(root string) ([]fixture, error) {
	var fixtures []fixture
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || skippedDirs[name]) {
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
		fixtures = append(fixtures, fixture{Path: filepath.ToSlash(rel), Abs: path})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].Path < fixtures[j].Path })
	for i := range fixtures {
		magic, version, err := sniffHeader(fixtures[i].Abs)
		if err != nil {
			magic, version = "UNKNOWN", 0
		}
		fixtures[i].Magic = magic
		fixtures[i].Version = version
	}
	return fixtures, nil
}

// sniffHeader reads the 9 byte rdb header and reports the magic and version.
func sniffHeader(path string) (string, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, 9)
	if _, err := io.ReadFull(f, header); err != nil {
		return "", 0, err
	}
	if bytes.HasPrefix(header, []byte("REDIS")) {
		version, err := strconv.Atoi(string(header[5:9]))
		if err != nil {
			return "REDIS", 0, fmt.Errorf("invalid redis rdb version %q", string(header[5:9]))
		}
		return "REDIS", version, nil
	}
	if bytes.HasPrefix(header, []byte("VALKEY")) {
		version, err := strconv.Atoi(string(header[6:9]))
		if err != nil {
			return "VALKEY", 0, fmt.Errorf("invalid valkey rdb version %q", string(header[6:9]))
		}
		return "VALKEY", version, nil
	}
	return "", 0, fmt.Errorf("unrecognized rdb magic %q", string(header))
}
