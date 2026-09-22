package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// fixture describes a single collected .rdb file
type fixture struct {
	relPath  string // slash-separated path relative to root
	absPath  string
	format   string // REDIS or VALKEY
	rdbVersion int
}

// collectFixtures walks root recursively and returns all .rdb files,
// sorted by relative path so logs are deterministic. It never follows
// paths outside root and never touches the network or home directory.
func collectFixtures(root string) ([]fixture, error) {
	var fixtures []fixture
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".rdb") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("compute relative path of %s: %v", path, err)
		}
		rel = filepath.ToSlash(rel)
		format, version, err := sniffHeader(path)
		if err != nil {
			return fmt.Errorf("read header of %s: %v", rel, err)
		}
		fixtures = append(fixtures, fixture{
			relPath:    rel,
			absPath:    path,
			format:     format,
			rdbVersion: version,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(fixtures, func(i, j int) bool {
		return fixtures[i].relPath < fixtures[j].relPath
	})
	return fixtures, nil
}

// sniffHeader reads the 9 byte RDB header and returns the format and version.
func sniffHeader(path string) (format string, version int, err error) {
	fp, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = fp.Close() }()
	header := make([]byte, 9)
	n, err := fp.Read(header)
	if err != nil || n < 9 {
		return "", 0, fmt.Errorf("file too short for RDB header")
	}
	magic := string(header[:5])
	if magic != "REDIS" && magic != "VALKEY" {
		return "", 0, fmt.Errorf("invalid magic %q", magic)
	}
	version, err = strconv.Atoi(string(header[5:9]))
	if err != nil {
		return "", 0, fmt.Errorf("invalid version in header: %v", err)
	}
	return magic, version, nil
}
