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

// fixture describes one discovered .rdb file in the repository.
type fixture struct {
	// Path is the slash separated path relative to the repository root.
	Path string
	// AbsPath is the absolute path used to open the file.
	AbsPath string
	// RDBVersion is the version detected from the file header, e.g. "REDIS:11".
	RDBVersion string
	// Valkey is true when the file uses the VALKEY magic header.
	Valkey bool
	// HeaderOK is false when the header could not be recognized; the decode
	// stage is expected to report the precise error.
	HeaderOK bool
}

// skippedDirs are directories never scanned for fixtures.
var skippedDirs = map[string]bool{
	".git":   true,
	"target": true,
	"tmp":    true,
}

// collectFixtures walks root and returns every *.rdb file, sorted by its
// slash separated relative path so logs are deterministic.
func collectFixtures(root string) ([]*fixture, error) {
	var fixtures []*fixture
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && skippedDirs[d.Name()] {
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
		rel = filepath.ToSlash(rel)
		f := &fixture{Path: rel, AbsPath: path}
		f.RDBVersion, f.Valkey, f.HeaderOK = inspectHeader(path)
		fixtures = append(fixtures, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].Path < fixtures[j].Path })
	return fixtures, nil
}

// inspectHeader reads the 9 byte RDB magic and version. It never fails:
// unrecognized headers are reported through HeaderOK so the decode stage
// produces the authoritative error.
func inspectHeader(path string) (version string, valkey bool, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return "unreadable", false, false
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, 9)
	n, err := f.Read(header)
	if err != nil || n < len(header) {
		return "unknown", false, false
	}
	if strings.HasPrefix(string(header), "REDIS") {
		v, err := strconv.Atoi(string(header[5:9]))
		if err != nil {
			return "unknown", false, false
		}
		return fmt.Sprintf("REDIS:%d", v), false, true
	}
	if strings.HasPrefix(string(header), "VALKEY") {
		v, err := strconv.Atoi(string(header[6:9]))
		if err != nil {
			return "unknown", false, false
		}
		return fmt.Sprintf("VALKEY:%d", v), true, true
	}
	return "unknown", false, false
}
