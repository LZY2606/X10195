package verify

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Fixture is one collected .rdb file together with the metadata that
// can be extracted without a full decode.
type Fixture struct {
	Path    string // slash separated path relative to Root
	AbsPath string
	Magic   string // REDIS or VALKEY, empty when unreadable
	Version int
	Types   []string // filled after the decode stage
	Objects []interface{}
}

// Collect walks root and returns every .rdb file, sorted by relative path.
// It never follows symlinks and never leaves the repository root.
func Collect(root string) ([]*Fixture, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root %s: %w", root, err)
	}
	var fixtures []*Fixture
	err = filepath.Walk(absRoot, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			if info.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if strings.ToLower(filepath.Ext(path)) != ".rdb" {
			return nil
		}
		rel, err := filepath.Rel(absRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		magic, version, err := readHeader(path)
		if err != nil {
			// keep the fixture; the decode stage will report the error
			magic, version = "?", 0
		}
		fixtures = append(fixtures, &Fixture{
			Path:    rel,
			AbsPath: path,
			Magic:   magic,
			Version: version,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", absRoot, err)
	}
	sort.Slice(fixtures, func(i, j int) bool {
		return fixtures[i].Path < fixtures[j].Path
	})
	return fixtures, nil
}

// readHeader reads the 9 byte rdb header: 5 byte magic + 4 byte version.
func readHeader(path string) (string, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, 9)
	n, err := f.Read(header)
	if err != nil && n == 0 {
		return "", 0, fmt.Errorf("read header: %w", err)
	}
	if n < 9 {
		return "", 0, fmt.Errorf("file too short for rdb header (%d bytes)", n)
	}
	magic := string(header[:5])
	if magic != "REDIS" && magic != "VALKEY" {
		return "", 0, fmt.Errorf("unknown magic %q", magic)
	}
	version := 0
	for _, c := range header[5:9] {
		if c < '0' || c > '9' {
			return "", 0, fmt.Errorf("invalid version digit %q", string(c))
		}
		version = version*10 + int(c-'0')
	}
	return magic, version, nil
}
