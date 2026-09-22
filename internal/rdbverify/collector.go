// Package rdbverify implements the repository-wide RDB fixture verification gate.
//
// Every .rdb file found below the repository root is decoded, re-encoded with
// this project's own encoder, decoded a second time and compared semantically
// with the first decode. No Redis server is involved.
package rdbverify

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// rdbSuffix is the only fixture extension collected.
const rdbSuffix = ".rdb"

// Fixture describes a single .rdb file discovered in the repository.
type Fixture struct {
	// AbsPath is the absolute path on disk.
	AbsPath string
	// RelPath is the repository-relative, slash-separated path used in reports.
	RelPath string
	// Size is the fixture size in bytes.
	Size int64
}

// headerInfo is the dialect/version pair read from an RDB file header.
type headerInfo struct {
	magic   string // "REDIS" or "VALKEY"
	version int
}

// CollectFixtures walks root (sorted) and returns every *.rdb file found.
//
// The result is sorted by relative path so that logs are deterministic.
// Directories that are not part of the source tree (e.g. .git and temp
// directories created by the gate itself) are pruned.
func CollectFixtures(root string) ([]*Fixture, error) {
	var fixtures []*Fixture
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || isGateTempDir(name) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, rdbSuffix) {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil {
			return statErr
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		fixtures = append(fixtures, &Fixture{
			AbsPath: path,
			RelPath: filepath.ToSlash(rel),
			Size:    info.Size(),
		})
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("collect rdb fixtures: %w", walkErr)
	}
	sort.Slice(fixtures, func(i, j int) bool {
		return fixtures[i].RelPath < fixtures[j].RelPath
	})
	return fixtures, nil
}

// isGateTempDir reports whether name is a temp directory created by the gate.
// Pruning these keeps repeated runs inside a temp root from self-collecting
// re-encoded artifacts.
func isGateTempDir(name string) bool {
	return strings.HasPrefix(name, gateTempPrefix)
}

// readHeader parses the 9-byte "REDISxxxx" / "VALKEYxx" header without touching
// the remainder of the file.
func readHeader(path string) (headerInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return headerInfo{}, err
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, 9)
	if _, err := readFull(f, header); err != nil {
		return headerInfo{}, err
	}
	return parseHeader(header)
}
