package verify

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// fixture is a collected .rdb file inside the repository.
type fixture struct {
	// RelPath is the slash separated path relative to the repository root.
	RelPath string
	// AbsPath is the absolute path of the fixture file.
	AbsPath string
	// Magic is the 9 byte rdb header, e.g. "REDIS0011" or "VALKEY080".
	Magic string
	// Valkey is true when the file uses the VALKEY magic number.
	Valkey bool
}

// collectFixtures walks root and returns every *.rdb file, sorted by
// relative path so that logs are deterministic. It never follows symlinks
// and never leaves root.
func collectFixtures(root string) ([]fixture, error) {
	var fixtures []fixture
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "node_modules" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
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
		magic, valkey, err := probeHeader(path)
		if err != nil {
			// keep the fixture; the decode stage will report the error
			magic = "unreadable: " + err.Error()
		}
		fixtures = append(fixtures, fixture{
			RelPath: rel,
			AbsPath: path,
			Magic:   magic,
			Valkey:  valkey,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s failed: %v", root, err)
	}
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].RelPath < fixtures[j].RelPath })
	return fixtures, nil
}

// probeHeader reads the 9 byte rdb magic and reports whether it is a
// Valkey file. It does not validate the version range; that is left to
// the decoder so that broken headers surface as decode-stage failures.
func probeHeader(path string) (magic string, valkey bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, 9)
	n, err := f.Read(header)
	if err != nil && n == 0 {
		return "", false, err
	}
	header = header[:n]
	if strings.HasPrefix(string(header), "VALKEY") {
		return string(header), true, nil
	}
	return string(header), false, nil
}
