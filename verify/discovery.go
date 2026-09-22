package verify

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

// Fixture describes one discovered .rdb file.
type Fixture struct {
	// AbsPath is the absolute path on disk.
	AbsPath string
	// RelPath is the path relative to the repository root, using slash separators.
	RelPath string
	// Flavor is "redis" or "valkey", derived from the magic header.
	Flavor string
	// RDBVersion is the numeric version from the header (e.g. 11, 80).
	RDBVersion int
	// HeaderError is non-nil when the 9-byte header could not be probed.
	// The pipeline still runs for such files so that negative fixtures
	// fail at the expected stage rather than disappearing from discovery.
	HeaderError error
}

// DiscoverFixtures walks the repository tree and returns every *.rdb file
// found, sorted by relative path to guarantee deterministic logs. No fixed
// filename list is used: any newly added fixture is picked up automatically.
func DiscoverFixtures(repoRoot string) ([]*Fixture, error) {
	var fixtures []*Fixture
	walkErr := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "verify_tmp":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(d.Name()) != ".rdb" {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		fixture := &Fixture{
			AbsPath: path,
			RelPath: rel,
		}
		fixture.HeaderError = probeHeader(path, fixture)
		fixtures = append(fixtures, fixture)
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("walk %s: %w", repoRoot, walkErr)
	}
	sort.Slice(fixtures, func(i, j int) bool {
		return fixtures[i].RelPath < fixtures[j].RelPath
	})
	return fixtures, nil
}

// probeHeader fills flavor/version from the 9-byte RDB header and returns
// the error (if any) so the reporter can show it without aborting discovery.
func probeHeader(path string, f *Fixture) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	header := make([]byte, 9)
	if _, err := io.ReadFull(file, header); err != nil {
		return err
	}
	switch {
	case strings.HasPrefix(string(header), "REDIS"):
		f.Flavor = "redis"
		v, err := strconv.Atoi(strings.TrimSpace(string(header[5:])))
		if err != nil {
			return fmt.Errorf("invalid redis version header %q", string(header))
		}
		f.RDBVersion = v
	case strings.HasPrefix(string(header), "VALKEY"):
		f.Flavor = "valkey"
		v, err := strconv.Atoi(strings.TrimSpace(string(header[6:])))
		if err != nil {
			return fmt.Errorf("invalid valkey version header %q", string(header))
		}
		f.RDBVersion = v
	default:
		return fmt.Errorf("bad magic header %q", string(header))
	}
	return nil
}
