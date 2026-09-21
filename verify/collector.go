package verify

// Fixture collection: walk the repository, find every *.rdb file and report
// its header (REDIS/VALKEY + version) plus the distinct redis data types it
// contains. There is deliberately no file-name allow-list; adding a new
// fixture anywhere under the repository root is enough to have it verified.

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// Fixture describes one discovered .rdb file.
type Fixture struct {
	// AbsPath is the absolute path on disk.
	AbsPath string
	// RelPath is the slash-separated path relative to the repository root.
	RelPath string
	// Version is the numeric rdb version parsed from the header (e.g. 11, 80).
	Version int
	// Flavor is "redis" or "valkey".
	Flavor string
	// Types is the sorted, de-duplicated set of logical data types present
	// (string/list/set/hash/zset/stream/functions, plus "aux" and "dbsize").
	Types []string
	// Encodings is the sorted, de-duplicated set of value encodings present.
	Encodings []string
	// ObjectCount is the number of objects (including special opcodes) seen.
	ObjectCount int
}

// dirsSkipped lists directory names that never contain source fixtures and
// must not be walked (build/VCS/editor metadata).
var dirsSkipped = map[string]bool{
	".git":         true,
	".svn":         true,
	".hg":          true,
	".idea":        true,
	".vscode":      true,
	"node_modules": true,
}

// Collect walks root recursively and returns all *.rdb fixtures sorted by
// repository-relative path. Type information is gathered by decoding each
// header, so a file with an unreadable header is still collected but returned
// as a fixture whose headerErr explains the problem; the caller decides
// whether a manifest entry covers it.
func Collect(root string) ([]*Fixture, error) {
	var fixtures []*Fixture
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if dirsSkipped[d.Name()] && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".rdb") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		fixtures = append(fixtures, &Fixture{AbsPath: path, RelPath: rel})
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].RelPath < fixtures[j].RelPath })
	for _, f := range fixtures {
		if err := inspect(f); err != nil {
			return nil, err
		}
	}
	return fixtures, nil
}

// inspect fills in header/type information by fully decoding the fixture with
// special opcodes enabled. A decode failure is recorded so the pipeline stage
// that consumes the fixture can report the same error deterministically.
func inspect(f *Fixture) error {
	data, err := os.ReadFile(f.AbsPath)
	if err != nil {
		return fmt.Errorf("read fixture %s: %w", f.RelPath, err)
	}
	dec := core.NewDecoder(bytes.NewReader(data)).WithSpecialOpCode()
	typeSet := make(map[string]bool)
	encSet := make(map[string]bool)
	err = dec.Parse(func(obj model.RedisObject) bool {
		f.ObjectCount++
		typeSet[obj.GetType()] = true
		if enc := obj.GetEncoding(); enc != "" {
			encSet[enc] = true
		}
		return true
	})
	if err != nil {
		// Even an unparseable file yields header/version info when the header
		// itself was valid. Surface decode errors via the pipeline, not here.
		f.Types = sortedKeys(typeSet)
		f.Encodings = sortedKeys(encSet)
		f.Version, f.Flavor = dec.RDBVersion(), flavor(dec.Valkey())
		return nil
	}
	f.Version, f.Flavor = dec.RDBVersion(), flavor(dec.Valkey())
	f.Types = sortedKeys(typeSet)
	f.Encodings = sortedKeys(encSet)
	return nil
}

func flavor(valkey bool) string {
	if valkey {
		return "valkey"
	}
	return "redis"
}
