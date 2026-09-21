package verify

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// ErrNoFixtures is returned when the collector discovers zero .rdb files.
// A zero-result collection always fails the gate: a typo in the scan root must
// never silently turn into a green run.
var ErrNoFixtures = errors.New("no .rdb fixtures discovered")

// peekHeader parses the 9-byte RDB magic header without decoding the body.
func peekHeader(data []byte) (flavor string, version int, err error) {
	if len(data) < 9 {
		return "", 0, errors.New("file too short for an RDB header")
	}
	switch {
	case strings.HasPrefix(string(data[:5]), "REDIS"):
		flavor = "redis"
	case strings.HasPrefix(string(data[:6]), "VALKEY"):
		flavor = "valkey"
	default:
		return "", 0, errors.New("file is not a RDB file")
	}
	prefix := 5
	if flavor == "valkey" {
		prefix = 6
	}
	version, err = strconv.Atoi(strings.TrimSpace(string(data[prefix:9])))
	if err != nil {
		return "", 0, fmt.Errorf("invalid rdb version in header: %q", string(data[prefix:9]))
	}
	return flavor, version, nil
}

// collect walks casesDir recursively, sorts every directory's entries and
// returns one FixtureInfo per discovered *.rdb file. Sorting guarantees
// deterministic logs regardless of filesystem iteration order.
//
// Decoding happens here as well so the printed inventory already shows the
// recognized RDB version and data types; un-decodable files are still
// collected (with whatever the header reveals) and reported as failures later.
func collect(opts Options) ([]FixtureInfo, map[string]error) {
	type found struct {
		rel, abs string
	}
	var founds []found
	root := opts.CasesDir
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(d.Name()) == ".rdb" {
			rel, relErr := filepath.Rel(opts.RepoRoot, path)
			if relErr != nil {
				rel = path
			}
			founds = append(founds, found{rel: filepath.ToSlash(rel), abs: path})
		}
		return nil
	})
	sort.Slice(founds, func(i, j int) bool { return founds[i].rel < founds[j].rel })

	infos := make([]FixtureInfo, 0, len(founds))
	decodeErrs := make(map[string]error)
	for _, f := range founds {
		info := FixtureInfo{RelPath: f.rel, AbsPath: f.abs, Types: []string{}, Encodings: []string{}}
		data, err := os.ReadFile(f.abs)
		if err == nil {
			if flavor, version, herr := peekHeader(data); herr == nil {
				info.Flavor, info.RDBVersion = flavor, version
			} else {
				decodeErrs[f.rel] = herr
			}
		}
		typeSet := map[string]struct{}{}
		encSet := map[string]struct{}{}
		file, err := os.Open(f.abs)
		if err != nil {
			decodeErrs[f.rel] = err
			infos = append(infos, info)
			continue
		}
		dec := core.NewDecoder(file).WithSpecialOpCode()
		parseErr := dec.Parse(func(object model.RedisObject) bool {
			switch object.(type) {
			case *model.AuxObject:
				typeSet[model.AuxType] = struct{}{}
			case *model.DBSizeObject:
				typeSet[model.DBSizeType] = struct{}{}
			case *model.FunctionsObject:
				typeSet[model.FunctionsType] = struct{}{}
				encSet["functions"] = struct{}{}
			default:
				info.KeyCount++
				typeSet[object.GetType()] = struct{}{}
				if enc := object.GetEncoding(); enc != "" {
					encSet[enc] = struct{}{}
				}
			}
			return true
		})
		_ = file.Close()
		if parseErr != nil {
			if _, known := decodeErrs[f.rel]; !known {
				decodeErrs[f.rel] = parseErr
			}
		}
		info.Types = sortedKeys(typeSet)
		info.Encodings = sortedKeys(encSet)
		infos = append(infos, info)
	}
	return infos, decodeErrs
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
