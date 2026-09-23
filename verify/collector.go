package verify

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Feature labels describing semantic dimensions every gate run must cover.
// They are independent test cases in the emitted report: the gate fails when
// a required feature is not covered by any collected (positive) fixture.
const (
	FeatureListpack      = "listpack"       // listpack encoded containers
	FeatureStreamV1      = "stream-v1"      // RDB_TYPE_STREAM_LISTPACKS
	FeatureStreamV2      = "stream-v2"      // RDB_TYPE_STREAM_LISTPACKS_2
	FeatureStreamV3      = "stream-v3"      // RDB_TYPE_STREAM_LISTPACKS_3
	FeatureHFE           = "hfe"            // hash field expiration (redis & valkey)
	FeatureEmptyColl     = "empty-collection"
	FeatureExpiredKey    = "expired-key"
	FeatureUnknownOpcode = "unknown-opcode" // only covered by negative fixtures
)

// RequiredFeatures must all be covered (positive or negative) for a gate run
// to pass. Stream v1/v2/v3 intentionally get independent entries.
var RequiredFeatures = []string{
	FeatureListpack,
	FeatureStreamV1,
	FeatureStreamV2,
	FeatureStreamV3,
	FeatureHFE,
	FeatureEmptyColl,
	FeatureExpiredKey,
	FeatureUnknownOpcode,
}

// Fixture describes one discovered .rdb file.
type Fixture struct {
	// RelPath is the repository-relative, slash separated path.
	RelPath string
	// SizeBytes is the on-disk size of the fixture.
	SizeBytes int64
}

// CollectRDBSorted walks root (deterministic sorted order) and returns every
// file ending in .rdb. No name list is consulted: discovery is fully
// automatic. Files named in excludeRel are skipped (used for the scratch
// directory, which always lives outside the source tree anyway).
func CollectRDBSorted(root string, excludeRel map[string]struct{}) ([]Fixture, error) {
	var out []Fixture
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "target" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".rdb") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if _, skip := excludeRel[rel]; skip {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, Fixture{RelPath: rel, SizeBytes: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RelPath < out[j].RelPath })
	return out, nil
}

// FeaturesOfSnapshot derives semantic feature tags from a decoded snapshot.
// "Expired" uses wall-clock comparison against now, matching redis semantics.
func FeaturesOfSnapshot(snap *Snapshot, now time.Time) []string {
	set := map[string]struct{}{}
	for _, objs := range snap.DBs {
		for _, o := range objs {
			enc := o.GetEncoding()
			switch o.GetType() {
			case model.ListType, model.SetType, model.HashType, model.ZSetType:
				if enc == model.ListPackEncoding || enc == model.ListPackExEncoding ||
					enc == model.QuickList2Encoding {
					set[FeatureListpack] = struct{}{}
				}
			case model.StreamType:
				stream := o.(*model.StreamObject)
				switch stream.Version {
				case 1:
					set[FeatureStreamV1] = struct{}{}
				case 2:
					set[FeatureStreamV2] = struct{}{}
				case 3:
					set[FeatureStreamV3] = struct{}{}
				}
			}
			if h, ok := o.(*model.HashObject); ok && len(h.FieldExpirations) > 0 {
				set[FeatureHFE] = struct{}{}
			}
			if isEmptyContainer(o) {
				set[FeatureEmptyColl] = struct{}{}
			}
			if ex := o.GetExpiration(); ex != nil && ex.Before(now) {
				set[FeatureExpiredKey] = struct{}{}
			}
		}
	}
	return sortedKeys(set)
}

func isEmptyContainer(o model.RedisObject) bool {
	switch o.GetType() {
	case model.ListType, model.SetType, model.HashType, model.ZSetType, model.StreamType:
		return o.GetElemCount() == 0
	}
	return false
}

// RequireNonZeroCollection enforces the "zero collected fixtures fails" rule.
func RequireNonZeroCollection(fixtures []Fixture) error {
	if len(fixtures) == 0 {
		return fmt.Errorf("fixture collector found 0 .rdb files: refusing to gate on an empty set")
	}
	return nil
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// repoRootAbs is unused helper guard; kept to avoid importing os in gate path
var _ = os.Stat
