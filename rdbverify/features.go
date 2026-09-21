package rdbverify

import (
	"sort"
	"unicode/utf8"

	"github.com/hdt3213/rdb/model"
)

// Feature tags produced independently of the lossless/lossy verdict so
// that specific data-structure scenarios are visible per fixture.
const (
	FeatureEmptyCollection = "empty-collection"
	FeatureExpiredKey      = "expired-key"
	FeatureHFE             = "hash-field-expiration"
	FeatureListpack        = "listpack"
	FeatureQuicklist2      = "quicklist2"
	FeatureStreamV1        = "stream-v1"
	FeatureStreamV2        = "stream-v2"
	FeatureStreamV3        = "stream-v3"
	FeatureStreamGroups    = "stream-groups"
	FeatureStreamPEL       = "stream-pel"
	FeatureFunctions       = "functions"
	FeatureMultiDB         = "multi-db"
	FeatureNonASCIIKey     = "non-ascii-key"
	FeatureTTLKey          = "ttl-key"
)

// detectFeatures inspects a decoded fixture to tag the scenarios it
// exercises. Tags exist regardless of the lossless/lossy verdict.
func detectFeatures(dec *DecodeResult, diffs []Diff, aof *AOFReport) []string {
	_ = diffs
	_ = aof
	set := map[string]struct{}{}
	if len(dec.DBIndexes) > 1 {
		set[FeatureMultiDB] = struct{}{}
	}
	if len(dec.ExpiredKeys) > 0 {
		set[FeatureExpiredKey] = struct{}{}
	}
	for _, obj := range dec.Objects {
		if !isDataObject(obj) {
			continue
		}
		if obj.GetExpiration() != nil {
			set[FeatureTTLKey] = struct{}{}
		}
		if !isASCII(string(obj.GetKey())) {
			set[FeatureNonASCIIKey] = struct{}{}
		}
		switch o := obj.(type) {
		case *model.StreamObject:
			switch o.Version {
			case 2:
				set[FeatureStreamV2] = struct{}{}
			case 3:
				set[FeatureStreamV3] = struct{}{}
			default:
				set[FeatureStreamV1] = struct{}{}
			}
			if len(o.Groups) > 0 {
				set[FeatureStreamGroups] = struct{}{}
			}
			for _, g := range o.Groups {
				if len(g.Pending) > 0 {
					set[FeatureStreamPEL] = struct{}{}
				}
				for _, c := range g.Consumers {
					if len(c.Pending) > 0 {
						set[FeatureStreamPEL] = struct{}{}
					}
				}
			}
		case *model.HashObject:
			if len(o.FieldExpirations) > 0 {
				set[FeatureHFE] = struct{}{}
			}
			if o.GetElemCount() == 0 {
				set[FeatureEmptyCollection] = struct{}{}
			}
		case *model.ListObject:
			if o.GetElemCount() == 0 {
				set[FeatureEmptyCollection] = struct{}{}
			}
		case *model.SetObject:
			if o.GetElemCount() == 0 {
				set[FeatureEmptyCollection] = struct{}{}
			}
		case *model.ZSetObject:
			if o.GetElemCount() == 0 {
				set[FeatureEmptyCollection] = struct{}{}
			}
		case *model.FunctionsObject:
			set[FeatureFunctions] = struct{}{}
		}
		switch obj.GetEncoding() {
		case model.ListPackEncoding, model.ListPackExEncoding:
			set[FeatureListpack] = struct{}{}
		case model.QuickList2Encoding:
			set[FeatureQuicklist2] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for f := range set {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func isASCII(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
