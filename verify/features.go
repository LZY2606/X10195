package verify

import (
	"sort"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Feature tags reported per fixture. They give the review an independent,
// machine-checkable result for each required dimension.
const (
	FeatureListpack      = "listpack"
	FeatureStreamV1      = "stream-v1"
	FeatureStreamV2      = "stream-v2"
	FeatureStreamV3      = "stream-v3"
	FeatureHFE           = "hfe"
	FeatureEmptySet      = "empty-set"
	FeatureEmptyHash     = "empty-hash"
	FeatureEmptyList     = "empty-list"
	FeatureEmptyZSet     = "empty-zset"
	FeatureEmptyStream   = "empty-stream"
	FeatureExpiredKey    = "expired-key"
	FeatureUnknownOpcode = "unknown-opcode"
	FeatureFunctions     = "function-library"
)

// detectFeatures inspects the first decode and returns the sorted,
// deduplicated feature list of a fixture.
func detectFeatures(decoded *decodeResult, nowMs int64) []string {
	set := map[string]bool{}
	for _, o := range decoded.objects {
		switch o := o.(type) {
		case *model.FunctionsObject:
			set[FeatureFunctions] = true
		case *model.StringObject:
			markExpired(set, o.BaseObject, nowMs)
		case *model.ListObject:
			markExpired(set, o.BaseObject, nowMs)
			if o.GetEncoding() == model.ListPackEncoding || o.GetEncoding() == model.QuickList2Encoding {
				set[FeatureListpack] = true
			}
			if len(o.Values) == 0 {
				set[FeatureEmptyList] = true
			}
		case *model.SetObject:
			markExpired(set, o.BaseObject, nowMs)
			if o.GetEncoding() == model.ListPackEncoding {
				set[FeatureListpack] = true
			}
			if len(o.Members) == 0 {
				set[FeatureEmptySet] = true
			}
		case *model.HashObject:
			markExpired(set, o.BaseObject, nowMs)
			if o.GetEncoding() == model.ListPackEncoding {
				set[FeatureListpack] = true
			}
			if o.GetEncoding() == model.HashExEncoding || o.GetEncoding() == model.ListPackExEncoding {
				set[FeatureHFE] = true
			}
			if len(o.Hash) == 0 {
				set[FeatureEmptyHash] = true
			}
		case *model.ZSetObject:
			markExpired(set, o.BaseObject, nowMs)
			if o.GetEncoding() == model.ListPackEncoding {
				set[FeatureListpack] = true
			}
			if len(o.Entries) == 0 {
				set[FeatureEmptyZSet] = true
			}
		case *model.StreamObject:
			markExpired(set, o.BaseObject, nowMs)
			switch o.Version {
			case 1:
				set[FeatureStreamV1] = true
			case 2:
				set[FeatureStreamV2] = true
			case 3:
				set[FeatureStreamV3] = true
			}
			if len(o.Entries) == 0 {
				set[FeatureEmptyStream] = true
			}
		}
	}
	return sortedKeys(set)
}

func markExpired(set map[string]bool, base *model.BaseObject, nowMs int64) {
	if base == nil || base.Expiration == nil {
		return
	}
	if base.Expiration.UnixNano()/int64(time.Millisecond) <= nowMs {
		set[FeatureExpiredKey] = true
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// staticLosses returns semantic losses that are unavoidable with the current
// encoder/converter, given a fixture's header and first decode.
func staticLosses(fixture *Fixture, decoded *decodeResult, snapshot *canonicalSnapshot) []string {
	set := map[string]bool{}

	// The encoder writes a fixed header version. Any other version, or the
	// other flavor, cannot be reproduced byte-faithfully.
	if fixture.Flavor == "redis" && fixture.RDBVersion != 11 {
		set[LossEncodingVersion] = true
	}

	for _, o := range decoded.objects {
		switch o := o.(type) {
		case *model.FunctionsObject:
			set[LossFunctionLibrary] = true
		case *model.StringObject:
			markEvictionLoss(set, o.BaseObject)
		case *model.ListObject:
			markEvictionLoss(set, o.BaseObject)
		case *model.SetObject:
			markEvictionLoss(set, o.BaseObject)
		case *model.HashObject:
			markEvictionLoss(set, o.BaseObject)
		case *model.ZSetObject:
			markEvictionLoss(set, o.BaseObject)
		case *model.StreamObject:
			markEvictionLoss(set, o.BaseObject)
		}
	}

	// AOF conversion of streams with groups cannot express ownership.
	for _, o := range snapshot.dbs {
		for _, co := range o.keys {
			if sv, ok := co.value.(*streamValue); ok && len(sv.obj.Groups) > 0 {
				set[LossAOFStreamMetadata] = true
			}
		}
	}

	return sortedKeys(set)
}

func markEvictionLoss(set map[string]bool, base *model.BaseObject) {
	if base == nil {
		return
	}
	if base.IdleTime != nil {
		set[LossLRU] = true
	}
	if base.Freq != nil {
		set[LossLFU] = true
	}
}
