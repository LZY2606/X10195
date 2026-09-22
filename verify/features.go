package main

import (
	"sort"
	"time"

	"github.com/hdt3213/rdb/model"
)

// feature tags are stable identifiers describing what one fixture exercises.
// They are used both for printed output and for coverage enforcement: every
// required feature must be exercised by at least one discovered fixture.
const (
	featureRDBVersion3  = "rdb-v3"
	featureRDBVersion4  = "rdb-v4"
	featureRDBVersion5  = "rdb-v5"
	featureRDBVersion8  = "rdb-v8"
	featureRDBVersion9  = "rdb-v9"
	featureRDBVersion10 = "rdb-v10"
	featureRDBVersion11 = "rdb-v11"
	featureRDBVersion12 = "rdb-v12"
	featureValkey80     = "valkey-v80"

	featureTypeString = "type-string"
	featureTypeList   = "type-list"
	featureTypeSet    = "type-set"
	featureTypeHash   = "type-hash"
	featureTypeZSet   = "type-zset"
	featureTypeStream = "type-stream"
	featureFunctions  = "function-library"

	featureEncListPack   = "listpack"
	featureEncZipList    = "ziplist"
	featureEncZipMap     = "zipmap"
	featureEncIntSet     = "intset"
	featureEncQuickList  = "quicklist"
	featureEncQuickList2 = "quicklist2"
	featureStreamV1      = "stream-v1"
	featureStreamV2      = "stream-v2"
	featureStreamV3      = "stream-v3"
	featureHFE           = "hfe"

	featureEmptyCollection = "empty-collection"
	featureExpiredKey      = "expired-key"
	featureWithExpiry      = "key-expiry"
	featureUnknownOpcode   = "unknown-opcode"
	featureLRU             = "lru"
	featureLFU             = "lfu"
	featureMultipleDB      = "multiple-db"
)

// requiredFeatures must each be covered by at least one fixture or the gate
// fails. This is the mechanism that prevents a subtle fixture deletion from
// shrinking coverage silently.
var requiredFeatures = []string{
	featureRDBVersion3,
	featureRDBVersion5,
	featureRDBVersion11,
	featureRDBVersion12,
	featureTypeString,
	featureTypeList,
	featureTypeSet,
	featureTypeHash,
	featureTypeZSet,
	featureTypeStream,
	featureEncListPack,
	featureEncZipList,
	featureEncZipMap,
	featureEncIntSet,
	featureEncQuickList,
	featureEncQuickList2,
	featureStreamV1,
	featureStreamV2,
	featureStreamV3,
	featureHFE,
	featureEmptyCollection,
	featureExpiredKey,
	featureUnknownOpcode,
	featureFunctions,
}

// classify returns the sorted, de-duplicated feature tags of a snapshot.
func classify(snap *snapshot) []string {
	set := make(map[string]struct{})
	switch {
	case snap.flavor == "VALKEY":
		set[featureValkey80] = struct{}{}
	case snap.version == 3:
		set[featureRDBVersion3] = struct{}{}
	case snap.version == 4:
		set[featureRDBVersion4] = struct{}{}
	case snap.version == 5:
		set[featureRDBVersion5] = struct{}{}
	case snap.version == 8:
		set[featureRDBVersion8] = struct{}{}
	case snap.version == 9:
		set[featureRDBVersion9] = struct{}{}
	case snap.version == 10:
		set[featureRDBVersion10] = struct{}{}
	case snap.version == 11:
		set[featureRDBVersion11] = struct{}{}
	case snap.version == 12:
		set[featureRDBVersion12] = struct{}{}
	}

	if len(snap.functions) > 0 {
		set[featureFunctions] = struct{}{}
	}
	dbSet := make(map[int]struct{})
	now := time.Now()
	for _, obj := range snap.objects {
		dbSet[obj.GetDBIndex()] = struct{}{}
		base := obj.GetBase()
		if base.IdleTime != nil {
			set[featureLRU] = struct{}{}
		}
		if base.Freq != nil {
			set[featureLFU] = struct{}{}
		}
		if obj.GetExpiration() != nil {
			set[featureWithExpiry] = struct{}{}
			if !obj.GetExpiration().After(now) {
				set[featureExpiredKey] = struct{}{}
			}
		}
		switch o := obj.(type) {
		case *model.StringObject:
			set[featureTypeString] = struct{}{}
		case *model.ListObject:
			set[featureTypeList] = struct{}{}
			if len(o.Values) == 0 {
				set[featureEmptyCollection] = struct{}{}
			}
		case *model.SetObject:
			set[featureTypeSet] = struct{}{}
			if len(o.Members) == 0 {
				set[featureEmptyCollection] = struct{}{}
			}
		case *model.HashObject:
			set[featureTypeHash] = struct{}{}
			if len(o.Hash) == 0 {
				set[featureEmptyCollection] = struct{}{}
			}
			if len(o.FieldExpirations) > 0 {
				set[featureHFE] = struct{}{}
			}
		case *model.ZSetObject:
			set[featureTypeZSet] = struct{}{}
			if len(o.Entries) == 0 {
				set[featureEmptyCollection] = struct{}{}
			}
		case *model.StreamObject:
			set[featureTypeStream] = struct{}{}
			if len(o.Entries) == 0 && o.Length == 0 {
				set[featureEmptyCollection] = struct{}{}
			}
			switch o.Version {
			case 2:
				set[featureStreamV2] = struct{}{}
			case 3:
				set[featureStreamV3] = struct{}{}
			default:
				set[featureStreamV1] = struct{}{}
			}
		}
		switch base.Encoding {
		case model.ListPackEncoding:
			set[featureEncListPack] = struct{}{}
		case model.ZipListEncoding:
			set[featureEncZipList] = struct{}{}
		case model.ZipMapEncoding:
			set[featureEncZipMap] = struct{}{}
		case model.IntSetEncoding:
			set[featureEncIntSet] = struct{}{}
		case model.QuickListEncoding:
			set[featureEncQuickList] = struct{}{}
		case model.QuickList2Encoding:
			set[featureEncQuickList2] = struct{}{}
		}
	}
	if len(dbSet) > 1 {
		set[featureMultipleDB] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for f := range set {
		result = append(result, f)
	}
	sort.Strings(result)
	return result
}
