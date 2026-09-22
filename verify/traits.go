package verify

import (
	"bytes"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Feature traits independently asserted by the gate. Each fixture is tagged
// with the traits it exercises, and the final run verifies that every required
// trait is covered by at least one fixture.
const (
	traitListpack        = "listpack"
	traitStreamV1        = "stream-v1"
	traitStreamV2        = "stream-v2"
	traitStreamV3        = "stream-v3"
	traitHFE             = "hfe"
	traitEmptyCollection = "empty-collection"
	traitExpiredKey      = "expired-key"
	traitUnknownOpcode   = "unknown-opcode"
	traitFunctions       = "functions"
	traitMultiDB         = "multi-db"
	traitNonASCII        = "non-ascii-key-or-value"
)

// requiredTraits must all be covered by the fixture corpus; the gate fails
// otherwise so a missing category cannot slip through silently.
var requiredTraits = []string{
	traitListpack,
	traitStreamV1,
	traitStreamV2,
	traitStreamV3,
	traitHFE,
	traitEmptyCollection,
	traitExpiredKey,
	traitUnknownOpcode,
	traitFunctions,
}

func detectTraits(version int, valkey bool, objects []model.RedisObject) []string {
	set := map[string]struct{}{}
	dbs := map[int]struct{}{}
	now := time.Now()
	for _, obj := range objects {
		dbs[obj.GetDBIndex()] = struct{}{}
		enc := obj.GetEncoding()
		if strings.Contains(enc, model.ListPackEncoding) || enc == model.ListPackExEncoding {
			set[traitListpack] = struct{}{}
		}
		switch o := obj.(type) {
		case *model.StreamObject:
			switch o.Version {
			case 2:
				set[traitStreamV2] = struct{}{}
			case 3:
				set[traitStreamV3] = struct{}{}
			default:
				set[traitStreamV1] = struct{}{}
			}
			if len(o.Entries) == 0 {
				set[traitEmptyCollection] = struct{}{}
			}
		case *model.HashObject:
			if len(o.FieldExpirations) > 0 {
				set[traitHFE] = struct{}{}
			}
		case *model.FunctionsObject:
			set[traitFunctions] = struct{}{}
		}
		if obj.GetExpiration() != nil && obj.GetExpiration().Before(now) {
			set[traitExpiredKey] = struct{}{}
		}
		if isEmptyDataObject(obj) {
			set[traitEmptyCollection] = struct{}{}
		}
		if containsNonASCII(obj.GetKey()) || containsNonASCIIValue(obj) {
			set[traitNonASCII] = struct{}{}
		}
	}
	if len(dbs) > 1 {
		set[traitMultiDB] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func isEmptyDataObject(obj model.RedisObject) bool {
	if obj.GetElemCount() == 0 {
		switch obj.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
			return false
		}
		return true
	}
	return false
}

func containsNonASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return true
		}
	}
	return false
}

func containsNonASCIIValue(obj model.RedisObject) bool {
	switch o := obj.(type) {
	case *model.StringObject:
		return hasHighByte(o.Value)
	case *model.ListObject:
		for _, v := range o.Values {
			if hasHighByte(v) {
				return true
			}
		}
	case *model.SetObject:
		for _, v := range o.Members {
			if hasHighByte(v) {
				return true
			}
		}
	case *model.HashObject:
		for _, v := range o.Hash {
			if hasHighByte(v) {
				return true
			}
		}
	}
	return false
}

func hasHighByte(b []byte) bool {
	for _, v := range b {
		if v >= 0x80 {
			return true
		}
	}
	return false
}

func bytesReader(b []byte) *bytes.Reader {
	return bytes.NewReader(b)
}
