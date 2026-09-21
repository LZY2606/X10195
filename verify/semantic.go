package verify

// Strict semantic comparator for two decoded rdb models.
//
// Order of map/set members is normalized (those containers are unordered in
// Redis semantics), but the comparator must NOT normalize away:
//   - db index of every object,
//   - raw bytes of keys / values / members,
//   - expiration millisecond precision (key-level and hash field-level),
//   - stream entry/group/consumer/pel ids and ownership,
//   - function library payload bytes,
//   - list/zset/stream ordering.
//
// Differences carry stable codes so the manifest can grant narrowly scoped
// expected-loss allowances instead of accepting a successful second decode.

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Semantic difference codes.
const (
	// DiffObjectCount: number of compared objects differs.
	DiffObjectCount = "object-count"
	// DiffAux: aux metadata key/value differs.
	DiffAux = "aux"
	// DiffFunctions: function library payload differs.
	DiffFunctions = "functions"
	// DiffType: redis type of an object differs.
	DiffType = "object-type"
	// DiffDB: db index of an object differs.
	DiffDB = "db-index"
	// DiffKey: raw key bytes differ.
	DiffKey = "key-bytes"
	// DiffTTL: key expiration (ms precision) differs.
	DiffTTL = "expiration-ms"
	// DiffLRU: lru idle metadata differs / is lost.
	DiffLRU = "lru-idle"
	// DiffLFU: lfu frequency metadata differs / is lost.
	DiffLFU = "lfu-freq"
	// DiffValue: value payload of a non-stream object differs.
	DiffValue = "value"
	// DiffFieldTTL: hash field-level expiration (ms precision) differs.
	DiffFieldTTL = "hash-field-expiration-ms"
	// DiffStream: structural difference inside a stream object.
	DiffStream = "stream"
)

// EncodingTransform is not a semantic difference: it marks that the on-disk
// encoding of an otherwise identical object changed (e.g. ziplist -> list).
// The gate reports transforms for visibility but never treats them as losses.
const EncodingTransform = "encoding-transform"

// Difference is a single, code-tagged mismatch between two models.
type Difference struct {
	Code    string
	Detail  string
}

func (d Difference) String() string {
	return fmt.Sprintf("%s: %s", d.Code, d.Detail)
}

type hashField struct {
	Field  string
	Value  []byte
	Expire int64 // 0 means no field-level ttl
}

// Comparator compares two decoded models.
type Comparator struct {
	// now is used only to flag (not to ignore) keys already expired.
	now time.Time
}

// NewComparator builds a comparator anchored at now.
func NewComparator(now time.Time) *Comparator {
	return &Comparator{now: now}
}

// Compare returns semantic differences plus the list of benign encoding
// transforms between the original decode and the second decode.
func (c *Comparator) Compare(a, b *Decoded) (diffs []Difference, transforms []string) {
	ao := filterComparable(a.Objects)
	bo := filterComparable(b.Objects)
	if len(ao) != len(bo) {
		diffs = append(diffs, Difference{
			Code:   DiffObjectCount,
			Detail: fmt.Sprintf("%d objects before re-encode, %d after", len(ao), len(bo)),
		})
	}
	n := len(ao)
	if len(bo) < n {
		n = len(bo)
	}
	for i := 0; i < n; i++ {
		x, y := ao[i], bo[i]
		if x.GetType() != y.GetType() {
			diffs = append(diffs, Difference{DiffType, fmt.Sprintf("object %d: %s != %s", i, x.GetType(), y.GetType())})
			continue
		}
		if x.GetDBIndex() != y.GetDBIndex() {
			diffs = append(diffs, Difference{DiffDB, fmt.Sprintf("key %q: db %d != %d", x.GetKey(), x.GetDBIndex(), y.GetDBIndex())})
		}
		if x.GetKey() != y.GetKey() {
			diffs = append(diffs, Difference{DiffKey, fmt.Sprintf("object %d: key %q != %q", i, x.GetKey(), y.GetKey())})
		}
		if x.GetEncoding() != y.GetEncoding() {
			transforms = append(transforms, fmt.Sprintf("key %q: %s -> %s", x.GetKey(), x.GetEncoding(), y.GetEncoding()))
		}
		if ms(x.GetExpiration()) != ms(y.GetExpiration()) {
			diffs = append(diffs, Difference{DiffTTL, fmt.Sprintf("key %q: expiration %d != %d ms", x.GetKey(), ms(x.GetExpiration()), ms(y.GetExpiration()))})
		}
		if idle(x) != idle(y) {
			diffs = append(diffs, Difference{DiffLRU, fmt.Sprintf("key %q: lru idle %d != %d", x.GetKey(), idle(x), idle(y))})
		}
		if freq(x) != freq(y) {
			diffs = append(diffs, Difference{DiffLFU, fmt.Sprintf("key %q: lfu freq %d != %d", x.GetKey(), freq(x), freq(y))})
		}
		switch xo := x.(type) {
		case *model.AuxObject:
			yo := y.(*model.AuxObject)
			if xo.Value != yo.Value {
				diffs = append(diffs, Difference{DiffAux, fmt.Sprintf("aux %q: value differs", xo.Key)})
			}
		case *model.FunctionsObject:
			yo := y.(*model.FunctionsObject)
			if xo.FunctionsLua != yo.FunctionsLua {
				diffs = append(diffs, Difference{DiffFunctions, "function library payload bytes differ"})
			}
		case *model.StringObject:
			yo := y.(*model.StringObject)
			if !bytes.Equal(xo.Value, yo.Value) {
				diffs = append(diffs, Difference{DiffValue, fmt.Sprintf("string key %q: value bytes differ", xo.Key)})
			}
		case *model.ListObject:
			yo := y.(*model.ListObject)
			if !byteSliceSliceEqual(xo.Values, yo.Values) {
				diffs = append(diffs, Difference{DiffValue, fmt.Sprintf("list key %q: ordered elements differ", xo.Key)})
			}
		case *model.SetObject:
			yo := y.(*model.SetObject)
			if !memberSetEqual(xo.Members, yo.Members) {
				diffs = append(diffs, Difference{DiffValue, fmt.Sprintf("set key %q: member set differs", xo.Key)})
			}
		case *model.ZSetObject:
			yo := y.(*model.ZSetObject)
			if !zsetEqual(xo.Entries, yo.Entries) {
				diffs = append(diffs, Difference{DiffValue, fmt.Sprintf("zset key %q: ordered member/score pairs differ", xo.Key)})
			}
		case *model.HashObject:
			yo := y.(*model.HashObject)
			d := compareHash(xo, yo)
			diffs = append(diffs, d...)
		case *model.StreamObject:
			yo := y.(*model.StreamObject)
			if d := compareStream(xo, yo); d != "" {
				diffs = append(diffs, Difference{DiffStream, fmt.Sprintf("stream key %q: %s", xo.Key, d)})
			}
		}
	}
	return diffs, transforms
}

// filterComparable drops DBSizeObject hints: they are recomputed during
// re-encoding from actual db contents rather than part of redis semantics.
func filterComparable(objs []model.RedisObject) []model.RedisObject {
	out := make([]model.RedisObject, 0, len(objs))
	for _, obj := range objs {
		if _, ok := obj.(*model.DBSizeObject); ok {
			continue
		}
		out = append(out, obj)
	}
	return out
}

func ms(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

func idle(o model.RedisObject) int64 {
	if ev, ok := o.(model.EvictionInfo); ok {
		return ev.GetIdleTime()
	}
	return -1
}

func freq(o model.RedisObject) int64 {
	if ev, ok := o.(model.EvictionInfo); ok {
		return ev.GetFreq()
	}
	return -1
}

func byteSliceSliceEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func memberSetEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	ca := make([][]byte, len(a))
	copy(ca, a)
	cb := make([][]byte, len(b))
	copy(cb, b)
	sort.Slice(ca, func(i, j int) bool { return bytes.Compare(ca[i], ca[j]) < 0 })
	sort.Slice(cb, func(i, j int) bool { return bytes.Compare(cb[i], cb[j]) < 0 })
	return byteSliceSliceEqual(ca, cb)
}

func zsetEqual(a, b []*model.ZSetEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		// order is part of the comparison: scores must match exactly too
		if a[i].Member != b[i].Member || a[i].Score != b[i].Score {
			return false
		}
	}
	return true
}

func compareHash(a, b *model.HashObject) []Difference {
	var diffs []Difference
	if len(a.Hash) != len(b.Hash) {
		diffs = append(diffs, Difference{DiffValue, fmt.Sprintf("hash key %q: field count %d != %d", a.Key, len(a.Hash), len(b.Hash))})
	}
	fields := make([]string, 0, len(a.Hash))
	for f := range a.Hash {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	for _, f := range fields {
		va, oka := a.Hash[f]
		vb, okb := b.Hash[f]
		if !oka || !okb {
			diffs = append(diffs, Difference{DiffValue, fmt.Sprintf("hash key %q: field %q presence differs", a.Key, f)})
			continue
		}
		if !bytes.Equal(va, vb) {
			diffs = append(diffs, Difference{DiffValue, fmt.Sprintf("hash key %q: field %q value bytes differ", a.Key, f)})
		}
		ea := a.FieldExpirations[f]
		eb := b.FieldExpirations[f]
		if ea != eb {
			diffs = append(diffs, Difference{DiffFieldTTL, fmt.Sprintf("hash key %q: field %q expiration %d != %d ms", a.Key, f, ea, eb)})
		}
	}
	return diffs
}
