package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Loss is one structured record of non-lossless behavior of the
// decode -> re-encode -> decode pipeline.
type Loss struct {
	// Code is the stable machine-readable category.
	Code string `json:"code"`
	// Key identifies the affected key ("" for file-level losses such as the RDB version).
	Key string `json:"db,omitempty"`
	// Detail is a human-readable description with the actual differing values.
	Detail string `json:"detail"`
}

// compareSnapshots returns semantic losses (blocking) and encoding losses
// (expected representation changes). Encoding differences never hide semantic
// differences: any mismatch in db number, raw key bytes, expiration precision,
// stream ids/group/pel ownership or function payload is a semantic Loss.
func compareSnapshots(a, b *snapshot) (semantic []Loss, encoding []Loss) {
	if a.flavor != b.flavor {
		semantic = append(semantic, Loss{Code: "flavor-mismatch", Detail: fmt.Sprintf("flavor %s -> %s", a.flavor, b.flavor)})
	}
	if a.version != b.version {
		encoding = append(encoding, Loss{
			Code:   "rdb-version-change",
			Detail: fmt.Sprintf("rdb version %s%04d -> %s%04d", a.flavor, a.version, b.flavor, b.version),
		})
	}

	// aux fields: compare as normalized maps (order in the file is irrelevant)
	auxA := auxMap(a.aux)
	auxB := auxMap(b.aux)
	for k, v := range auxA {
		if vb, ok := auxB[k]; !ok {
			semantic = append(semantic, Loss{Code: "aux-missing", Key: k, Detail: fmt.Sprintf("aux field %q dropped", k)})
		} else if v != vb {
			semantic = append(semantic, Loss{Code: "aux-changed", Key: k, Detail: fmt.Sprintf("aux %q: %q -> %q", k, v, vb)})
		}
	}
	for k := range auxB {
		if _, ok := auxA[k]; !ok {
			semantic = append(semantic, Loss{Code: "aux-added", Key: k, Detail: fmt.Sprintf("aux field %q synthesized", k)})
		}
	}

	// function library payloads must be byte-identical
	if len(a.functions) != len(b.functions) {
		semantic = append(semantic, Loss{Code: "functions-count", Detail: fmt.Sprintf("function libraries %d -> %d", len(a.functions), len(b.functions))})
	} else {
		for i := range a.functions {
			if string(a.functions[i]) != string(b.functions[i]) {
				semantic = append(semantic, Loss{Code: "functions-payload", Detail: fmt.Sprintf("function library #%d payload changed", i+1)})
			}
		}
	}

	// db sets
	if !sameIntSet(a.dbs, b.dbs) {
		semantic = append(semantic, Loss{Code: "db-index-mismatch", Detail: fmt.Sprintf("db indexes %v -> %v", a.dbs, b.dbs)})
	}

	keysA := indexObjects(a)
	keysB := indexObjects(b)
	for k, oa := range keysA {
		ob, ok := keysB[k]
		if !ok {
			semantic = append(semantic, Loss{Code: "key-missing", Key: k.key, Detail: fmt.Sprintf("db %d key %q missing after re-encode", k.db, displayKey(k.key))})
			continue
		}
		sem, enc := compareObject(oa, ob)
		for _, l := range sem {
			l.Key = k.key
			semantic = append(semantic, l)
		}
		encoding = append(encoding, enc...)
	}
	for k := range keysB {
		if _, ok := keysA[k]; !ok {
			semantic = append(semantic, Loss{Code: "key-added", Key: k.key, Detail: fmt.Sprintf("db %d key %q synthesized by encoder", k.db, displayKey(k.key))})
		}
	}

	sort.Slice(semantic, func(i, j int) bool { return lossLess(semantic[i], semantic[j]) })
	sort.Slice(encoding, func(i, j int) bool { return lossLess(encoding[i], encoding[j]) })
	return semantic, encoding
}

func lossLess(a, b Loss) bool {
	if a.Code != b.Code {
		return a.Code < b.Code
	}
	if a.Key != b.Key {
		return a.Key < b.Key
	}
	return a.Detail < b.Detail
}

func auxMap(pairs []kvPair) map[string]string {
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		m[p.key] = p.value
	}
	return m
}

func sameIntSet(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]int(nil), a...)
	y := append([]int(nil), b...)
	sort.Ints(x)
	sort.Ints(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func indexObjects(s *snapshot) map[objectKey]model.RedisObject {
	m := make(map[objectKey]model.RedisObject, len(s.objects))
	for _, o := range s.objects {
		m[objectKey{db: o.GetDBIndex(), key: o.GetKey()}] = o
	}
	return m
}

func compareObject(a, b model.RedisObject) (semantic []Loss, encoding []Loss) {
	// raw key bytes are compared through the GetKey string; keys are []byte read
	// directly from the rdb and embedded via unsafe, so any byte difference shows.
	if a.GetType() != b.GetType() {
		semantic = append(semantic, Loss{Code: "type-mismatch", Detail: fmt.Sprintf("type %s -> %s", a.GetType(), b.GetType())})
		return semantic, encoding
	}
	if a.GetDBIndex() != b.GetDBIndex() {
		semantic = append(semantic, Loss{Code: "db-mismatch", Detail: fmt.Sprintf("db %d -> %d", a.GetDBIndex(), b.GetDBIndex())})
	}
	// expiration precision: exact millisecond equality
	emA := expirationMillis(a.GetExpiration())
	emB := expirationMillis(b.GetExpiration())
	if emA != emB {
		semantic = append(semantic, Loss{Code: "expiration-mismatch", Detail: fmt.Sprintf("expireAt ms %d -> %d", emA, emB)})
	}
	if evictionDiff(a, b) != "" {
		semantic = append(semantic, Loss{Code: "eviction-mismatch", Detail: evictionDiff(a, b)})
	}
	// raw rdb type flag change is an encoding-level (representation) difference
	if ta, tb := a.GetBase().RDBType, b.GetBase().RDBType; ta != tb {
		encoding = append(encoding, Loss{
			Code:   "encoding-change",
			Detail: fmt.Sprintf("rdb type byte %d (%s) -> %d (%s)", ta, a.GetEncoding(), tb, b.GetEncoding()),
		})
	}

	switch oa := a.(type) {
	case *model.StringObject:
		ob := b.(*model.StringObject)
		if !bytesEqual(oa.Value, ob.Value) {
			semantic = append(semantic, Loss{Code: "string-value-mismatch", Detail: "string bytes differ"})
		}
	case *model.ListObject:
		ob := b.(*model.ListObject)
		if !bytesListEqual(oa.Values, ob.Values) {
			semantic = append(semantic, Loss{Code: "list-value-mismatch", Detail: describeListDiff(oa.Values, ob.Values)})
		}
	case *model.SetObject:
		ob := b.(*model.SetObject)
		if !bytesSetEqual(oa.Members, ob.Members) {
			semantic = append(semantic, Loss{Code: "set-value-mismatch", Detail: "set members differ"})
		}
	case *model.HashObject:
		ob := b.(*model.HashObject)
		if !hashEqual(oa.Hash, ob.Hash) {
			semantic = append(semantic, Loss{Code: "hash-value-mismatch", Detail: "hash fields/values differ"})
		}
		if !fieldExpirationEqual(oa.FieldExpirations, ob.FieldExpirations) {
			semantic = append(semantic, Loss{Code: "field-expiration-mismatch", Detail: "hash field expiration (HFE) precision/values differ"})
		}
	case *model.ZSetObject:
		ob := b.(*model.ZSetObject)
		if !zsetEqual(oa.Entries, ob.Entries) {
			semantic = append(semantic, Loss{Code: "zset-value-mismatch", Detail: "zset members or scores differ"})
		}
	case *model.StreamObject:
		ob := b.(*model.StreamObject)
		ssem, senc := compareStream(oa, ob)
		semantic = append(semantic, ssem...)
		encoding = append(encoding, senc...)
	default:
		semantic = append(semantic, Loss{Code: "unsupported-comparison", Detail: fmt.Sprintf("cannot compare %T", a)})
	}
	return semantic, encoding
}

func expirationMillis(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

func evictionDiff(a, b model.RedisObject) string {
	ai, bi := idleValue(a), idleValue(b)
	af, bf := freqValue(a), freqValue(b)
	if (ai >= 0) != (bi >= 0) || (ai >= 0 && ai != bi) {
		return fmt.Sprintf("LRU idle %d -> %d", ai, bi)
	}
	if (af >= 0) != (bf >= 0) || (af >= 0 && af != bf) {
		return fmt.Sprintf("LFU freq %d -> %d", af, bf)
	}
	return ""
}

func idleValue(o model.RedisObject) int64 {
	if v := o.GetBase().IdleTime; v != nil {
		return *v
	}
	return -1
}

func freqValue(o model.RedisObject) int64 {
	if v := o.GetBase().Freq; v != nil {
		return *v
	}
	return -1
}

func displayKey(k string) string {
	if len(k) > 48 {
		return k[:48] + "..."
	}
	return k
}

// ---- collection normalization helpers ----

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func bytesListEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func bytesSetEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	x := make([]string, len(a))
	y := make([]string, len(b))
	for i, v := range a {
		x[i] = string(v)
	}
	for i, v := range b {
		y[i] = string(v)
	}
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func hashEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		vb, ok := b[k]
		if !ok || !bytesEqual(v, vb) {
			return false
		}
	}
	return true
}

func fieldExpirationEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func zsetEqual(a, b []*model.ZSetEntry) bool {
	if len(a) != len(b) {
		return false
	}
	sa := make(map[string]float64, len(a))
	sb := make(map[string]float64, len(b))
	for _, e := range a {
		sa[e.Member] = e.Score
	}
	for _, e := range b {
		sb[e.Member] = e.Score
	}
	if len(sa) != len(sb) {
		return false
	}
	for m, va := range sa {
		vb, ok := sb[m]
		if !ok {
			return false
		}
		if math.IsNaN(va) || math.IsNaN(vb) {
			if math.IsNaN(va) != math.IsNaN(vb) {
				return false
			}
			continue
		}
		if va != vb {
			return false
		}
	}
	return true
}

func describeListDiff(a, b [][]byte) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "list length %d -> %d", len(a), len(b))
	return sb.String()
}
