package verify

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/hdt3213/rdb/model"
)

// canonicalDB holds the semantically meaningful content of one logical database.
// Map/set iteration order is normalized away, while DB number, raw key bytes,
// expiration precision, stream ids and group/consumer/PEL ownership are kept.
type canonicalDB struct {
	index int
	keys  map[string]*canonicalObject
}

type canonicalObject struct {
	db         int
	key        string
	typ        string
	expireMs   int64 // 0 means persistent
	idleTime   int64 // -1 means absent
	freq       int64 // -1 means absent
	value      canonicalValue
}

type canonicalValue interface {
	kind() string
}

type stringValue struct{ data []byte }

func (stringValue) kind() string { return model.StringType }

type listValue struct{ items [][]byte }

func (listValue) kind() string { return model.ListType }

// setValue is sorted so member order does not affect equality.
type setValue struct{ members [][]byte }

func (setValue) kind() string { return model.SetType }

type hashField struct {
	value  []byte
	expire int64 // field-level expiration in ms; 0 means persistent
}

type hashValue struct{ fields map[string]hashField }

func (*hashValue) kind() string { return model.HashType }

type zsetMember struct {
	member string
	score  float64
}

type zsetValue struct{ members []zsetMember }

func (*zsetValue) kind() string { return model.ZSetType }

type streamValue struct {
	obj *model.StreamObject
}

func (*streamValue) kind() string { return model.StreamType }

// auxField is a single ordered aux metadata pair.
type auxField struct {
	key   string
	value string
}

// canonicalSnapshot is the order-independent semantic view of one RDB decode.
type canonicalSnapshot struct {
	aux       []auxField
	functions []string // raw functions payloads
	dbs       map[int]*canonicalDB
	dbOrder   []int
	flavor    string
	version   int
}

func canonicalize(decoded *decodeResult) *canonicalSnapshot {
	snap := &canonicalSnapshot{
		flavor:  decoded.flavor,
		version: decoded.version,
		dbs:     map[int]*canonicalDB{},
	}
	for _, o := range decoded.objects {
		switch o := o.(type) {
		case *model.AuxObject:
			snap.aux = append(snap.aux, auxField{key: o.Key, value: o.Value})
			continue
		case *model.FunctionsObject:
			snap.functions = append(snap.functions, o.FunctionsLua)
			continue
		case *model.DBSizeObject:
			continue
		}
		db, ok := snap.dbs[o.GetDBIndex()]
		if !ok {
			db = &canonicalDB{index: o.GetDBIndex(), keys: map[string]*canonicalObject{}}
			snap.dbs[o.GetDBIndex()] = db
			snap.dbOrder = append(snap.dbOrder, o.GetDBIndex())
		}
		db.keys[o.GetKey()] = canonicalObjectOf(o)
	}
	return snap
}

func canonicalObjectOf(o model.RedisObject) *canonicalObject {
	co := &canonicalObject{
		db:       o.GetDBIndex(),
		key:      o.GetKey(),
		typ:      o.GetType(),
		idleTime: -1,
		freq:     -1,
	}
	if exp := o.GetExpiration(); exp != nil {
		co.expireMs = exp.UnixNano() / 1e6
	}
	if ei, ok := o.(model.EvictionInfo); ok {
		co.idleTime = ei.GetIdleTime()
		co.freq = ei.GetFreq()
	}
	switch o := o.(type) {
	case *model.StringObject:
		co.value = stringValue{data: append([]byte(nil), o.Value...)}
	case *model.ListObject:
		items := make([][]byte, len(o.Values))
		for i, v := range o.Values {
			items[i] = append([]byte(nil), v...)
		}
		co.value = listValue{items: items}
	case *model.SetObject:
		members := make([][]byte, len(o.Members))
		for i, v := range o.Members {
			members[i] = append([]byte(nil), v...)
		}
		sort.Slice(members, func(i, j int) bool { return bytesLess(members[i], members[j]) })
		co.value = setValue{members: members}
	case *model.HashObject:
		fields := make(map[string]hashField, len(o.Hash))
		for field, value := range o.Hash {
			fields[field] = hashField{
				value:  append([]byte(nil), value...),
				expire: o.FieldExpirations[field],
			}
		}
		co.value = &hashValue{fields: fields}
	case *model.ZSetObject:
		members := make([]zsetMember, 0, len(o.Entries))
		for _, e := range o.Entries {
			members = append(members, zsetMember{member: e.Member, score: e.Score})
		}
		sort.Slice(members, func(i, j int) bool { return members[i].member < members[j].member })
		co.value = &zsetValue{members: members}
	case *model.StreamObject:
		co.value = &streamValue{obj: o}
	default:
		co.value = &unsupportedValue{name: fmt.Sprintf("%T", o)}
	}
	return co
}

type unsupportedValue struct{ name string }

func (*unsupportedValue) kind() string { return "unsupported" }

func bytesLess(a, b []byte) bool {
	return strings.Compare(string(a), string(b)) < 0
}

// compareSnapshots returns the list of semantic differences between the first
// and second decode. An empty result means the re-encode preserved semantics.
func compareSnapshots(a, b *canonicalSnapshot) []string {
	var diffs []string

	// DB numbering must match exactly.
	if len(a.dbOrder) != len(b.dbOrder) {
		diffs = append(diffs, fmt.Sprintf("db count: %d != %d", len(a.dbOrder), len(b.dbOrder)))
	}
	for _, dbIndex := range a.dbOrder {
		adb := a.dbs[dbIndex]
		bdb, ok := b.dbs[dbIndex]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("db %d missing after re-encode", dbIndex))
			continue
		}
		diffs = append(diffs, compareDBs(dbIndex, adb, bdb)...)
	}
	for _, dbIndex := range b.dbOrder {
		if _, ok := a.dbs[dbIndex]; !ok {
			diffs = append(diffs, fmt.Sprintf("db %d unexpectedly appeared after re-encode", dbIndex))
		}
	}

	if len(a.functions) != len(b.functions) {
		diffs = append(diffs, fmt.Sprintf("function libraries: %d != %d", len(a.functions), len(b.functions)))
	} else {
		for i := range a.functions {
			if a.functions[i] != b.functions[i] {
				diffs = append(diffs, fmt.Sprintf("function library[%d] payload changed", i))
			}
		}
	}
	return diffs
}

func compareDBs(dbIndex int, a, b *canonicalDB) []string {
	var diffs []string
	if len(a.keys) != len(b.keys) {
		diffs = append(diffs, fmt.Sprintf("db %d key count: %d != %d", dbIndex, len(a.keys), len(b.keys)))
	}
	for key, ao := range a.keys {
		bo, ok := b.keys[key]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("db %d key %q missing after re-encode", dbIndex, key))
			continue
		}
		diffs = append(diffs, compareObjects(dbIndex, key, ao, bo)...)
	}
	for key := range b.keys {
		if _, ok := a.keys[key]; !ok {
			diffs = append(diffs, fmt.Sprintf("db %d key %q unexpectedly appeared after re-encode", dbIndex, key))
		}
	}
	return diffs
}

func compareObjects(dbIndex int, key string, a, b *canonicalObject) []string {
	prefix := fmt.Sprintf("db %d key %q", dbIndex, key)
	var diffs []string
	if a.typ != b.typ {
		diffs = append(diffs, fmt.Sprintf("%s type: %s != %s", prefix, a.typ, b.typ))
	}
	if a.expireMs != b.expireMs {
		diffs = append(diffs, fmt.Sprintf("%s expiration(ms): %d != %d", prefix, a.expireMs, b.expireMs))
	}
	if a.idleTime != b.idleTime {
		diffs = append(diffs, fmt.Sprintf("%s lru-idle: %d != %d", prefix, a.idleTime, b.idleTime))
	}
	if a.freq != b.freq {
		diffs = append(diffs, fmt.Sprintf("%s lfu-freq: %d != %d", prefix, a.freq, b.freq))
	}
	if a.value.kind() != b.value.kind() {
		diffs = append(diffs, fmt.Sprintf("%s value kind: %s != %s", prefix, a.value.kind(), b.value.kind()))
		return diffs
	}
	switch av := a.value.(type) {
	case stringValue:
		bv := b.value.(stringValue)
		if !bytesEqual(av.data, bv.data) {
			diffs = append(diffs, fmt.Sprintf("%s string value changed", prefix))
		}
	case listValue:
		bv := b.value.(listValue)
		diffs = append(diffs, compareBytesList(prefix+" list", av.items, bv.items)...)
	case setValue:
		bv := b.value.(setValue)
		diffs = append(diffs, compareBytesList(prefix+" set", av.members, bv.members)...)
	case *hashValue:
		bv := b.value.(*hashValue)
		diffs = append(diffs, compareHashes(prefix, av, bv)...)
	case *zsetValue:
		bv := b.value.(*zsetValue)
		diffs = append(diffs, compareZSets(prefix, av, bv)...)
	case *streamValue:
		bv := b.value.(*streamValue)
		diffs = append(diffs, compareStreams(prefix, av.obj, bv.obj)...)
	}
	return diffs
}

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

func compareBytesList(prefix string, a, b [][]byte) []string {
	if len(a) != len(b) {
		return []string{fmt.Sprintf("%s length: %d != %d", prefix, len(a), len(b))}
	}
	for i := range a {
		if !bytesEqual(a[i], b[i]) {
			return []string{fmt.Sprintf("%s element[%d] changed", prefix, i)}
		}
	}
	return nil
}

func compareHashes(prefix string, a, b *hashValue) []string {
	var diffs []string
	if len(a.fields) != len(b.fields) {
		diffs = append(diffs, fmt.Sprintf("%s field count: %d != %d", prefix, len(a.fields), len(b.fields)))
	}
	for field, af := range a.fields {
		bf, ok := b.fields[field]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("%s field %q missing", prefix, field))
			continue
		}
		if !bytesEqual(af.value, bf.value) {
			diffs = append(diffs, fmt.Sprintf("%s field %q value changed", prefix, field))
		}
		if af.expire != bf.expire {
			diffs = append(diffs, fmt.Sprintf("%s field %q expiration(ms): %d != %d", prefix, field, af.expire, bf.expire))
		}
	}
	for field := range b.fields {
		if _, ok := a.fields[field]; !ok {
			diffs = append(diffs, fmt.Sprintf("%s unexpected field %q", prefix, field))
		}
	}
	return diffs
}

func compareZSets(prefix string, a, b *zsetValue) []string {
	var diffs []string
	if len(a.members) != len(b.members) {
		return []string{fmt.Sprintf("%s member count: %d != %d", prefix, len(a.members), len(b.members))}
	}
	// both slices are sorted by member
	for i := range a.members {
		am, bm := a.members[i], b.members[i]
		if am.member != bm.member {
			diffs = append(diffs, fmt.Sprintf("%s member[%d]: %q != %q", prefix, i, am.member, bm.member))
			continue
		}
		if !floatEqual(am.score, bm.score) {
			diffs = append(diffs, fmt.Sprintf("%s member %q score: %v != %v", prefix, am.member, am.score, bm.score))
		}
	}
	return diffs
}

// floatEqual treats NaN as equal to NaN (zset scores may be NaN/inf) and
// otherwise requires exact bit equality so score precision loss is caught.
func floatEqual(a, b float64) bool {
	if math.IsNaN(a) {
		return math.IsNaN(b)
	}
	return math.Float64bits(a) == math.Float64bits(b)
}
