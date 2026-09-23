package main

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// Loss is a structured record of information the encoder cannot preserve.
// Only kinds whitelisted in expectedLossKinds may pass the gate; anything
// else is a blocker.
type Loss struct {
	Kind   string `json:"kind"`
	Key    string `json:"key,omitempty"`
	Detail string `json:"detail"`
}

// expectedLossKinds documents why each acceptable loss kind is unavoidable.
var expectedLossKinds = map[string]string{
	"rdb-version":       "encoder always writes its own rdb version (REDIS0011 / VALKEY0080); payload is unaffected",
	"encoding":          "encoder normalizes compact on-disk encodings; decoded payload verified identical",
	"functions-dropped": "encoder cannot emit RDB_OPCODE_FUNCTION libraries",
	"eviction-meta":     "encoder cannot emit LRU idle-time / LFU frequency metadata",
	"dbsize-added":      "encoder always emits RESIZEDB hints, even when the source file had none",
}

// comparison is the outcome of semantically comparing two decodes.
type comparison struct {
	losses   []Loss
	blockers []string
}

func quoteKey(key string) string {
	return strconv.Quote(key)
}

type objID struct {
	db  int
	key string
	typ string
}

// compareObjects semantically compares two decode passes of "the same" data.
// Map and set ordering is normalized, but db numbers, raw key bytes,
// expiration timestamps, stream ids, group/consumer/PEL ownership, function
// libraries and encoding downgrades are all checked explicitly.
func compareObjects(before, after []model.RedisObject) comparison {
	c := comparison{}

	beforeData := map[objID]model.RedisObject{}
	afterData := map[objID]model.RedisObject{}
	var beforeAux, afterAux []string
	beforeDBSize := map[int][2]uint64{}
	afterDBSize := map[int][2]uint64{}
	var beforeFuncs, afterFuncs []string

	collect := func(objs []model.RedisObject, data map[objID]model.RedisObject,
		aux *[]string, dbSize map[int][2]uint64, funcs *[]string) {
		for _, o := range objs {
			switch obj := o.(type) {
			case *model.AuxObject:
				*aux = append(*aux, obj.Key+"\x00"+obj.Value)
			case *model.DBSizeObject:
				dbSize[obj.DB] = [2]uint64{obj.KeyCount, obj.TTLCount}
			case *model.FunctionsObject:
				*funcs = append(*funcs, obj.FunctionsLua)
			default:
				id := objID{db: o.GetDBIndex(), key: o.GetKey(), typ: o.GetType()}
				if _, dup := data[id]; dup {
					c.blockers = append(c.blockers,
						fmt.Sprintf("duplicate object db=%d key=%s type=%s in one decode pass", id.db, quoteKey(id.key), id.typ))
					continue
				}
				data[id] = o
			}
		}
	}
	collect(before, beforeData, &beforeAux, beforeDBSize, &beforeFuncs)
	collect(after, afterData, &afterAux, afterDBSize, &afterFuncs)

	// data objects, keyed by (db, raw key bytes, type)
	ids := make([]objID, 0, len(beforeData))
	for id := range beforeData {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].db != ids[j].db {
			return ids[i].db < ids[j].db
		}
		if ids[i].key != ids[j].key {
			return ids[i].key < ids[j].key
		}
		return ids[i].typ < ids[j].typ
	})
	for _, id := range ids {
		a := beforeData[id]
		b, ok := afterData[id]
		if !ok {
			c.blockers = append(c.blockers,
				fmt.Sprintf("object lost after re-encode: db=%d key=%s type=%s", id.db, quoteKey(id.key), id.typ))
			continue
		}
		lo, bl := compareOne(a, b)
		c.losses = append(c.losses, lo...)
		c.blockers = append(c.blockers, bl...)
	}
	for id := range afterData {
		if _, ok := beforeData[id]; !ok {
			c.blockers = append(c.blockers,
				fmt.Sprintf("unexpected object after re-encode: db=%d key=%s type=%s", id.db, quoteKey(id.key), id.typ))
		}
	}

	// aux fields round-trip exactly (as a multiset)
	sort.Strings(beforeAux)
	sort.Strings(afterAux)
	if !strSliceEqual(beforeAux, afterAux) {
		c.blockers = append(c.blockers, "aux fields differ after re-encode")
	}

	// resize-db hints: additions are an expected-loss, changes are blockers
	for db, counts := range beforeDBSize {
		after2, ok := afterDBSize[db]
		if !ok {
			c.blockers = append(c.blockers, fmt.Sprintf("resize-db hint for db %d lost", db))
			continue
		}
		if after2 != counts {
			c.blockers = append(c.blockers,
				fmt.Sprintf("resize-db hint for db %d changed from %v to %v", db, counts, after2))
		}
	}
	for db := range afterDBSize {
		if _, ok := beforeDBSize[db]; !ok {
			c.losses = append(c.losses, Loss{
				Kind:  "dbsize-added",
				Key:   fmt.Sprintf("db=%d", db),
				Detail: expectedLossKinds["dbsize-added"],
			})
		}
	}

	// function libraries must survive; the encoder cannot emit them
	sort.Strings(beforeFuncs)
	sort.Strings(afterFuncs)
	if !strSliceEqual(beforeFuncs, afterFuncs) {
		if len(afterFuncs) == 0 && len(beforeFuncs) > 0 {
			for _, lua := range beforeFuncs {
				c.losses = append(c.losses, Loss{
					Kind:   "functions-dropped",
					Key:    "functions",
					Detail: fmt.Sprintf("function library of %d bytes dropped: %s", len(lua), expectedLossKinds["functions-dropped"]),
				})
			}
		} else {
			c.blockers = append(c.blockers, "function libraries differ after re-encode")
		}
	}

	sort.Strings(c.blockers)
	return c
}

func strSliceEqual(a, b []string) bool {
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

// compareOne compares two objects known to share db, key and type.
func compareOne(a, b model.RedisObject) (losses []Loss, blockers []string) {
	where := fmt.Sprintf("db=%d key=%s type=%s", a.GetDBIndex(), quoteKey(a.GetKey()), a.GetType())

	// expiration with millisecond precision
	aExp, bExp := a.GetExpiration(), b.GetExpiration()
	if (aExp == nil) != (bExp == nil) {
		blockers = append(blockers, where+": expiration presence changed")
	} else if aExp != nil && aExp.UnixNano()/1e6 != bExp.UnixNano()/1e6 {
		blockers = append(blockers, fmt.Sprintf("%s: expiration changed from %dms to %dms",
			where, aExp.UnixNano()/1e6, bExp.UnixNano()/1e6))
	}

	// encoding downgrade is an expected-loss, never silently ignored
	if a.GetEncoding() != b.GetEncoding() {
		losses = append(losses, Loss{
			Kind:   "encoding",
			Key:    where,
			Detail: fmt.Sprintf("encoding %q re-encoded as %q", a.GetEncoding(), b.GetEncoding()),
		})
	}

	// LRU/LFU metadata cannot be emitted by the encoder
	if a.GetIdleTime() >= 0 || a.GetFreq() >= 0 {
		losses = append(losses, Loss{
			Kind:   "eviction-meta",
			Key:    where,
			Detail: fmt.Sprintf("lru=%d lfu=%d dropped", a.GetIdleTime(), a.GetFreq()),
		})
	}

	switch ao := a.(type) {
	case *model.StringObject:
		bo := b.(*model.StringObject)
		if !bytes.Equal(ao.Value, bo.Value) {
			blockers = append(blockers, where+": string value changed")
		}
	case *model.ListObject:
		bo := b.(*model.ListObject)
		if len(ao.Values) != len(bo.Values) {
			blockers = append(blockers, fmt.Sprintf("%s: list length %d != %d", where, len(ao.Values), len(bo.Values)))
			break
		}
		for i := range ao.Values {
			if !bytes.Equal(ao.Values[i], bo.Values[i]) {
				blockers = append(blockers, fmt.Sprintf("%s: list element %d changed", where, i))
			}
		}
	case *model.SetObject:
		bo := b.(*model.SetObject)
		if !sortedBytesEqual(ao.Members, bo.Members) {
			blockers = append(blockers, where+": set members changed")
		}
	case *model.HashObject:
		bo := b.(*model.HashObject)
		if !hashEqual(ao.Hash, bo.Hash) {
			blockers = append(blockers, where+": hash fields changed")
		}
		if !fieldExpireEqual(ao.FieldExpirations, bo.FieldExpirations) {
			blockers = append(blockers, where+": hash field expirations (HFE) changed")
		}
	case *model.ZSetObject:
		bo := b.(*model.ZSetObject)
		am := zsetMap(ao.Entries)
		bm := zsetMap(bo.Entries)
		if len(am) != len(bm) {
			blockers = append(blockers, fmt.Sprintf("%s: zset size %d != %d", where, len(am), len(bm)))
			break
		}
		for member, score := range am {
			other, ok := bm[member]
			if !ok {
				blockers = append(blockers, fmt.Sprintf("%s: zset member %s lost", where, quoteKey(member)))
				continue
			}
			if math.Float64bits(score) != math.Float64bits(other) {
				blockers = append(blockers, fmt.Sprintf("%s: zset score of %s changed from %v to %v",
					where, quoteKey(member), score, other))
			}
		}
	case *model.StreamObject:
		bo := b.(*model.StreamObject)
		blockers = append(blockers, compareStreams(where, ao, bo)...)
	default:
		blockers = append(blockers, fmt.Sprintf("%s: no comparator for object type %T", where, a))
	}
	return losses, blockers
}

func sortedBytesEqual(a, b [][]byte) bool {
	as := make([]string, len(a))
	for i, v := range a {
		as[i] = string(v)
	}
	bs := make([]string, len(b))
	for i, v := range b {
		bs[i] = string(v)
	}
	sort.Strings(as)
	sort.Strings(bs)
	return strSliceEqual(as, bs)
}

func hashEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		other, ok := b[k]
		if !ok || !bytes.Equal(v, other) {
			return false
		}
	}
	return true
}

func fieldExpireEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		other, ok := b[k]
		if !ok || v != other {
			return false
		}
	}
	return true
}

func zsetMap(entries []*model.ZSetEntry) map[string]float64 {
	m := make(map[string]float64, len(entries))
	for _, e := range entries {
		m[e.Member] = e.Score
	}
	return m
}
