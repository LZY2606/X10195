package rdbverify

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/model"
)

// SemanticDiff is a single semantic mismatch between first and second decode.
type SemanticDiff struct {
	Code   string
	Detail string
}

// compareSnapshots compares two decodes of the same logical data set.
//
// Ordering of hashes and set membership is normalised because Redis stores
// these unordered. Nothing else is relaxed: DB index, raw key bytes,
// millisecond expiration precision, stream ids, group/consumer/PEL ownership,
// function libraries and dialect metadata are all compared.
func compareSnapshots(before, after *snapshot) []SemanticDiff {
	var diffs []SemanticDiff

	if before.header.magic != after.header.magic || before.header.version != after.header.version {
		diffs = append(diffs, SemanticDiff{
			Code: LossRDBVersion,
			Detail: fmt.Sprintf("rdb dialect/version changed: %s -> %s",
				before.header.String(), after.header.String()),
		})
	}

	// AUX fields compared as an ordered list of key/value pairs (RDB aux
	// fields are normally unique, but compare positionally and by content).
	diffs = append(diffs, compareAux(before.aux, after.aux)...)

	// Function libraries compared byte-for-byte.
	diffs = append(diffs, compareFunctions(before.functions, after.functions)...)

	// DB size hints are metadata but cheap to compare exactly.
	diffs = append(diffs, compareDBSizes(before.dbsizes, after.dbsizes)...)

	beforeByKey := indexObjects(before.objects)
	afterByKey := indexObjects(after.objects)

	keySet := make(map[string]struct{}, len(beforeByKey)+len(afterByKey))
	for k := range beforeByKey {
		keySet[k] = struct{}{}
	}
	for k := range afterByKey {
		keySet[k] = struct{}{}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		bObj, bok := beforeByKey[key]
		aObj, aok := afterByKey[key]
		switch {
		case bok && !aok:
			diffs = append(diffs, SemanticDiff{
				Code:   "key-missing",
				Detail: fmt.Sprintf("key %q (db %d) lost after re-encode", key, bObj.GetDBIndex()),
			})
			continue
		case !bok && aok:
			diffs = append(diffs, SemanticDiff{
				Code:   "key-added",
				Detail: fmt.Sprintf("unexpected key %q (db %d) after re-encode", key, aObj.GetDBIndex()),
			})
			continue
		}
		diffs = append(diffs, compareObject(key, bObj, aObj)...)
	}
	return dedupeDiffs(diffs)
}

// objectKey keeps the raw key bytes; Go strings preserve arbitrary bytes.
type objectKey struct {
	db  int
	key string
}

func indexObjects(objs []model.RedisObject) map[objectKey]model.RedisObject {
	m := make(map[objectKey]model.RedisObject, len(objs))
	for _, obj := range objs {
		m[objectKey{db: obj.GetDBIndex(), key: obj.GetKey()}] = obj
	}
	return m
}

func compareObject(key string, before, after model.RedisObject) []SemanticDiff {
	var diffs []SemanticDiff
	if before.GetDBIndex() != after.GetDBIndex() {
		diffs = append(diffs, SemanticDiff{
			Code:   "db-index",
			Detail: fmt.Sprintf("key %q db changed: %d -> %d", key, before.GetDBIndex(), after.GetDBIndex()),
		})
	}
	if before.GetType() != after.GetType() {
		diffs = append(diffs, SemanticDiff{
			Code:   "type",
			Detail: fmt.Sprintf("key %q type changed: %s -> %s", key, before.GetType(), after.GetType()),
		})
	}
	compareExpiration(&diffs, key, before.GetExpiration(), after.GetExpiration())
	compareEviction(&diffs, key, before, after)

	switch b := before.(type) {
	case *model.StringObject:
		a, _ := after.(*model.StringObject)
		diffs = append(diffs, compareBytes(key, "string value", b.Value, a.Value)...)
	case *model.ListObject:
		a, _ := after.(*model.ListObject)
		diffs = append(diffs, compareBytesList(key, "list", b.Values, a.Values)...)
	case *model.SetObject:
		a, _ := after.(*model.SetObject)
		diffs = append(diffs, compareSet(key, b.Members, a.Members)...)
	case *model.HashObject:
		a, _ := after.(*model.HashObject)
		diffs = append(diffs, compareHash(key, b, a)...)
	case *model.ZSetObject:
		a, _ := after.(*model.ZSetObject)
		diffs = append(diffs, compareZSet(key, b.Entries, a.Entries)...)
	case *model.StreamObject:
		a, _ := after.(*model.StreamObject)
		diffs = append(diffs, compareStream(key, b, a)...)
	case *model.FunctionsObject:
		a, _ := after.(*model.FunctionsObject)
		diffs = append(diffs, compareString(key, "function payload", b.FunctionsLua, a.FunctionsLua)...)
	default:
		diffs = append(diffs, SemanticDiff{
			Code:   "unsupported-compare",
			Detail: fmt.Sprintf("key %q unsupported type pair %T vs %T", key, before, after),
		})
	}
	return diffs
}

func compareExpiration(diffs *[]SemanticDiff, key string, before, after *time.Time) {
	switch {
	case before == nil && after == nil:
		return
	case before == nil || after == nil:
		*diffs = append(*diffs, SemanticDiff{
			Code: "expiration",
			Detail: fmt.Sprintf("key %q expiration changed: %s -> %s",
				key, formatExp(before), formatExp(after)),
		})
	default:
		// Millisecond precision is the on-disk resolution; compare exactly.
		bMs := before.UnixNano() / int64(time.Millisecond)
		aMs := after.UnixNano() / int64(time.Millisecond)
		if bMs != aMs {
			*diffs = append(*diffs, SemanticDiff{
				Code:   "expiration",
				Detail: fmt.Sprintf("key %q expiration ms changed: %d -> %d", key, bMs, aMs),
			})
		}
	}
}

func formatExp(t *time.Time) string {
	if t == nil {
		return "<persist>"
	}
	return fmt.Sprintf("%d", t.UnixNano()/int64(time.Millisecond))
}

func compareEviction(diffs *[]SemanticDiff, key string, before, after model.RedisObject) {
	bInfo, bOK := before.(model.EvictionInfo)
	aInfo, aOK := after.(model.EvictionInfo)
	if bOK != aOK {
		return
	}
	if bInfo.GetIdleTime() != aInfo.GetIdleTime() {
		*diffs = append(*diffs, SemanticDiff{
			Code:   LossEvictionMeta,
			Detail: fmt.Sprintf("key %q LRU idle changed: %d -> %d", key, bInfo.GetIdleTime(), aInfo.GetIdleTime()),
		})
	}
	if bInfo.GetFreq() != aInfo.GetFreq() {
		*diffs = append(*diffs, SemanticDiff{
			Code:   LossEvictionMeta,
			Detail: fmt.Sprintf("key %q LFU freq changed: %d -> %d", key, bInfo.GetFreq(), aInfo.GetFreq()),
		})
	}
}

func compareBytes(key, what string, before, after []byte) []SemanticDiff {
	if string(before) == string(after) {
		return nil
	}
	return []SemanticDiff{{Code: "value", Detail: fmt.Sprintf("key %q %s mismatch", key, what)}}
}

func compareString(key, what, before, after string) []SemanticDiff {
	if before == after {
		return nil
	}
	return []SemanticDiff{{Code: "value", Detail: fmt.Sprintf("key %q %s mismatch", key, what)}}
}

// compareBytesList compares ordered (list) sequences element by element.
func compareBytesList(key, what string, before, after [][]byte) []SemanticDiff {
	if len(before) != len(after) {
		return []SemanticDiff{{Code: "length", Detail: fmt.Sprintf("key %q %s length %d -> %d", key, what, len(before), len(after))}}
	}
	for i := range before {
		if string(before[i]) != string(after[i]) {
			return []SemanticDiff{{Code: "value", Detail: fmt.Sprintf("key %q %s element %d mismatch", key, what, i)}}
		}
	}
	return nil
}

// compareSet normalises member order but compares the exact member multiset.
func compareSet(key string, before, after [][]byte) []SemanticDiff {
	bCount := countBytes(before)
	aCount := countBytes(after)
	if len(bCount) != len(aCount) {
		return []SemanticDiff{{Code: "set-members", Detail: fmt.Sprintf("key %q set member count %d -> %d", key, len(bCount), len(aCount))}}
	}
	var missing []string
	for member, n := range bCount {
		if aCount[member] != n {
			missing = append(missing, member)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return []SemanticDiff{{Code: "set-members", Detail: fmt.Sprintf("key %q set members differ: %s", key, strings.Join(missing, ", "))}}
}

func countBytes(values [][]byte) map[string]int {
	m := make(map[string]int, len(values))
	for _, v := range values {
		m[string(v)]++
	}
	return m
}

// compareHash normalises field order; field names and values keep raw bytes,
// field expirations keep millisecond precision.
func compareHash(key string, before, after *model.HashObject) []SemanticDiff {
	var diffs []SemanticDiff
	if len(before.Hash) != len(after.Hash) {
		diffs = append(diffs, SemanticDiff{
			Code:   "hash-size",
			Detail: fmt.Sprintf("key %q hash field count %d -> %d", key, len(before.Hash), len(after.Hash)),
		})
	}
	fields := make(map[string]struct{}, len(before.Hash))
	for f := range before.Hash {
		fields[f] = struct{}{}
	}
	for f := range after.Hash {
		fields[f] = struct{}{}
	}
	for field := range fields {
		bv, bok := before.Hash[field]
		av, aok := after.Hash[field]
		if !bok || !aok {
			diffs = append(diffs, SemanticDiff{Code: "hash-field", Detail: fmt.Sprintf("key %q field %q presence differs", key, field)})
			continue
		}
		if string(bv) != string(av) {
			diffs = append(diffs, SemanticDiff{Code: "hash-field", Detail: fmt.Sprintf("key %q field %q value differs", key, field)})
		}
		bExp, bHasExp := before.FieldExpirations[field]
		aExp, aHasExp := after.FieldExpirations[field]
		if bHasExp != aHasExp || bExp != aExp {
			diffs = append(diffs, SemanticDiff{
				Code:   LossHFE,
				Detail: fmt.Sprintf("key %q field %q expiration %d(present=%v) -> %d(present=%v)", key, field, bExp, bHasExp, aExp, aHasExp),
			})
		}
	}
	return diffs
}

// compareZSet compares member->score maps; score equality is exact float64.
func compareZSet(key string, before, after []*model.ZSetEntry) []SemanticDiff {
	if len(before) != len(after) {
		return []SemanticDiff{{Code: "zset-size", Detail: fmt.Sprintf("key %q zset size %d -> %d", key, len(before), len(after))}}
	}
	bm := make(map[string]float64, len(before))
	for _, e := range before {
		bm[e.Member] = e.Score
	}
	for _, e := range after {
		bs, ok := bm[e.Member]
		if !ok {
			return []SemanticDiff{{Code: "zset-member", Detail: fmt.Sprintf("key %q unexpected member %q", key, e.Member)}}
		}
		if bs != e.Score && !(math.IsNaN(bs) && math.IsNaN(e.Score)) {
			return []SemanticDiff{{Code: "zset-score", Detail: fmt.Sprintf("key %q member %q score %v -> %v", key, e.Member, bs, e.Score)}}
		}
	}
	return nil
}
