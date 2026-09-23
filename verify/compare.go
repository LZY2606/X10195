package main

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// check names used for independent per-aspect results
const (
	checkObjects    = "compare.objects"
	checkValue      = "compare.value"
	checkExpiration = "compare.expiration"
	checkEviction   = "compare.eviction"
	checkHFE        = "compare.hfe"
	checkStream     = "compare.stream"
	checkFunctions  = "compare.functions"
	checkModule     = "compare.module"
)

// compareObjects semantically compares the objects decoded from the original
// fixture (src) with the ones decoded from the re-encoded file (dst).
// Map and set orders are normalized, but db numbers, raw key bytes,
// expiration precision, stream ids, group/consumer/PEL ownership, function
// libraries and encoding-version-dependent fields are all verified.
func compareObjects(fixture string, src, dst []model.RedisObject) []finding {
	srcMap := indexObjects(src)
	dstMap := indexObjects(dst)
	var findings []finding
	newFinding := func(check, kind, code, format string, args ...interface{}) finding {
		return finding{
			Fixture: fixture,
			Stage:   stageCompare,
			Kind:    kind,
			Check:   check,
			Code:    code,
			Message: fmt.Sprintf(format, args...),
		}
	}
	ids := make([]string, 0, len(srcMap)+len(dstMap))
	seen := make(map[string]bool)
	for id := range srcMap {
		ids = append(ids, id)
		seen[id] = true
	}
	for id := range dstMap {
		if !seen[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		srcObjs := srcMap[id]
		dstObjs := dstMap[id]
		desc := describeIdentity(id)
		if len(srcObjs) == 0 {
			findings = append(findings, newFinding(checkObjects, kindError, "extra-object",
				"%s exists only in re-encoded output", desc))
			continue
		}
		if len(dstObjs) == 0 {
			switch srcObjs[0].(type) {
			case *model.FunctionsObject:
				findings = append(findings, newFinding(checkFunctions, kindExpectedLoss, "function-library",
					"%s dropped: encoder cannot represent function libraries", desc))
			case *model.ModuleTypeObject:
				findings = append(findings, newFinding(checkModule, kindExpectedLoss, "module-data",
					"%s dropped: encoder cannot represent module data", desc))
			default:
				findings = append(findings, newFinding(checkObjects, kindError, "missing-object",
					"%s missing after re-encode", desc))
			}
			continue
		}
		if len(srcObjs) != len(dstObjs) {
			findings = append(findings, newFinding(checkObjects, kindError, "object-count-mismatch",
				"%s occurs %d times before but %d times after re-encode", desc, len(srcObjs), len(dstObjs)))
			continue
		}
		srcSorted := sortObjects(srcObjs)
		dstSorted := sortObjects(dstObjs)
		for i := range srcSorted {
			findings = append(findings, comparePair(fixture, desc, srcSorted[i], dstSorted[i])...)
		}
	}
	return findings
}

// indexObjects groups objects by their semantic identity. DB size hints are
// excluded: they are recomputed by the encoder and carry no key semantics.
func indexObjects(objects []model.RedisObject) map[string][]model.RedisObject {
	index := make(map[string][]model.RedisObject)
	for _, obj := range objects {
		if _, ok := obj.(*model.DBSizeObject); ok {
			continue
		}
		id := identityOf(obj)
		index[id] = append(index[id], obj)
	}
	return index
}

// identityOf builds the semantic identity of an object from its db number,
// type and raw key bytes.
func identityOf(obj model.RedisObject) string {
	db := obj.GetDBIndex()
	switch obj.(type) {
	case *model.AuxObject, *model.FunctionsObject:
		db = -1 // global metadata is not bound to a db
	}
	return strconv.Itoa(db) + "\x00" + obj.GetType() + "\x00" + obj.GetKey()
}

func describeIdentity(id string) string {
	parts := bytes.Split([]byte(id), []byte{0})
	if len(parts) != 3 {
		return strconv.Quote(id)
	}
	return fmt.Sprintf("db=%s type=%s key=%q", parts[0], parts[1], parts[2])
}

// sortObjects returns a copy of objects sorted by a deterministic dump so
// that multi-map entries pair up deterministically.
func sortObjects(objects []model.RedisObject) []model.RedisObject {
	sorted := make([]model.RedisObject, len(objects))
	copy(sorted, objects)
	sort.Slice(sorted, func(i, j int) bool {
		return fmt.Sprintf("%#v", sorted[i]) < fmt.Sprintf("%#v", sorted[j])
	})
	return sorted
}

// comparePair compares two objects that share the same semantic identity.
func comparePair(fixture, desc string, src, dst model.RedisObject) []finding {
	var findings []finding
	add := func(check, kind, code, format string, args ...interface{}) {
		findings = append(findings, finding{
			Fixture: fixture,
			Stage:   stageCompare,
			Kind:    kind,
			Check:   check,
			Code:    code,
			Message: fmt.Sprintf(format, args...),
		})
	}
	// expiration, compared at millisecond precision
	srcExp, dstExp := src.GetExpiration(), dst.GetExpiration()
	switch {
	case srcExp == nil && dstExp != nil:
		add(checkExpiration, kindError, "expiration-mismatch",
			"%s gained an unexpected expiration %v", desc, dstExp)
	case srcExp != nil && dstExp == nil:
		add(checkExpiration, kindError, "expiration-mismatch",
			"%s lost expiration %v", desc, srcExp)
	case srcExp != nil && dstExp != nil && srcExp.UnixMilli() != dstExp.UnixMilli():
		add(checkExpiration, kindError, "expiration-mismatch",
			"%s expiration changed from %dms to %dms", desc, srcExp.UnixMilli(), dstExp.UnixMilli())
	}
	// LRU/LFU eviction metadata
	findings = append(findings, compareEviction(fixture, desc, src, dst)...)
	// type-specific payload
	switch srcObj := src.(type) {
	case *model.StringObject:
		dstObj := dst.(*model.StringObject)
		if !bytes.Equal(srcObj.Value, dstObj.Value) {
			add(checkValue, kindError, "value-mismatch",
				"%s string value changed from %q to %q", desc, srcObj.Value, dstObj.Value)
		}
	case *model.ListObject:
		dstObj := dst.(*model.ListObject)
		if len(srcObj.Values) != len(dstObj.Values) {
			add(checkValue, kindError, "value-mismatch",
				"%s list length changed from %d to %d", desc, len(srcObj.Values), len(dstObj.Values))
		} else {
			for i := range srcObj.Values {
				if !bytes.Equal(srcObj.Values[i], dstObj.Values[i]) {
					add(checkValue, kindError, "value-mismatch",
						"%s list element %d changed from %q to %q", desc, i, srcObj.Values[i], dstObj.Values[i])
					break
				}
			}
		}
	case *model.SetObject:
		dstObj := dst.(*model.SetObject)
		if !sortedBytesEqual(srcObj.Members, dstObj.Members) {
			add(checkValue, kindError, "value-mismatch",
				"%s set members changed from %s to %s", desc,
				dumpBytesSorted(srcObj.Members), dumpBytesSorted(dstObj.Members))
		}
	case *model.HashObject:
		dstObj := dst.(*model.HashObject)
		findings = append(findings, compareHash(fixture, desc, srcObj, dstObj)...)
	case *model.ZSetObject:
		dstObj := dst.(*model.ZSetObject)
		findings = append(findings, compareZSet(fixture, desc, srcObj, dstObj)...)
	case *model.StreamObject:
		dstObj := dst.(*model.StreamObject)
		findings = append(findings, compareStream(fixture, desc, srcObj, dstObj)...)
	case *model.AuxObject:
		dstObj := dst.(*model.AuxObject)
		if srcObj.Value != dstObj.Value {
			add(checkValue, kindError, "aux-mismatch",
				"%s aux value changed from %q to %q", desc, srcObj.Value, dstObj.Value)
		}
	case *model.FunctionsObject:
		dstObj := dst.(*model.FunctionsObject)
		if srcObj.FunctionsLua != dstObj.FunctionsLua {
			add(checkFunctions, kindError, "function-mismatch",
				"%s function library changed", desc)
		}
	case *model.ModuleTypeObject:
		dstObj := dst.(*model.ModuleTypeObject)
		if !reflect.DeepEqual(srcObj.Value, dstObj.Value) {
			add(checkModule, kindError, "module-mismatch",
				"%s module value changed", desc)
		}
	}
	return findings
}

// compareEviction verifies LRU idle time and LFU frequency metadata. The
// encoder cannot write these opcodes, so a loss is classified as expected.
func compareEviction(fixture, desc string, src, dst model.RedisObject) []finding {
	var findings []finding
	srcInfo, srcOK := src.(model.EvictionInfo)
	dstInfo, dstOK := dst.(model.EvictionInfo)
	if !srcOK {
		return nil
	}
	srcIdle, srcFreq := srcInfo.GetIdleTime(), srcInfo.GetFreq()
	var dstIdle, dstFreq int64 = -1, -1
	if dstOK {
		dstIdle, dstFreq = dstInfo.GetIdleTime(), dstInfo.GetFreq()
	}
	add := func(kind, code, format string, args ...interface{}) {
		findings = append(findings, finding{
			Fixture: fixture,
			Stage:   stageCompare,
			Kind:    kind,
			Check:   checkEviction,
			Code:    code,
			Message: fmt.Sprintf(format, args...),
		})
	}
	if srcIdle >= 0 && dstIdle < 0 {
		add(kindExpectedLoss, "lru-lfu-metadata",
			"%s lost LRU idle time %d: encoder cannot write idle opcodes", desc, srcIdle)
	} else if srcIdle >= 0 && srcIdle != dstIdle {
		add(kindError, "eviction-mismatch",
			"%s LRU idle time changed from %d to %d", desc, srcIdle, dstIdle)
	}
	if srcFreq >= 0 && dstFreq < 0 {
		add(kindExpectedLoss, "lru-lfu-metadata",
			"%s lost LFU frequency %d: encoder cannot write freq opcodes", desc, srcFreq)
	} else if srcFreq >= 0 && srcFreq != dstFreq {
		add(kindError, "eviction-mismatch",
			"%s LFU frequency changed from %d to %d", desc, srcFreq, dstFreq)
	}
	return findings
}

// compareHash compares hash fields order-free and field expirations (HFE)
// independently.
func compareHash(fixture, desc string, src, dst *model.HashObject) []finding {
	var findings []finding
	add := func(check, kind, code, format string, args ...interface{}) {
		findings = append(findings, finding{
			Fixture: fixture, Stage: stageCompare, Kind: kind, Check: check, Code: code,
			Message: fmt.Sprintf(format, args...),
		})
	}
	if len(src.Hash) != len(dst.Hash) {
		add(checkValue, kindError, "value-mismatch",
			"%s hash field count changed from %d to %d", desc, len(src.Hash), len(dst.Hash))
	} else {
		fields := make([]string, 0, len(src.Hash))
		for field := range src.Hash {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			dstVal, ok := dst.Hash[field]
			if !ok {
				add(checkValue, kindError, "value-mismatch",
					"%s hash field %q missing after re-encode", desc, field)
				continue
			}
			if !bytes.Equal(src.Hash[field], dstVal) {
				add(checkValue, kindError, "value-mismatch",
					"%s hash field %q changed from %q to %q", desc, field, src.Hash[field], dstVal)
			}
		}
	}
	// field-level expirations (HFE) are compared independently
	srcExp := src.FieldExpirations
	dstExp := dst.FieldExpirations
	if len(srcExp) != len(dstExp) {
		add(checkHFE, kindError, "hfe-mismatch",
			"%s field expiration count changed from %d to %d", desc, len(srcExp), len(dstExp))
	} else {
		fields := make([]string, 0, len(srcExp))
		for field := range srcExp {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			if srcExp[field] != dstExp[field] {
				add(checkHFE, kindError, "hfe-mismatch",
					"%s field %q expiration changed from %d to %d", desc, field, srcExp[field], dstExp[field])
			}
		}
	}
	return findings
}

// compareZSet compares sorted sets order-free with exact score equality.
func compareZSet(fixture, desc string, src, dst *model.ZSetObject) []finding {
	var findings []finding
	add := func(format string, args ...interface{}) {
		findings = append(findings, finding{
			Fixture: fixture, Stage: stageCompare, Kind: kindError, Check: checkValue, Code: "value-mismatch",
			Message: fmt.Sprintf(format, args...),
		})
	}
	srcMap := make(map[string]float64, len(src.Entries))
	for _, e := range src.Entries {
		srcMap[e.Member] = e.Score
	}
	dstMap := make(map[string]float64, len(dst.Entries))
	for _, e := range dst.Entries {
		dstMap[e.Member] = e.Score
	}
	if len(srcMap) != len(src.Entries) || len(dstMap) != len(dst.Entries) {
		add("%s zset contains duplicate members (src=%d/%d dst=%d/%d)", desc,
			len(srcMap), len(src.Entries), len(dstMap), len(dst.Entries))
	}
	if len(srcMap) != len(dstMap) {
		add("%s zset member count changed from %d to %d", desc, len(srcMap), len(dstMap))
		return findings
	}
	members := make([]string, 0, len(srcMap))
	for member := range srcMap {
		members = append(members, member)
	}
	sort.Strings(members)
	for _, member := range members {
		dstScore, ok := dstMap[member]
		if !ok {
			add("%s zset member %q missing after re-encode", desc, member)
			continue
		}
		if srcMap[member] != dstScore {
			add("%s zset member %q score changed from %v to %v", desc, member, srcMap[member], dstScore)
		}
	}
	return findings
}

func sortedBytesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	as := sortBytesCopy(a)
	bs := sortBytesCopy(b)
	for i := range as {
		if !bytes.Equal(as[i], bs[i]) {
			return false
		}
	}
	return true
}

func sortBytesCopy(values [][]byte) [][]byte {
	sorted := make([][]byte, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i], sorted[j]) < 0 })
	return sorted
}

func dumpBytesSorted(values [][]byte) string {
	sorted := sortBytesCopy(values)
	buf := bytes.NewBufferString("[")
	for i, v := range sorted {
		if i > 0 {
			buf.WriteString(" ")
		}
		fmt.Fprintf(buf, "%q", v)
	}
	buf.WriteString("]")
	return buf.String()
}
