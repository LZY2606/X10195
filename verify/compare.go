package verify

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// lossRecord is a structured, expected (or tolerated) difference between
// the original fixture and its re-encoded form. Anything the encoder
// cannot represent losslessly must surface here instead of being
// silently dropped.
type lossRecord struct {
	// Aspect classifies the loss: functions, lru-lfu, encoding-change,
	// empty-db, aux-metadata.
	Aspect string `json:"aspect"`
	// Key is the redis key the loss belongs to, empty for file level losses.
	Key string `json:"key,omitempty"`
	// Detail is a human readable explanation.
	Detail string `json:"detail"`
}

// comparison holds the outcome of comparing two decodes of the same data.
type comparison struct {
	diffs   []string
	changes []lossRecord
}

func (c *comparison) diff(format string, args ...interface{}) {
	c.diffs = append(c.diffs, fmt.Sprintf(format, args...))
}

func (c *comparison) change(aspect, key, format string, args ...interface{}) {
	c.changes = append(c.changes, lossRecord{
		Aspect: aspect,
		Key:    key,
		Detail: fmt.Sprintf(format, args...),
	})
}

// isDataObject reports whether the object carries key data that the
// encoder is expected to persist.
func isDataObject(obj model.RedisObject) bool {
	switch obj.GetType() {
	case model.StringType, model.ListType, model.SetType,
		model.HashType, model.ZSetType, model.StreamType:
		return true
	}
	return false
}

// dataObjects filters a decoded stream down to key-value objects.
func dataObjects(objs []model.RedisObject) []model.RedisObject {
	var out []model.RedisObject
	for _, obj := range objs {
		if isDataObject(obj) {
			out = append(out, obj)
		}
	}
	return out
}

// compareObjects semantically compares the originally decoded objects
// with the objects decoded from the re-encoded file. Map and set order
// is normalized; db index, raw key bytes, expiration, stream metadata,
// group/consumer/PEL ownership and stream encoding version are all
// compared exactly.
func compareObjects(orig, redec []model.RedisObject) *comparison {
	c := &comparison{}
	origMap, origDups := indexByKey(orig)
	redecMap, redecDups := indexByKey(redec)
	for _, d := range origDups {
		c.diff("original: %s", d)
	}
	for _, d := range redecDups {
		c.diff("re-encoded: %s", d)
	}
	for id, a := range origMap {
		b, ok := redecMap[id]
		if !ok {
			c.diff("db=%d key=%q missing after re-encode", a.GetDBIndex(), a.GetKey())
			continue
		}
		compareObject(c, a, b)
	}
	for id, b := range redecMap {
		if _, ok := origMap[id]; !ok {
			c.diff("db=%d key=%q appeared after re-encode", b.GetDBIndex(), b.GetKey())
		}
	}
	return c
}

func objectID(obj model.RedisObject) string {
	return strconv.Itoa(obj.GetDBIndex()) + "\x00" + obj.GetKey()
}

func indexByKey(objs []model.RedisObject) (map[string]model.RedisObject, []string) {
	m := make(map[string]model.RedisObject, len(objs))
	var dups []string
	for _, obj := range objs {
		id := objectID(obj)
		if _, ok := m[id]; ok {
			dups = append(dups, fmt.Sprintf("duplicate object db=%d key=%q", obj.GetDBIndex(), obj.GetKey()))
			continue
		}
		m[id] = obj
	}
	return m, dups
}

func compareObject(c *comparison, a, b model.RedisObject) {
	where := fmt.Sprintf("db=%d key=%q", a.GetDBIndex(), a.GetKey())
	if a.GetType() != b.GetType() {
		c.diff("%s: type changed %s -> %s", where, a.GetType(), b.GetType())
		return
	}
	aExp, bExp := a.GetExpiration(), b.GetExpiration()
	if (aExp == nil) != (bExp == nil) {
		c.diff("%s: expiration presence changed", where)
	} else if aExp != nil && aExp.UnixMilli() != bExp.UnixMilli() {
		c.diff("%s: expiration changed %d -> %d (ms)", where, aExp.UnixMilli(), bExp.UnixMilli())
	}
	// Encoding changes are recorded as structured expected-loss records,
	// except the stream version which is semantic and compared exactly.
	if a.GetEncoding() != b.GetEncoding() {
		c.change("encoding-change", a.GetKey(),
			"encoding changed %s -> %s", a.GetEncoding(), b.GetEncoding())
	}
	switch a.GetType() {
	case model.StringType:
		ao, bo := a.(*model.StringObject), b.(*model.StringObject)
		if !bytes.Equal(ao.Value, bo.Value) {
			c.diff("%s: string value changed", where)
		}
	case model.ListType:
		ao, bo := a.(*model.ListObject), b.(*model.ListObject)
		if len(ao.Values) != len(bo.Values) {
			c.diff("%s: list length changed %d -> %d", where, len(ao.Values), len(bo.Values))
			return
		}
		for i := range ao.Values {
			if !bytes.Equal(ao.Values[i], bo.Values[i]) {
				c.diff("%s: list element %d changed", where, i)
			}
		}
	case model.SetType:
		ao, bo := a.(*model.SetObject), b.(*model.SetObject)
		if !sortedBytesEqual(ao.Members, bo.Members) {
			c.diff("%s: set members changed", where)
		}
	case model.HashType:
		ao, bo := a.(*model.HashObject), b.(*model.HashObject)
		compareHash(c, where, ao, bo)
	case model.ZSetType:
		ao, bo := a.(*model.ZSetObject), b.(*model.ZSetObject)
		compareZSet(c, where, ao, bo)
	case model.StreamType:
		ao, bo := a.(*model.StreamObject), b.(*model.StreamObject)
		compareStream(c, where, ao, bo)
	}
}

// sortedBytesEqual compares two byte-slice collections as multisets,
// normalizing order.
func sortedBytesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	as := make([][]byte, len(a))
	bs := make([][]byte, len(b))
	copy(as, a)
	copy(bs, b)
	sort.Slice(as, func(i, j int) bool { return bytes.Compare(as[i], as[j]) < 0 })
	sort.Slice(bs, func(i, j int) bool { return bytes.Compare(bs[i], bs[j]) < 0 })
	for i := range as {
		if !bytes.Equal(as[i], bs[i]) {
			return false
		}
	}
	return true
}

func compareHash(c *comparison, where string, a, b *model.HashObject) {
	if len(a.Hash) != len(b.Hash) {
		c.diff("%s: hash field count changed %d -> %d", where, len(a.Hash), len(b.Hash))
		return
	}
	for field, av := range a.Hash {
		bv, ok := b.Hash[field]
		if !ok {
			c.diff("%s: hash field %q missing after re-encode", where, field)
			continue
		}
		if !bytes.Equal(av, bv) {
			c.diff("%s: hash field %q value changed", where, field)
		}
	}
	// field level expiration (HFE) is semantic and must round-trip exactly
	aExp, bExp := a.FieldExpirations, b.FieldExpirations
	if len(aExp) != len(bExp) {
		c.diff("%s: hash field-expiration count changed %d -> %d", where, len(aExp), len(bExp))
		return
	}
	for field, av := range aExp {
		bv, ok := bExp[field]
		if !ok {
			c.diff("%s: hash field-expiration for %q missing after re-encode", where, field)
			continue
		}
		if av != bv {
			c.diff("%s: hash field-expiration for %q changed %d -> %d", where, field, av, bv)
		}
	}
}

func compareZSet(c *comparison, where string, a, b *model.ZSetObject) {
	if len(a.Entries) != len(b.Entries) {
		c.diff("%s: zset entry count changed %d -> %d", where, len(a.Entries), len(b.Entries))
		return
	}
	bScores := make(map[string]uint64, len(b.Entries))
	for _, e := range b.Entries {
		bScores[e.Member] = math.Float64bits(e.Score)
	}
	for _, e := range a.Entries {
		bits, ok := bScores[e.Member]
		if !ok {
			c.diff("%s: zset member %q missing after re-encode", where, e.Member)
			continue
		}
		if bits != math.Float64bits(e.Score) {
			c.diff("%s: zset member %q score changed %v -> %v", where, e.Member, e.Score, math.Float64frombits(bits))
		}
	}
}

func streamIDEqual(where, field string, a, b *model.StreamId, c *comparison) {
	if (a == nil) != (b == nil) {
		c.diff("%s: stream %s presence changed", where, field)
		return
	}
	if a != nil && (a.Ms != b.Ms || a.Sequence != b.Sequence) {
		c.diff("%s: stream %s changed %d-%d -> %d-%d", where, field, a.Ms, a.Sequence, b.Ms, b.Sequence)
	}
}

func compareStream(c *comparison, where string, a, b *model.StreamObject) {
	// the stream encoding version (v1/v2/v3) is semantic: it decides which
	// metadata fields exist, so a version change is a real difference
	if a.Version != b.Version {
		c.diff("%s: stream version changed %d -> %d", where, a.Version, b.Version)
	}
	if a.Length != b.Length {
		c.diff("%s: stream length changed %d -> %d", where, a.Length, b.Length)
	}
	streamIDEqual(where, "lastId", a.LastId, b.LastId, c)
	streamIDEqual(where, "firstId", a.FirstId, b.FirstId, c)
	streamIDEqual(where, "maxDeletedId", a.MaxDeletedId, b.MaxDeletedId, c)
	if a.AddedEntriesCount != b.AddedEntriesCount {
		c.diff("%s: stream addedEntriesCount changed %d -> %d", where, a.AddedEntriesCount, b.AddedEntriesCount)
	}
	if len(a.Entries) != len(b.Entries) {
		c.diff("%s: stream entry count changed %d -> %d", where, len(a.Entries), len(b.Entries))
	} else {
		for i := range a.Entries {
			compareStreamEntry(c, fmt.Sprintf("%s entry[%d]", where, i), a.Entries[i], b.Entries[i])
		}
	}
	if len(a.Groups) != len(b.Groups) {
		c.diff("%s: stream group count changed %d -> %d", where, len(a.Groups), len(b.Groups))
		return
	}
	for i := range a.Groups {
		compareStreamGroup(c, fmt.Sprintf("%s group[%d]", where, i), a.Groups[i], b.Groups[i])
	}
}

func compareStreamEntry(c *comparison, where string, a, b *model.StreamEntry) {
	streamIDEqual(where, "firstMsgId", a.FirstMsgId, b.FirstMsgId, c)
	if len(a.Fields) != len(b.Fields) {
		c.diff("%s: master field count changed %d -> %d", where, len(a.Fields), len(b.Fields))
	} else {
		for i := range a.Fields {
			if a.Fields[i] != b.Fields[i] {
				c.diff("%s: master field %d changed %q -> %q", where, i, a.Fields[i], b.Fields[i])
			}
		}
	}
	if len(a.Msgs) != len(b.Msgs) {
		c.diff("%s: message count changed %d -> %d", where, len(a.Msgs), len(b.Msgs))
		return
	}
	for i := range a.Msgs {
		am, bm := a.Msgs[i], b.Msgs[i]
		msgWhere := fmt.Sprintf("%s msg[%d]", where, i)
		streamIDEqual(msgWhere, "id", am.Id, bm.Id, c)
		if am.Deleted != bm.Deleted {
			c.diff("%s: deleted flag changed %v -> %v", msgWhere, am.Deleted, bm.Deleted)
		}
		if len(am.Fields) != len(bm.Fields) {
			c.diff("%s: field count changed %d -> %d", msgWhere, len(am.Fields), len(bm.Fields))
			continue
		}
		for k, av := range am.Fields {
			bv, ok := bm.Fields[k]
			if !ok {
				c.diff("%s: field %q missing after re-encode", msgWhere, k)
				continue
			}
			if av != bv {
				c.diff("%s: field %q changed %q -> %q", msgWhere, k, av, bv)
			}
		}
	}
}

func compareStreamGroup(c *comparison, where string, a, b *model.StreamGroup) {
	if a.Name != b.Name {
		c.diff("%s: group name changed %q -> %q", where, a.Name, b.Name)
	}
	streamIDEqual(where, "group lastId", a.LastId, b.LastId, c)
	if a.EntriesRead != b.EntriesRead {
		c.diff("%s: group entriesRead changed %d -> %d", where, a.EntriesRead, b.EntriesRead)
	}
	// group PEL: ownership of pending entries at group level
	if len(a.Pending) != len(b.Pending) {
		c.diff("%s: group PEL size changed %d -> %d", where, len(a.Pending), len(b.Pending))
	} else {
		for i := range a.Pending {
			ap, bp := a.Pending[i], b.Pending[i]
			streamIDEqual(where, "group PEL id", ap.Id, bp.Id, c)
			if ap.DeliveryTime != bp.DeliveryTime {
				c.diff("%s: group PEL deliveryTime changed %d -> %d", where, ap.DeliveryTime, bp.DeliveryTime)
			}
			if ap.DeliveryCount != bp.DeliveryCount {
				c.diff("%s: group PEL deliveryCount changed %d -> %d", where, ap.DeliveryCount, bp.DeliveryCount)
			}
		}
	}
	// consumers and their own PELs
	if len(a.Consumers) != len(b.Consumers) {
		c.diff("%s: consumer count changed %d -> %d", where, len(a.Consumers), len(b.Consumers))
		return
	}
	for i := range a.Consumers {
		ac, bc := a.Consumers[i], b.Consumers[i]
		cWhere := fmt.Sprintf("%s consumer[%d]", where, i)
		if ac.Name != bc.Name {
			c.diff("%s: consumer name changed %q -> %q", cWhere, ac.Name, bc.Name)
		}
		if ac.SeenTime != bc.SeenTime {
			c.diff("%s: consumer seenTime changed %d -> %d", cWhere, ac.SeenTime, bc.SeenTime)
		}
		if ac.ActiveTime != bc.ActiveTime {
			c.diff("%s: consumer activeTime changed %d -> %d", cWhere, ac.ActiveTime, bc.ActiveTime)
		}
		if len(ac.Pending) != len(bc.Pending) {
			c.diff("%s: consumer PEL size changed %d -> %d", cWhere, len(ac.Pending), len(bc.Pending))
			continue
		}
		for j := range ac.Pending {
			streamIDEqual(cWhere, "consumer PEL id", ac.Pending[j], bc.Pending[j], c)
		}
	}
}
