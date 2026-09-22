package verify

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/model"
)

// objectID identifies one decoded object. Aux fields and function
// libraries do not belong to a logical db, they use db -1.
type objectID struct {
	db  int
	typ string
	key string
}

// canonObj is the normalized semantic form of one object. Map and set
// member order is normalized away; db index, raw key bytes, expiration at
// millisecond precision, stream ids, group/consumer/PEL ownership,
// function libraries and encoding are all preserved.
type canonObj struct {
	encoding    string
	hasExpiry   bool
	expireMs    int64
	hasEviction bool
	payload     string
}

// compareDecoded semantically compares two decodes of the same logical
// data. Known encoder limitations become structured expected-loss records,
// anything else is a blocker.
func compareDecoded(rel string, before, after []model.RedisObject) ([]Loss, []Blocker) {
	beforeMap, dupBlockers := indexObjects(rel, before)
	afterMap, dupBlockers2 := indexObjects(rel, after)
	blockers := append(dupBlockers, dupBlockers2...)

	ids := make(map[objectID]bool)
	for id := range beforeMap {
		ids[id] = true
	}
	for id := range afterMap {
		ids[id] = true
	}
	sorted := make([]objectID, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].db != sorted[j].db {
			return sorted[i].db < sorted[j].db
		}
		if sorted[i].typ != sorted[j].typ {
			return sorted[i].typ < sorted[j].typ
		}
		return sorted[i].key < sorted[j].key
	})

	lossAgg := make(map[string]*Loss)
	addLoss := func(kind, object, detail, reason string) {
		key := kind + "|" + detail
		if l, ok := lossAgg[key]; ok {
			l.Count++
			return
		}
		lossAgg[key] = &Loss{
			Fixture: rel,
			Phase:   PhaseCompare,
			Kind:    kind,
			Object:  object,
			Detail:  detail,
			Reason:  reason,
			Count:   1,
		}
	}

	for _, id := range sorted {
		b, inBefore := beforeMap[id]
		a, inAfter := afterMap[id]
		label := fmt.Sprintf("db=%d type=%s key=%q", id.db, id.typ, id.key)
		switch {
		case !inAfter:
			if id.typ == model.FunctionsType {
				addLoss("functions", label, "function library dropped",
					"encoder has no API to write function libraries")
			} else {
				blockers = append(blockers, Blocker{
					Fixture: rel,
					Phase:   PhaseCompare,
					Detail:  fmt.Sprintf("object lost after re-encode: %s", label),
				})
			}
			continue
		case !inBefore:
			blockers = append(blockers, Blocker{
				Fixture: rel,
				Phase:   PhaseCompare,
				Detail:  fmt.Sprintf("unexpected object after re-encode: %s", label),
			})
			continue
		}
		if b.hasEviction {
			addLoss("eviction-metadata", label, "LRU/LFU metadata dropped",
				"encoder does not write RDB_OPCODE_IDLE/RDB_OPCODE_FREQ")
		}
		if b.encoding != a.encoding {
			addLoss("encoding", label,
				fmt.Sprintf("encoding changed: %s -> %s", b.encoding, a.encoding),
				"encoder re-encodes values with its own canonical encoding")
		}
		if b.hasExpiry != a.hasExpiry || b.expireMs != a.expireMs {
			blockers = append(blockers, Blocker{
				Fixture: rel,
				Phase:   PhaseCompare,
				Detail: fmt.Sprintf("expiration mismatch at ms precision for %s: (%v,%d) vs (%v,%d)",
					label, b.hasExpiry, b.expireMs, a.hasExpiry, a.expireMs),
			})
		}
		if b.payload != a.payload {
			blockers = append(blockers, Blocker{
				Fixture: rel,
				Phase:   PhaseCompare,
				Detail:  fmt.Sprintf("semantic payload mismatch for %s", label),
			})
		}
	}

	losses := make([]Loss, 0, len(lossAgg))
	for _, l := range lossAgg {
		losses = append(losses, *l)
	}
	return losses, blockers
}

func indexObjects(rel string, objects []model.RedisObject) (map[objectID]canonObj, []Blocker) {
	m := make(map[objectID]canonObj)
	var blockers []Blocker
	for _, o := range objects {
		if _, ok := o.(*model.DBSizeObject); ok {
			// resize hints are recomputed by the encoder, they are not data
			continue
		}
		id := objectID{db: o.GetDBIndex(), typ: o.GetType(), key: o.GetKey()}
		switch o.(type) {
		case *model.AuxObject, *model.FunctionsObject:
			id.db = -1
		}
		if _, dup := m[id]; dup {
			blockers = append(blockers, Blocker{
				Fixture: rel,
				Phase:   PhaseCompare,
				Detail:  fmt.Sprintf("duplicate object identity: db=%d type=%s key=%q", id.db, id.typ, id.key),
			})
			continue
		}
		m[id] = canonicalOf(o)
	}
	return m, blockers
}

func canonicalOf(o model.RedisObject) canonObj {
	c := canonObj{encoding: o.GetEncoding()}
	if exp := o.GetExpiration(); exp != nil {
		c.hasExpiry = true
		c.expireMs = exp.UnixMilli()
	}
	if ev, ok := o.(model.EvictionInfo); ok && (ev.GetIdleTime() >= 0 || ev.GetFreq() >= 0) {
		c.hasEviction = true
	}
	var b strings.Builder
	switch obj := o.(type) {
	case *model.StringObject:
		b.WriteString("string;")
		rawBytes(&b, obj.Value)
	case *model.ListObject:
		b.WriteString("list;")
		rawList(&b, obj.Values)
	case *model.SetObject:
		b.WriteString("set;")
		members := make([]string, len(obj.Members))
		for i, m := range obj.Members {
			members[i] = string(m)
		}
		sort.Strings(members)
		for _, m := range members {
			rawStr(&b, m)
		}
	case *model.HashObject:
		b.WriteString("hash;")
		fields := make([]string, 0, len(obj.Hash))
		for f := range obj.Hash {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		if obj.FieldExpirations != nil {
			b.WriteString("hfe;")
		}
		for _, f := range fields {
			rawStr(&b, f)
			rawBytes(&b, obj.Hash[f])
			if obj.FieldExpirations != nil {
				signedNum(&b, obj.FieldExpirations[f])
			}
		}
	case *model.ZSetObject:
		b.WriteString("zset;")
		entries := make([]*model.ZSetEntry, len(obj.Entries))
		copy(entries, obj.Entries)
		sort.Slice(entries, func(i, j int) bool { return entries[i].Member < entries[j].Member })
		for _, e := range entries {
			rawStr(&b, e.Member)
			num(&b, math.Float64bits(e.Score))
		}
	case *model.StreamObject:
		canonStream(&b, obj)
	case *model.AuxObject:
		b.WriteString("aux;")
		rawStr(&b, obj.Value)
	case *model.FunctionsObject:
		b.WriteString("functions;")
		rawStr(&b, obj.FunctionsLua)
	default:
		b.WriteString("unknown;")
	}
	c.payload = b.String()
	return c
}

func canonStream(b *strings.Builder, obj *model.StreamObject) {
	b.WriteString("stream;")
	num(b, uint64(obj.Version))
	num(b, obj.Length)
	streamID(b, obj.LastId)
	streamID(b, obj.FirstId)
	streamID(b, obj.MaxDeletedId)
	num(b, obj.AddedEntriesCount)
	num(b, uint64(len(obj.Entries)))
	for _, entry := range obj.Entries {
		streamID(b, entry.FirstMsgId)
		num(b, uint64(len(entry.Fields)))
		for _, f := range entry.Fields {
			rawStr(b, f)
		}
		num(b, uint64(len(entry.Msgs)))
		for _, msg := range entry.Msgs {
			streamID(b, msg.Id)
			if msg.Deleted {
				b.WriteString("1;")
			} else {
				b.WriteString("0;")
			}
			fields := make([]string, 0, len(msg.Fields))
			for f := range msg.Fields {
				fields = append(fields, f)
			}
			sort.Strings(fields)
			num(b, uint64(len(fields)))
			for _, f := range fields {
				rawStr(b, f)
				rawStr(b, msg.Fields[f])
			}
		}
	}
	groups := make([]*model.StreamGroup, len(obj.Groups))
	copy(groups, obj.Groups)
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
	num(b, uint64(len(groups)))
	for _, g := range groups {
		rawStr(b, g.Name)
		streamID(b, g.LastId)
		num(b, g.EntriesRead)
		pending := make([]*model.StreamNAck, len(g.Pending))
		copy(pending, g.Pending)
		sort.Slice(pending, func(i, j int) bool { return streamIDLess(pending[i].Id, pending[j].Id) })
		num(b, uint64(len(pending)))
		for _, p := range pending {
			streamID(b, p.Id)
			num(b, p.DeliveryTime)
			num(b, p.DeliveryCount)
		}
		consumers := make([]*model.StreamConsumer, len(g.Consumers))
		copy(consumers, g.Consumers)
		sort.Slice(consumers, func(i, j int) bool { return consumers[i].Name < consumers[j].Name })
		num(b, uint64(len(consumers)))
		for _, c := range consumers {
			rawStr(b, c.Name)
			num(b, c.SeenTime)
			num(b, c.ActiveTime)
			ids := make([]*model.StreamId, len(c.Pending))
			copy(ids, c.Pending)
			sort.Slice(ids, func(i, j int) bool { return streamIDLess(ids[i], ids[j]) })
			num(b, uint64(len(ids)))
			for _, id := range ids {
				streamID(b, id)
			}
		}
	}
}

func streamIDLess(a, b *model.StreamId) bool {
	if a == nil || b == nil {
		return b != nil
	}
	if a.Ms != b.Ms {
		return a.Ms < b.Ms
	}
	return a.Sequence < b.Sequence
}

func streamID(b *strings.Builder, id *model.StreamId) {
	if id == nil {
		b.WriteString("-;")
		return
	}
	num(b, id.Ms)
	num(b, id.Sequence)
}

func rawStr(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
	b.WriteByte(';')
}

func rawBytes(b *strings.Builder, s []byte) {
	rawStr(b, string(s))
}

func rawList(b *strings.Builder, values [][]byte) {
	num(b, uint64(len(values)))
	for _, v := range values {
		rawBytes(b, v)
	}
}

func num(b *strings.Builder, v uint64) {
	b.WriteString(strconv.FormatUint(v, 10))
	b.WriteByte(';')
}

func signedNum(b *strings.Builder, v int64) {
	b.WriteString(strconv.FormatInt(v, 10))
	b.WriteByte(';')
}
