package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/model"
)

// canonObject is the normalized, comparable form of a decoded object.
// Map and set member order is normalized away; db index, raw key bytes,
// expiration (millisecond precision), stream ids, group/consumer/PEL
// ownership, function library payload and encoding are all preserved.
type canonObject struct {
	db       int
	key      string
	typ      string
	encoding string
	expireMs int64 // -1 when persistent
	idle     int64 // -1 when absent
	freq     int64 // -1 when absent
	payload  string
}

func hexb(b []byte) string { return hex.EncodeToString(b) }

func streamIDString(id *model.StreamId) string {
	if id == nil {
		return "-"
	}
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Sequence, 10)
}

func canonicalize(obj model.RedisObject) canonObject {
	c := canonObject{
		db:       obj.GetDBIndex(),
		key:      obj.GetKey(),
		typ:      obj.GetType(),
		encoding: obj.GetEncoding(),
		expireMs: -1,
		idle:     -1,
		freq:     -1,
	}
	if exp := obj.GetExpiration(); exp != nil {
		c.expireMs = exp.UnixNano() / int64(1e6)
	}
	if ev, ok := obj.(model.EvictionInfo); ok {
		c.idle = ev.GetIdleTime()
		c.freq = ev.GetFreq()
	}
	switch o := obj.(type) {
	case *model.StringObject:
		c.payload = hexb(o.Value)
	case *model.ListObject:
		parts := make([]string, 0, len(o.Values))
		for _, v := range o.Values {
			parts = append(parts, hexb(v))
		}
		c.payload = strings.Join(parts, "|")
	case *model.SetObject:
		parts := make([]string, 0, len(o.Members))
		for _, v := range o.Members {
			parts = append(parts, hexb(v))
		}
		sort.Strings(parts) // set order is not significant
		c.payload = strings.Join(parts, "|")
	case *model.HashObject:
		parts := make([]string, 0, len(o.Hash))
		for field, value := range o.Hash {
			parts = append(parts, hexb([]byte(field))+"="+hexb(value))
		}
		sort.Strings(parts) // hash field order is not significant
		var b strings.Builder
		b.WriteString(strings.Join(parts, "|"))
		hfe := make([]string, 0, len(o.FieldExpirations))
		for field, expire := range o.FieldExpirations {
			if expire == 0 {
				continue // 0 means no TTL in every HFE encoding variant
			}
			hfe = append(hfe, hexb([]byte(field))+"@"+strconv.FormatInt(expire, 10))
		}
		if len(hfe) > 0 {
			sort.Strings(hfe)
			b.WriteString("#hfe:")
			b.WriteString(strings.Join(hfe, "|"))
		}
		c.payload = b.String()
	case *model.ZSetObject:
		parts := make([]string, 0, len(o.Entries))
		for _, e := range o.Entries {
			parts = append(parts, hexb([]byte(e.Member))+"="+
				strconv.FormatUint(math.Float64bits(e.Score), 16))
		}
		sort.Strings(parts) // zset iteration order is not significant
		c.payload = strings.Join(parts, "|")
	case *model.AuxObject:
		c.payload = o.Value
	case *model.DBSizeObject:
		c.payload = fmt.Sprintf("keys=%d,ttls=%d", o.KeyCount, o.TTLCount)
	case *model.FunctionsObject:
		c.payload = o.FunctionsLua
	case *model.StreamObject:
		c.payload = canonicalStream(o)
	default:
		// unknown objects (e.g. module types) compare by their JSON-free
		// type identity; they are expected to be blocked at encode stage
		c.payload = fmt.Sprintf("unsupported:%T", obj)
	}
	return c
}

func canonicalStream(o *model.StreamObject) string {
	var b strings.Builder
	fmt.Fprintf(&b, "v=%d;len=%d;last=%s;first=%s;maxdel=%s;added=%d;",
		o.Version, o.Length, streamIDString(o.LastId), streamIDString(o.FirstId),
		streamIDString(o.MaxDeletedId), o.AddedEntriesCount)
	// entries keep their order: stream entry order is significant
	for _, entry := range o.Entries {
		b.WriteString("entry{")
		if entry.FirstMsgId != nil {
			b.WriteString("first=" + streamIDString(entry.FirstMsgId) + ";")
		}
		fields := append([]string(nil), entry.Fields...)
		sort.Strings(fields)
		b.WriteString("fields=" + strings.Join(fields, ",") + ";")
		for _, msg := range entry.Msgs {
			pairs := make([]string, 0, len(msg.Fields))
			for k, v := range msg.Fields {
				pairs = append(pairs, hexb([]byte(k))+"="+hexb([]byte(v)))
			}
			sort.Strings(pairs)
			fmt.Fprintf(&b, "msg(%s,del=%v,%s);", streamIDString(msg.Id), msg.Deleted,
				strings.Join(pairs, ","))
		}
		b.WriteString("}")
	}
	groups := append([]*model.StreamGroup(nil), o.Groups...)
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
	for _, g := range groups {
		fmt.Fprintf(&b, "group(%s,last=%s,read=%d,", g.Name, streamIDString(g.LastId), g.EntriesRead)
		pending := make([]string, 0, len(g.Pending))
		for _, p := range g.Pending {
			pending = append(pending, fmt.Sprintf("%s@%d#%d",
				streamIDString(p.Id), p.DeliveryTime, p.DeliveryCount))
		}
		sort.Strings(pending)
		b.WriteString("pel=" + strings.Join(pending, ",") + ",")
		consumers := append([]*model.StreamConsumer(nil), g.Consumers...)
		sort.Slice(consumers, func(i, j int) bool { return consumers[i].Name < consumers[j].Name })
		for _, cons := range consumers {
			ids := make([]string, 0, len(cons.Pending))
			for _, id := range cons.Pending {
				ids = append(ids, streamIDString(id))
			}
			sort.Strings(ids)
			fmt.Fprintf(&b, "consumer(%s,seen=%d,active=%d,pel=%s);",
				cons.Name, cons.SeenTime, cons.ActiveTime, strings.Join(ids, ","))
		}
		b.WriteString(")")
	}
	return b.String()
}

// diff is one semantic difference between the two decodings
type diff struct {
	kind    string
	subject string
	detail  string
}

func (d diff) String() string {
	return fmt.Sprintf("%s %s: %s", d.kind, d.subject, d.detail)
}

func objectKey(c canonObject) string {
	return strconv.Itoa(c.db) + "\x00" + c.typ + "\x00" + c.key
}

func subjectOf(c canonObject) string {
	return fmt.Sprintf("db=%d type=%s key=%q", c.db, c.typ, c.key)
}

// compareDecodings semantically compares two decoded fixtures.
func compareDecodings(a, b *decodedFixture) []diff {
	groupA := groupObjects(a.objects)
	groupB := groupObjects(b.objects)
	var diffs []diff
	for key, listA := range groupA {
		listB, ok := groupB[key]
		if !ok {
			for _, ca := range listA {
				diffs = append(diffs, diff{
					kind:    "missing-object",
					subject: subjectOf(ca),
					detail:  "object exists in source but not in re-encoded rdb",
				})
			}
			continue
		}
		diffs = append(diffs, compareGroup(listA, listB)...)
	}
	for key, listB := range groupB {
		if _, ok := groupA[key]; !ok {
			for _, cb := range listB {
				diffs = append(diffs, diff{
					kind:    "extra-object",
					subject: subjectOf(cb),
					detail:  "object exists in re-encoded rdb but not in source",
				})
			}
		}
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].String() < diffs[j].String() })
	return diffs
}

func groupObjects(objects []model.RedisObject) map[string][]canonObject {
	groups := map[string][]canonObject{}
	for _, obj := range objects {
		c := canonicalize(obj)
		groups[objectKey(c)] = append(groups[objectKey(c)], c)
	}
	return groups
}

func compareGroup(listA, listB []canonObject) []diff {
	var diffs []diff
	if len(listA) != len(listB) {
		diffs = append(diffs, diff{
			kind:    "count",
			subject: subjectOf(listA[0]),
			detail:  fmt.Sprintf("source has %d objects, re-encoded has %d", len(listA), len(listB)),
		})
	}
	sortPayloads(listA)
	sortPayloads(listB)
	for i := 0; i < len(listA) && i < len(listB); i++ {
		ca, cb := listA[i], listB[i]
		if ca.payload != cb.payload {
			diffs = append(diffs, diff{
				kind:    "payload",
				subject: subjectOf(ca),
				detail:  "semantic payload differs",
			})
		}
		if ca.expireMs != cb.expireMs {
			diffs = append(diffs, diff{
				kind:    "expiry",
				subject: subjectOf(ca),
				detail:  fmt.Sprintf("expiration differs: %d vs %d (millisecond precision)", ca.expireMs, cb.expireMs),
			})
		}
		if ca.encoding != cb.encoding {
			diffs = append(diffs, diff{
				kind:    "encoding",
				subject: subjectOf(ca),
				detail:  fmt.Sprintf("encoding changed: %s -> %s", ca.encoding, cb.encoding),
			})
		}
		if ca.idle != cb.idle || ca.freq != cb.freq {
			diffs = append(diffs, diff{
				kind:    "lru-lfu",
				subject: subjectOf(ca),
				detail:  fmt.Sprintf("eviction metadata differs: idle %d->%d freq %d->%d", ca.idle, cb.idle, ca.freq, cb.freq),
			})
		}
	}
	return diffs
}

func sortPayloads(list []canonObject) {
	sort.Slice(list, func(i, j int) bool { return list[i].payload < list[j].payload })
}

// classifyDiffs splits diffs into structured expected losses (known,
// documented consequences of the encoder design) and hard failures.
func classifyDiffs(diffs []diff) (losses []lossEntry, failures []diff) {
	for _, d := range diffs {
		switch d.kind {
		case "encoding":
			// the encoder deliberately picks its own in-memory encoding;
			// the semantic payload is verified equal by the same diff pass
			losses = append(losses, lossEntry{kind: "encoding-changed", subject: d.subject, detail: d.detail})
		case "lru-lfu":
			losses = append(losses, lossEntry{kind: "lru-lfu-dropped", subject: d.subject, detail: d.detail})
		case "missing-object":
			if strings.Contains(d.subject, "type="+model.DBSizeType) {
				losses = append(losses, lossEntry{kind: "empty-db-dropped", subject: d.subject, detail: d.detail})
			} else {
				failures = append(failures, d)
			}
		case "extra-object":
			if strings.Contains(d.subject, "type="+model.DBSizeType) {
				// the encoder always emits a RESIZEDB hint per db, even when
				// the source rdb (pre v7 or hand crafted) did not carry one
				losses = append(losses, lossEntry{kind: "dbsize-added", subject: d.subject, detail: d.detail})
			} else {
				failures = append(failures, d)
			}
		default:
			failures = append(failures, d)
		}
	}
	return losses, failures
}
