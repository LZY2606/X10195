package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
func b64b(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// canonObj is the normalized semantic form of one object. Map and set
// ordering is normalized away; DB number, raw key bytes, expiration
// precision, stream ids, group/consumer/PEL ownership and HFE field
// expirations are all preserved.
type canonObj struct {
	DB       int         `json:"db"`
	Type     string      `json:"type"`
	Key      string      `json:"key"` // base64 of raw key bytes
	ExpireMs *int64      `json:"expireMs,omitempty"`
	Payload  interface{} `json:"payload,omitempty"`
}

type canonHash struct {
	Fields  map[string]string `json:"fields"`           // base64 field -> base64 value
	Expires map[string]int64  `json:"expires,omitempty"` // base64 field -> absolute unix ms
}

type canonStreamID [2]uint64

type canonStreamMsg struct {
	ID      canonStreamID     `json:"id"`
	Deleted bool              `json:"deleted"`
	Fields  map[string]string `json:"fields"` // base64 field -> base64 value
}

type canonStreamEntry struct {
	FirstMsgID canonStreamID   `json:"firstMsgId"`
	Fields     []string        `json:"fields"` // base64 master fields, order preserved
	Msgs       []canonStreamMsg `json:"msgs"`
}

type canonStreamNAck struct {
	ID            canonStreamID `json:"id"`
	DeliveryTime  uint64        `json:"deliveryTime"`
	DeliveryCount uint64        `json:"deliveryCount"`
}

type canonStreamConsumer struct {
	Name       string          `json:"name"` // base64
	SeenTime   uint64          `json:"seenTime"`
	ActiveTime uint64          `json:"activeTime"`
	Pending    []canonStreamID `json:"pending,omitempty"`
}

type canonStreamGroup struct {
	Name        string               `json:"name"` // base64
	LastID      canonStreamID        `json:"lastId"`
	EntriesRead uint64               `json:"entriesRead"`
	Pending     []canonStreamNAck    `json:"pending,omitempty"`
	Consumers   []canonStreamConsumer `json:"consumers,omitempty"`
}

type canonStream struct {
	Version           uint64             `json:"version"`
	Length            uint64             `json:"length"`
	LastID            canonStreamID      `json:"lastId"`
	FirstID           *canonStreamID     `json:"firstId,omitempty"`
	MaxDeletedID      *canonStreamID     `json:"maxDeletedId,omitempty"`
	AddedEntriesCount uint64             `json:"addedEntriesCount"`
	Entries           []canonStreamEntry `json:"entries"`
	Groups            []canonStreamGroup `json:"groups,omitempty"`
}

func streamID(id *model.StreamId) canonStreamID {
	if id == nil {
		return canonStreamID{}
	}
	return canonStreamID{id.Ms, id.Sequence}
}

func canonicalObject(obj model.RedisObject) ([]byte, error) {
	c := &canonObj{
		DB:   obj.GetDBIndex(),
		Type: obj.GetType(),
		Key:  b64(obj.GetKey()),
	}
	if expiration := obj.GetExpiration(); expiration != nil {
		ms := expiration.UnixMilli()
		c.ExpireMs = &ms
	}
	switch o := obj.(type) {
	case *model.StringObject:
		c.Payload = b64b(o.Value)
	case *model.ListObject:
		values := make([]string, len(o.Values))
		for i, v := range o.Values {
			values[i] = b64b(v)
		}
		c.Payload = values
	case *model.SetObject:
		members := make([]string, len(o.Members))
		for i, m := range o.Members {
			members[i] = b64b(m)
		}
		sort.Strings(members)
		c.Payload = members
	case *model.HashObject:
		h := &canonHash{Fields: make(map[string]string)}
		for field, value := range o.Hash {
			h.Fields[b64(field)] = b64b(value)
		}
		if len(o.FieldExpirations) > 0 {
			h.Expires = make(map[string]int64)
			for field, expire := range o.FieldExpirations {
				h.Expires[b64(field)] = expire
			}
		}
		c.Payload = h
	case *model.ZSetObject:
		entries := make(map[string]uint64, len(o.Entries))
		for _, e := range o.Entries {
			entries[b64(e.Member)] = math.Float64bits(e.Score)
		}
		c.Payload = entries
	case *model.StreamObject:
		c.Payload = canonicalStream(o)
	default:
		return nil, fmt.Errorf("cannot canonicalize type %q", obj.GetType())
	}
	return json.Marshal(c)
}

func canonicalStream(s *model.StreamObject) *canonStream {
	cs := &canonStream{
		Version:           uint64(s.Version),
		Length:            s.Length,
		LastID:            streamID(s.LastId),
		AddedEntriesCount: s.AddedEntriesCount,
	}
	if s.FirstId != nil {
		id := streamID(s.FirstId)
		cs.FirstID = &id
	}
	if s.MaxDeletedId != nil {
		id := streamID(s.MaxDeletedId)
		cs.MaxDeletedID = &id
	}
	for _, entry := range s.Entries {
		ce := canonStreamEntry{FirstMsgID: streamID(entry.FirstMsgId)}
		for _, f := range entry.Fields {
			ce.Fields = append(ce.Fields, b64(f))
		}
		for _, msg := range entry.Msgs {
			cm := canonStreamMsg{
				ID:      streamID(msg.Id),
				Deleted: msg.Deleted,
				Fields:  make(map[string]string),
			}
			for field, value := range msg.Fields {
				cm.Fields[b64(field)] = b64(value)
			}
			ce.Msgs = append(ce.Msgs, cm)
		}
		sort.Slice(ce.Msgs, func(i, j int) bool { return lessStreamID(ce.Msgs[i].ID, ce.Msgs[j].ID) })
		cs.Entries = append(cs.Entries, ce)
	}
	for _, group := range s.Groups {
		cg := canonStreamGroup{
			Name:        b64(group.Name),
			LastID:      streamID(group.LastId),
			EntriesRead: group.EntriesRead,
		}
		for _, nack := range group.Pending {
			cg.Pending = append(cg.Pending, canonStreamNAck{
				ID:            streamID(nack.Id),
				DeliveryTime:  nack.DeliveryTime,
				DeliveryCount: nack.DeliveryCount,
			})
		}
		sort.Slice(cg.Pending, func(i, j int) bool { return lessStreamID(cg.Pending[i].ID, cg.Pending[j].ID) })
		for _, consumer := range group.Consumers {
			cc := canonStreamConsumer{
				Name:       b64(consumer.Name),
				SeenTime:   consumer.SeenTime,
				ActiveTime: consumer.ActiveTime,
			}
			for _, id := range consumer.Pending {
				cc.Pending = append(cc.Pending, streamID(id))
			}
			sort.Slice(cc.Pending, func(i, j int) bool { return lessStreamID(cc.Pending[i], cc.Pending[j]) })
			cg.Consumers = append(cg.Consumers, cc)
		}
		sort.Slice(cg.Consumers, func(i, j int) bool { return cg.Consumers[i].Name < cg.Consumers[j].Name })
		cs.Groups = append(cs.Groups, cg)
	}
	sort.Slice(cs.Groups, func(i, j int) bool { return cs.Groups[i].Name < cs.Groups[j].Name })
	return cs
}

func lessStreamID(a, b canonStreamID) bool {
	if a[0] != b[0] {
		return a[0] < b[0]
	}
	return a[1] < b[1]
}

func objectID(obj model.RedisObject) string {
	return strconv.Itoa(obj.GetDBIndex()) + "|" + obj.GetType() + "|" + b64(obj.GetKey())
}

// compareDumps semantically compares two decoded dumps. Function libraries
// are compared separately by the caller (they are a declared expected-loss),
// everything else must match exactly after normalization.
func compareDumps(a, b *objectDump) []string {
	var diffs []string

	auxA := sortedAux(a.aux)
	auxB := sortedAux(b.aux)
	if !equalStringPairs(auxA, auxB) {
		diffs = append(diffs, fmt.Sprintf("aux mismatch: original=%v re-encoded=%v", auxA, auxB))
	}

	for db, counts := range a.dbsize {
		got, ok := b.dbsize[db]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("dbsize missing after re-encode: db=%d", db))
			continue
		}
		if got != counts {
			diffs = append(diffs, fmt.Sprintf("dbsize mismatch db=%d: original=%v re-encoded=%v", db, counts, got))
		}
	}

	indexB := make(map[string]model.RedisObject)
	for _, obj := range b.objects {
		indexB[objectID(obj)] = obj
	}
	matchedB := make(map[string]bool)
	for _, objA := range a.objects {
		id := objectID(objA)
		objB, ok := indexB[id]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("missing object after re-encode: db=%d type=%s key=%q",
				objA.GetDBIndex(), objA.GetType(), objA.GetKey()))
			continue
		}
		matchedB[id] = true
		canonA, err := canonicalObject(objA)
		if err != nil {
			diffs = append(diffs, err.Error())
			continue
		}
		canonB, err := canonicalObject(objB)
		if err != nil {
			diffs = append(diffs, err.Error())
			continue
		}
		if !bytes.Equal(canonA, canonB) {
			diffs = append(diffs, fmt.Sprintf("object mismatch db=%d type=%s key=%q: original=%s re-encoded=%s",
				objA.GetDBIndex(), objA.GetType(), objA.GetKey(),
				truncate(string(canonA), 200), truncate(string(canonB), 200)))
		}
	}
	for _, objB := range b.objects {
		id := objectID(objB)
		if !matchedB[id] {
			diffs = append(diffs, fmt.Sprintf("unexpected object after re-encode: db=%d type=%s key=%q",
				objB.GetDBIndex(), objB.GetType(), objB.GetKey()))
		}
	}
	return diffs
}

func sortedAux(aux [][2]string) [][2]string {
	out := append([][2]string(nil), aux...)
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}

func equalStringPairs(a, b [][2]string) bool {
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
