package verify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Snapshot is the full, order-independent semantic view of an RDB file.
// Everything inside must be canonical so that two snapshots can be compared
// with a single deep-equal check.
type Snapshot struct {
	Valkey    bool
	Version   int
	Aux       []auxEntry
	Functions []string
	Objects   []canonicalObject
}

type auxEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// canonicalObject is a lossless JSON-able projection of a RedisObject.
// Raw key bytes are kept as base64-independent JSON strings built from the
// raw bytes via JSON's own escaping, so non-UTF8 keys stay distinguishable.
type canonicalObject struct {
	DB         int    `json:"db"`
	Type       string `json:"type"`
	Encoding   string `json:"encoding"`
	Key        string `json:"key"`
	Expiration int64  `json:"expirationMs"` // 0 means persistent
	Idle       int64  `json:"idle"`         // -1 means absent
	Freq       int64  `json:"freq"`         // -1 means absent
	Value      any    `json:"value"`
}

func buildSnapshot(version int, valkey bool, objects []model.RedisObject) *Snapshot {
	snap := &Snapshot{
		Valkey:  valkey,
		Version: version,
	}
	for _, obj := range objects {
		switch o := obj.(type) {
		case *model.AuxObject:
			snap.Aux = append(snap.Aux, auxEntry{Key: o.Key, Value: o.Value})
		case *model.FunctionsObject:
			snap.Functions = append(snap.Functions, o.FunctionsLua)
		}
	}
	for _, obj := range objects {
		if obj.GetType() == model.AuxType || obj.GetType() == model.FunctionsType || obj.GetType() == model.DBSizeType {
			continue
		}
		snap.Objects = append(snap.Objects, canonicalize(obj))
	}
	sort.Slice(snap.Objects, func(i, j int) bool {
		if snap.Objects[i].DB != snap.Objects[j].DB {
			return snap.Objects[i].DB < snap.Objects[j].DB
		}
		return snap.Objects[i].Key < snap.Objects[j].Key
	})
	sort.Slice(snap.Aux, func(i, j int) bool { return snap.Aux[i].Key < snap.Aux[j].Key })
	sort.Strings(snap.Functions)
	return snap
}

func canonicalize(obj model.RedisObject) canonicalObject {
	co := canonicalObject{
		DB:         obj.GetDBIndex(),
		Type:       obj.GetType(),
		Encoding:   obj.GetEncoding(),
		Key:        obj.GetKey(),
		Expiration: 0,
		Idle:       -1,
		Freq:       -1,
	}
	if exp := obj.GetExpiration(); exp != nil {
		co.Expiration = exp.UnixNano() / int64(time.Millisecond)
	}
	if ev, ok := obj.(model.EvictionInfo); ok {
		co.Idle = ev.GetIdleTime()
		co.Freq = ev.GetFreq()
	}
	switch o := obj.(type) {
	case *model.StringObject:
		co.Value = string(o.Value)
	case *model.ListObject:
		vals := make([]string, len(o.Values))
		for i, v := range o.Values {
			vals[i] = string(v)
		}
		co.Value = vals
	case *model.SetObject:
		members := make([]string, len(o.Members))
		for i, v := range o.Members {
			members[i] = string(v)
		}
		sort.Strings(members)
		co.Value = members
	case *model.HashObject:
		type pair struct {
			Field  string `json:"field"`
			Value  string `json:"value"`
			Expire int64  `json:"fieldExpireMs"`
		}
		pairs := make([]pair, 0, len(o.Hash))
		for field, val := range o.Hash {
			p := pair{Field: field, Value: string(val)}
			if len(o.FieldExpirations) > 0 {
				p.Expire = o.FieldExpirations[field]
			}
			pairs = append(pairs, p)
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].Field < pairs[j].Field })
		co.Value = pairs
	case *model.ZSetObject:
		type entry struct {
			Member string  `json:"member"`
			Score  float64 `json:"score"`
		}
		entries := make([]entry, len(o.Entries))
		for i, e := range o.Entries {
			entries[i] = entry{Member: e.Member, Score: e.Score}
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Member < entries[j].Member })
		co.Value = entries
	case *model.StreamObject:
		co.Value = canonicalizeStream(o)
	case *model.ModuleTypeObject:
		raw, _ := json.Marshal(o.Value)
		co.Value = map[string]any{"moduleType": o.ModuleType, "value": json.RawMessage(raw)}
	default:
		co.Value = fmt.Sprintf("unsupported-object-type-%T", obj)
	}
	return co
}

func canonicalizeStream(o *model.StreamObject) map[string]any {
	type id struct {
		Ms       uint64 `json:"ms"`
		Sequence uint64 `json:"sequence"`
	}
	idc := func(s *model.StreamId) id {
		if s == nil {
			return id{}
		}
		return id{Ms: s.Ms, Sequence: s.Sequence}
	}
	type message struct {
		ID      id          `json:"id"`
		Deleted bool        `json:"deleted"`
		Fields  [][2]string `json:"fields"`
	}
	type entry struct {
		FirstMsgID id        `json:"firstMsgId"`
		Messages   []message `json:"msgs"`
	}
	type pel struct {
		ID            id     `json:"id"`
		DeliveryTime  uint64 `json:"deliveryTime"`
		DeliveryCount uint64 `json:"deliveryCount"`
	}
	type consumer struct {
		Name       string `json:"name"`
		SeenTime   uint64 `json:"seenTime"`
		ActiveTime uint64 `json:"activeTime"`
		Pending    []id   `json:"pending"`
	}
	type group struct {
		Name        string     `json:"name"`
		LastID      id         `json:"lastId"`
		EntriesRead uint64     `json:"entriesRead"`
		Pending     []pel      `json:"pending"`
		Consumers   []consumer `json:"consumers"`
	}

	entries := make([]entry, 0, len(o.Entries))
	for _, e := range o.Entries {
		ce := entry{FirstMsgID: idc(e.FirstMsgId)}
		for _, m := range e.Msgs {
			cm := message{ID: idc(m.Id), Deleted: m.Deleted}
			keys := make([]string, 0, len(m.Fields))
			for k := range m.Fields {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				cm.Fields = append(cm.Fields, [2]string{k, m.Fields[k]})
			}
			ce.Messages = append(ce.Messages, cm)
		}
		sort.Slice(ce.Messages, func(i, j int) bool {
			if ce.Messages[i].ID.Ms != ce.Messages[j].ID.Ms {
				return ce.Messages[i].ID.Ms < ce.Messages[j].ID.Ms
			}
			return ce.Messages[i].ID.Sequence < ce.Messages[j].ID.Sequence
		})
		entries = append(entries, ce)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].FirstMsgID.Ms != entries[j].FirstMsgID.Ms {
			return entries[i].FirstMsgID.Ms < entries[j].FirstMsgID.Ms
		}
		return entries[i].FirstMsgID.Sequence < entries[j].FirstMsgID.Sequence
	})

	groups := make([]group, 0, len(o.Groups))
	for _, g := range o.Groups {
		cg := group{Name: g.Name, LastID: idc(g.LastId), EntriesRead: g.EntriesRead}
		for _, p := range g.Pending {
			cg.Pending = append(cg.Pending, pel{ID: idc(p.Id), DeliveryTime: p.DeliveryTime, DeliveryCount: p.DeliveryCount})
		}
		sort.Slice(cg.Pending, func(i, j int) bool {
			if cg.Pending[i].ID.Ms != cg.Pending[j].ID.Ms {
				return cg.Pending[i].ID.Ms < cg.Pending[j].ID.Ms
			}
			return cg.Pending[i].ID.Sequence < cg.Pending[j].ID.Sequence
		})
		for _, c := range g.Consumers {
			cc := consumer{Name: c.Name, SeenTime: c.SeenTime, ActiveTime: c.ActiveTime}
			for _, p := range c.Pending {
				cc.Pending = append(cc.Pending, idc(p))
			}
			sort.Slice(cc.Pending, func(i, k int) bool {
				if cc.Pending[i].Ms != cc.Pending[k].Ms {
					return cc.Pending[i].Ms < cc.Pending[k].Ms
				}
				return cc.Pending[i].Sequence < cc.Pending[k].Sequence
			})
			cg.Consumers = append(cg.Consumers, cc)
		}
		sort.Slice(cg.Consumers, func(i, j int) bool { return cg.Consumers[i].Name < cg.Consumers[j].Name })
		groups = append(groups, cg)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })

	return map[string]any{
		"version":           o.Version,
		"len":               o.Length,
		"lastId":            idc(o.LastId),
		"firstId":           idc(o.FirstId),
		"maxDeletedId":      idc(o.MaxDeletedId),
		"addedEntriesCount": o.AddedEntriesCount,
		"entries":           entries,
		"groups":            groups,
	}
}

// Diff describes one semantic mismatch between two snapshots.
type Diff struct {
	Path string
	Msg  string
}

// compareSnapshots deep-compares canonical projections. Auxiliary fields and
// function libraries are compared strictly; DB index, raw key bytes,
// millisecond expiry precision, stream ids, group/consumer/PEL ownership and
// function libraries are all part of the projection.
func compareSnapshots(a, b *Snapshot) []Diff {
	var diffs []Diff
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	if bytes.Equal(aj, bj) {
		return nil
	}
	if len(a.Aux) != len(b.Aux) {
		diffs = append(diffs, Diff{Msg: fmt.Sprintf("aux field count %d != %d", len(a.Aux), len(b.Aux))})
	} else {
		for i := range a.Aux {
			if a.Aux[i] != b.Aux[i] {
				diffs = append(diffs, Diff{Msg: fmt.Sprintf("aux[%s]: %q != %q", a.Aux[i].Key, a.Aux[i].Value, b.Aux[i].Value)})
			}
		}
	}
	if len(a.Functions) != len(b.Functions) {
		diffs = append(diffs, Diff{Msg: fmt.Sprintf("function library count %d != %d", len(a.Functions), len(b.Functions))})
	} else {
		for i := range a.Functions {
			if a.Functions[i] != b.Functions[i] {
				diffs = append(diffs, Diff{Msg: "function library payload differs"})
			}
		}
	}
	if len(a.Objects) != len(b.Objects) {
		diffs = append(diffs, Diff{Msg: fmt.Sprintf("object count %d != %d", len(a.Objects), len(b.Objects))})
	}
	n := len(a.Objects)
	if len(b.Objects) < n {
		n = len(b.Objects)
	}
	for i := 0; i < n; i++ {
		oa, ob := a.Objects[i], b.Objects[i]
		if oa.DB != ob.DB {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("object %d db %d != %d", i, oa.DB, ob.DB)})
		}
		if oa.Key != ob.Key {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("object %d key %q != %q", i, oa.Key, ob.Key)})
		}
		if oa.Type != ob.Type {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("key %q type %s != %s", oa.Key, oa.Type, ob.Type)})
		}
		if oa.Expiration != ob.Expiration {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("key %q expiration %d != %d", oa.Key, oa.Expiration, ob.Expiration)})
		}
		if oa.Idle != ob.Idle {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("key %q lru-idle %d != %d", oa.Key, oa.Idle, ob.Idle)})
		}
		if oa.Freq != ob.Freq {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("key %q lfu-freq %d != %d", oa.Key, oa.Freq, ob.Freq)})
		}
		ovj, _ := json.Marshal(oa.Value)
		nvj, _ := json.Marshal(ob.Value)
		if !bytes.Equal(ovj, nvj) {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("key %q value differs: %s vs %s", oa.Key, compactJSON(ovj), compactJSON(nvj))})
		}
	}
	return diffs
}

func compactJSON(raw []byte) string {
	if len(raw) > 400 {
		return string(raw[:400]) + "...(truncated)"
	}
	return string(raw)
}
