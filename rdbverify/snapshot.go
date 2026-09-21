// Package rdbverify implements the offline fixture verification gate for
// RDB files: discovery, decode -> re-encode -> re-decode roundtrips with
// semantic comparison, plus AOF conversion structure checks.
package rdbverify

import (
	"sort"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Snapshot is a fully normalized, semantic view of an RDB file.
// Build it from a parsed object stream with BuildSnapshot.
type Snapshot struct {
	Magic   string // "REDIS" or "VALKEY"
	Version int    // header version, e.g. 11 or 80
	Aux     map[string]string
	// DBSizes captures RESIZEDB hints keyed by db index.
	DBSizes map[uint]DBSizeHint
	// DBS is ordered by DB index in Snapshot.DBs (canonical form).
	DBS []*DBSnapshot
}

// DBSizeHint captures the two RDB_OPCODE_RESIZEDB counters.
type DBSizeHint struct {
	KeyCount uint64
	TTLCount uint64
}

// DBSnapshot is all data held by one logical database.
type DBSnapshot struct {
	Index int
	Keys  []*KeySnapshot // canonical: sorted by key bytes
}

// KeySnapshot is one Redis key with all ownership metadata preserved.
type KeySnapshot struct {
	Key        []byte // raw bytes, never coerced to string
	Type       string
	Encoding   string
	Expiration *time.Time // exact ms precision, nil for persistent keys
	IdleTime   int64      // -1 when absent
	Freq       int64      // -1 when absent
	Value      ValueSnapshot
}

// ValueSnapshot is the type-erased semantic payload of a key.
type ValueSnapshot struct {
	String *StringValue
	List   *ListValue
	Set    *SetValue
	Hash   *HashValue
	ZSet   *ZSetValue
	Stream *StreamValue
	// Function holds a function library payload.
	Function *FunctionValue
}

// StringValue holds a raw string value.
type StringValue struct {
	Bytes []byte
}

// ListValue is an ordered list.
type ListValue struct {
	Items [][]byte
}

// SetValue is a set; member order is normalized on construction.
type SetValue struct {
	Members [][]byte
}

// HashValue preserves field order-independence but keeps the raw field
// bytes and exact per-field expiration (ms epoch; 0 means HFE persisted).
type HashValue struct {
	Fields          []HashField
	FieldExpiration map[string]int64
}

// HashField is one field/value pair of a hash.
type HashField struct {
	Field []byte
	Value []byte
}

// ZSetValue keeps members in a canonical (member-bytes) order but preserves
// every score bit-for-bit.
type ZSetValue struct {
	Entries []ZSetEntry
}

// ZSetEntry is a zset member/score pair.
type ZSetEntry struct {
	Member []byte
	Score  float64
}

// StreamValue captures the complete stream semantics, including versions,
// ids, groups/consumers and PEL ownership.
type StreamValue struct {
	Version           uint
	Length            uint64
	LastID            StreamID
	FirstID           *StreamID
	MaxDeletedID      *StreamID
	AddedEntriesCount uint64
	Entries           []StreamEntry
	Groups            []StreamGroup
}

// StreamID is the 128 bit stream id, compared exactly.
type StreamID struct {
	Ms       uint64
	Sequence uint64
}

// StreamEntry is one listpack node with its messages in stored order.
type StreamEntry struct {
	FirstMsgID StreamID
	Fields     [][]byte
	Msgs       []StreamMessage
}

// StreamMessage is one stream message; Deleted markers are preserved.
type StreamMessage struct {
	ID      StreamID
	Deleted bool
	Fields  []StreamKVPair
}

// StreamKVPair is a message field/value pair in stored (not sorted) order.
type StreamKVPair struct {
	Field []byte
	Value []byte
}

// StreamGroup captures group ownership metadata including PELs.
type StreamGroup struct {
	Name        string
	LastID      StreamID
	EntriesRead uint64
	Pending     []StreamNAck
	Consumers   []StreamConsumer
}

// StreamNAck is a group-level pending entry with ownership metadata.
type StreamNAck struct {
	ID            StreamID
	DeliveryTime  uint64
	DeliveryCount uint64
}

// StreamConsumer is a consumer with its seen/active times and PEL ids.
type StreamConsumer struct {
	Name       string
	SeenTime   uint64
	ActiveTime uint64
	Pending    []StreamID
}

// FunctionValue is a Redis function library blob.
type FunctionValue struct {
	Lua string
}

// SnapshotBuilder accumulates parsed objects into a Snapshot.
type SnapshotBuilder struct {
	snap *Snapshot
}

// NewSnapshotBuilder returns an empty builder with the given header info.
func NewSnapshotBuilder(magic string, version int) *SnapshotBuilder {
	return &SnapshotBuilder{
		snap: &Snapshot{
			Magic:   magic,
			Version: version,
			Aux:     make(map[string]string),
			DBSizes: make(map[uint]DBSizeHint),
		},
	}
}

// AddObject folds one parsed model object into the builder.
func (b *SnapshotBuilder) AddObject(obj model.RedisObject) {
	switch o := obj.(type) {
	case *model.AuxObject:
		b.snap.Aux[o.Key] = o.Value
	case *model.DBSizeObject:
		b.snap.DBSizes[uint(o.DB)] = DBSizeHint{KeyCount: o.KeyCount, TTLCount: o.TTLCount}
	case *model.FunctionsObject:
		ks := keyFromBase(o.BaseObject, []byte("functions"))
		ks.Value.Function = &FunctionValue{Lua: o.FunctionsLua}
		b.db(o.GetDBIndex()).addKey(ks, nil)
	default:
		b.addDataKey(o)
	}
}

func (b *SnapshotBuilder) addDataKey(obj model.RedisObject) {
	base := getBase(obj)
	if base == nil {
		return
	}
	db := b.db(base.DB)
	ks := &KeySnapshot{
		Key:        []byte(base.Key),
		Type:       obj.GetType(),
		Encoding:   base.Encoding,
		Expiration: cloneTime(base.Expiration),
		IdleTime:   base.GetIdleTime(),
		Freq:       base.GetFreq(),
	}
	switch o := obj.(type) {
	case *model.StringObject:
		ks.Value.String = &StringValue{Bytes: cloneBytes(o.Value)}
	case *model.ListObject:
		ks.Value.List = &ListValue{Items: cloneBytesSlice(o.Values)}
	case *model.SetObject:
		members := cloneBytesSlice(o.Members)
		sort.Slice(members, func(i, j int) bool { return bytesLess(members[i], members[j]) })
		ks.Value.Set = &SetValue{Members: members}
	case *model.HashObject:
		ks.Value.Hash = hashValue(o)
	case *model.ZSetObject:
		entries := make([]ZSetEntry, 0, len(o.Entries))
		for _, e := range o.Entries {
			entries = append(entries, ZSetEntry{Member: []byte(e.Member), Score: e.Score})
		}
		sort.Slice(entries, func(i, j int) bool { return bytesLess(entries[i].Member, entries[j].Member) })
		ks.Value.ZSet = &ZSetValue{Entries: entries}
	case *model.StreamObject:
		ks.Value.Stream = streamValue(o)
	default:
		// Unknown object types cannot be compared; leave value nil so the
		// comparator reports a typed, structural mismatch instead of panic.
	}
	db.addKey(ks, nil)
}

func hashValue(o *model.HashObject) *HashValue {
	fields := make([]HashField, 0, len(o.Hash))
	for field, value := range o.Hash {
		fields = append(fields, HashField{Field: []byte(field), Value: cloneBytes(value)})
	}
	sort.Slice(fields, func(i, j int) bool { return bytesLess(fields[i].Field, fields[j].Field) })
	exp := make(map[string]int64, len(o.FieldExpirations))
	for k, v := range o.FieldExpirations {
		exp[k] = v
	}
	return &HashValue{Fields: fields, FieldExpiration: exp}
}

func streamValue(o *model.StreamObject) *StreamValue {
	v := &StreamValue{
		Version: o.Version,
		Length:  o.Length,
		LastID:  streamID(o.LastId),
	}
	if o.FirstId != nil {
		id := streamID(o.FirstId)
		v.FirstID = &id
	}
	if o.MaxDeletedId != nil {
		id := streamID(o.MaxDeletedId)
		v.MaxDeletedID = &id
	}
	v.AddedEntriesCount = o.AddedEntriesCount
	for _, e := range o.Entries {
		entry := StreamEntry{
			FirstMsgID: streamID(e.FirstMsgId),
			Fields:     cloneStrings(e.Fields),
		}
		for _, m := range e.Msgs {
			msg := StreamMessage{ID: streamID(m.Id), Deleted: m.Deleted}
			// message.Fields is a map; normalize by the node field order when
			// possible, falling back to sorted keys. Ordering semantics here are
			// name/value pairing order, which Redis defines via entry.Fields.
			msg.Fields = normalizeStreamFields(e.Fields, m.Fields)
			entry.Msgs = append(entry.Msgs, msg)
		}
		v.Entries = append(v.Entries, entry)
	}
	for _, g := range o.Groups {
		group := StreamGroup{
			Name:        g.Name,
			LastID:      streamID(g.LastId),
			EntriesRead: g.EntriesRead,
		}
		for _, p := range g.Pending {
			group.Pending = append(group.Pending, StreamNAck{
				ID:            streamID(p.Id),
				DeliveryTime:  p.DeliveryTime,
				DeliveryCount: p.DeliveryCount,
			})
		}
		for _, c := range g.Consumers {
			consumer := StreamConsumer{
				Name:       c.Name,
				SeenTime:   c.SeenTime,
				ActiveTime: c.ActiveTime,
			}
			for _, pid := range c.Pending {
				consumer.Pending = append(consumer.Pending, streamID(pid))
			}
			group.Consumers = append(group.Consumers, consumer)
		}
		v.Groups = append(v.Groups, group)
	}
	return v
}

func normalizeStreamFields(order []string, values map[string]string) []StreamKVPair {
	pairs := make([]StreamKVPair, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, name := range order {
		if val, ok := values[name]; ok {
			pairs = append(pairs, StreamKVPair{Field: []byte(name), Value: []byte(val)})
			seen[name] = struct{}{}
		}
	}
	if len(seen) < len(values) {
		extra := make([]string, 0, len(values)-len(seen))
		for name := range values {
			if _, ok := seen[name]; !ok {
				extra = append(extra, name)
			}
		}
		sort.Strings(extra)
		for _, name := range extra {
			pairs = append(pairs, StreamKVPair{Field: []byte(name), Value: []byte(values[name])})
		}
	}
	return pairs
}

func streamID(id *model.StreamId) StreamID {
	if id == nil {
		return StreamID{}
	}
	return StreamID{Ms: id.Ms, Sequence: id.Sequence}
}

func (b *SnapshotBuilder) db(index int) *DBSnapshot {
	for i := range b.snap.DBS {
		if b.snap.DBS[i].Index == index {
			return b.snap.DBS[i]
		}
	}
	db := &DBSnapshot{Index: index}
	b.snap.DBS = append(b.snap.DBS, db)
	return db
}

// addKey inserts a key; the second argument path exists only so callers may
// pass a prebuilt KeySnapshot for function libraries.
func (d *DBSnapshot) addKey(key *KeySnapshot, _ []byte) {
	d.Keys = append(d.Keys, key)
}

func keyFromBase(base *model.BaseObject, fallback []byte) *KeySnapshot {
	return &KeySnapshot{
		Key:      fallback,
		Type:     base.Type,
		Encoding: base.Encoding,
		IdleTime: base.GetIdleTime(),
		Freq:     base.GetFreq(),
	}
}

func getBase(obj model.RedisObject) *model.BaseObject {
	switch o := obj.(type) {
	case *model.StringObject:
		return o.BaseObject
	case *model.ListObject:
		return o.BaseObject
	case *model.SetObject:
		return o.BaseObject
	case *model.HashObject:
		return o.BaseObject
	case *model.ZSetObject:
		return o.BaseObject
	case *model.StreamObject:
		return o.BaseObject
	case *model.AuxObject:
		return o.BaseObject
	case *model.FunctionsObject:
		return o.BaseObject
	case *model.DBSizeObject:
		return o.BaseObject
	case *model.ModuleTypeObject:
		return o.BaseObject
	}
	return nil
}

// Build finalizes the snapshot into canonical (sorted) form.
func (b *SnapshotBuilder) Build() *Snapshot {
	sort.Slice(b.snap.DBS, func(i, j int) bool { return b.snap.DBS[i].Index < b.snap.DBS[j].Index })
	for _, db := range b.snap.DBS {
		sort.Slice(db.Keys, func(i, j int) bool { return bytesLess(db.Keys[i].Key, db.Keys[j].Key) })
		for _, k := range db.Keys {
			if k.Value.Stream != nil {
				groups := k.Value.Stream.Groups
				sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
			}
		}
	}
	return b.snap
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func cloneBytesSlice(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = cloneBytes(in[i])
	}
	return out
}

func cloneStrings(in []string) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = []byte(in[i])
	}
	return out
}

func bytesLess(a, b []byte) bool {
	la, lb := len(a), len(b)
	n := la
	if lb < n {
		n = lb
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return la < lb
}
