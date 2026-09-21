package rdbverify

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// DiffCode is a stable, machine-readable identifier for a semantic
// discrepancy. The gate matches manifest expectations against these codes.
type DiffCode string

const (
	DiffRDBVersion       DiffCode = "rdb-version-changed"
	DiffMagic            DiffCode = "magic-changed"
	DiffAuxFieldMissing  DiffCode = "aux-field-missing"
	DiffAuxFieldChanged  DiffCode = "aux-field-changed"
	DiffDBSizeHint       DiffCode = "dbsize-hint-changed"
	DiffDBMissing        DiffCode = "db-missing"
	DiffDBExtra          DiffCode = "db-extra"
	DiffKeyMissing       DiffCode = "key-missing"
	DiffKeyExtra         DiffCode = "key-extra"
	DiffKeyType          DiffCode = "key-type-changed"
	DiffKeyEncoding      DiffCode = "key-encoding-changed"
	DiffExpiration       DiffCode = "key-expiration-changed"
	DiffLRU              DiffCode = "key-lru-lost"
	DiffLFU              DiffCode = "key-lfu-lost"
	DiffStringValue      DiffCode = "string-value-changed"
	DiffListValue        DiffCode = "list-value-changed"
	DiffSetValue         DiffCode = "set-value-changed"
	DiffHashValue        DiffCode = "hash-value-changed"
	DiffHashFieldExpire  DiffCode = "hash-field-expiration-changed"
	DiffZSetValue        DiffCode = "zset-value-changed"
	DiffStreamVersion    DiffCode = "stream-version-changed"
	DiffStreamMetadata   DiffCode = "stream-metadata-changed"
	DiffStreamEntry      DiffCode = "stream-entry-changed"
	DiffStreamGroup      DiffCode = "stream-group-changed"
	DiffStreamConsumer   DiffCode = "stream-consumer-changed"
	DiffStreamPEL        DiffCode = "stream-pel-changed"
	DiffFunction         DiffCode = "function-library-changed"
	DiffUnsupportedValue DiffCode = "unsupported-value-type"
)

// Diff is one semantic discrepancy located by db/key with details.
type Diff struct {
	Code   DiffCode `json:"code"`
	DB     int      `json:"db,omitempty"`
	Key    string   `json:"key,omitempty"`
	Detail string   `json:"detail,omitempty"`
}

// CompareSnapshots returns the full list of semantic differences between
// an original snapshot and a roundtripped one. Order is normalized inside
// snapshots, so map/set iteration order never appears as a difference.
func CompareSnapshots(want, got *Snapshot) []Diff {
	c := &comparator{}
	c.compareHeader(want, got)
	c.compareAux(want, got)
	c.compareDBSizes(want, got)
	c.compareDBs(want, got)
	return c.diffs
}

type comparator struct {
	diffs []Diff
}

func (c *comparator) add(code DiffCode, db int, key string, format string, args ...interface{}) {
	c.diffs = append(c.diffs, Diff{
		Code:   code,
		DB:     db,
		Key:    key,
		Detail: fmt.Sprintf(format, args...),
	})
}

func (c *comparator) compareHeader(want, got *Snapshot) {
	if want.Magic != got.Magic {
		c.add(DiffMagic, -1, "", "magic %q -> %q", want.Magic, got.Magic)
	}
	if want.Version != got.Version {
		c.add(DiffRDBVersion, -1, "", "rdb version %d -> %d", want.Version, got.Version)
	}
}

func (c *comparator) compareAux(want, got *Snapshot) {
	keys := make([]string, 0, len(want.Aux))
	for k := range want.Aux {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		gv, ok := got.Aux[k]
		if !ok {
			c.add(DiffAuxFieldMissing, -1, "", "aux %q dropped", k)
			continue
		}
		if gv != want.Aux[k] {
			c.add(DiffAuxFieldChanged, -1, "", "aux %q: %q -> %q", k, want.Aux[k], gv)
		}
	}
}

func (c *comparator) compareDBSizes(want, got *Snapshot) {
	dbs := make([]uint, 0, len(want.DBSizes))
	for db := range want.DBSizes {
		dbs = append(dbs, db)
	}
	sort.Slice(dbs, func(i, j int) bool { return dbs[i] < dbs[j] })
	for _, db := range dbs {
		w := want.DBSizes[db]
		g, ok := got.DBSizes[db]
		if !ok {
			c.add(DiffDBSizeHint, int(db), "", "resize hint dropped")
			continue
		}
		if w.KeyCount != g.KeyCount || w.TTLCount != g.TTLCount {
			c.add(DiffDBSizeHint, int(db), "", "db %d hint %d/%d -> %d/%d",
				db, w.KeyCount, w.TTLCount, g.KeyCount, g.TTLCount)
		}
	}
}

func (c *comparator) compareDBs(want, got *Snapshot) {
	gotByIndex := make(map[int]*DBSnapshot, len(got.DBS))
	for _, db := range got.DBS {
		gotByIndex[db.Index] = db
	}
	seen := make(map[int]struct{}, len(got.DBS))
	for _, wdb := range want.DBS {
		gdb, ok := gotByIndex[wdb.Index]
		if !ok {
			c.add(DiffDBMissing, wdb.Index, "", "db %d missing after roundtrip", wdb.Index)
			continue
		}
		seen[wdb.Index] = struct{}{}
		c.compareDB(wdb, gdb)
	}
	for _, gdb := range got.DBS {
		if _, ok := seen[gdb.Index]; !ok {
			c.add(DiffDBExtra, gdb.Index, "", "db %d appeared after roundtrip", gdb.Index)
		}
	}
}

func (c *comparator) compareDB(want, got *DBSnapshot) {
	gotKeys := make(map[string]*KeySnapshot, len(got.Keys))
	for _, k := range got.Keys {
		gotKeys[string(k.Key)] = k
	}
	seen := make(map[string]struct{}, len(got.Keys))
	for _, wk := range want.Keys {
		gk, ok := gotKeys[string(wk.Key)]
		if !ok {
			c.add(DiffKeyMissing, want.Index, string(wk.Key), "key missing after roundtrip")
			continue
		}
		seen[string(wk.Key)] = struct{}{}
		c.compareKey(want.Index, wk, gk)
	}
	for _, gk := range got.Keys {
		if _, ok := seen[string(gk.Key)]; !ok {
			c.add(DiffKeyExtra, want.Index, string(gk.Key), "key appeared after roundtrip")
		}
	}
}

func (c *comparator) compareKey(db int, want, got *KeySnapshot) {
	if want.Type != got.Type {
		c.add(DiffKeyType, db, string(want.Key), "%s -> %s", want.Type, got.Type)
		return
	}
	if want.Encoding != got.Encoding {
		c.add(DiffKeyEncoding, db, string(want.Key), "encoding %s -> %s", want.Encoding, got.Encoding)
	}
	if !expirationEqual(want.Expiration, got.Expiration) {
		c.add(DiffExpiration, db, string(want.Key), "expiration %s -> %s",
			formatTime(want.Expiration), formatTime(got.Expiration))
	}
	if want.IdleTime != got.IdleTime {
		c.add(DiffLRU, db, string(want.Key), "lru idle %d -> %d", want.IdleTime, got.IdleTime)
	}
	if want.Freq != got.Freq {
		c.add(DiffLFU, db, string(want.Key), "lfu freq %d -> %d", want.Freq, got.Freq)
	}
	c.compareValue(db, want, got)
}

func expirationEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UnixNano() == b.UnixNano()
}

func formatTime(t *time.Time) string {
	if t == nil {
		return "<persistent>"
	}
	return fmt.Sprintf("%d", t.UnixNano()/int64(time.Millisecond))
}

func (c *comparator) compareValue(db int, want, got *KeySnapshot) {
	key := string(want.Key)
	switch {
	case want.Value.String != nil:
		if got.Value.String == nil || !bytesEqual(want.Value.String.Bytes, got.Value.String.Bytes) {
			c.add(DiffStringValue, db, key, "string value mismatch")
		}
	case want.Value.List != nil:
		if got.Value.List == nil || !bytesMatrixEqual(want.Value.List.Items, got.Value.List.Items) {
			c.add(DiffListValue, db, key, "list items mismatch")
		}
	case want.Value.Set != nil:
		if got.Value.Set == nil || !bytesMatrixEqual(want.Value.Set.Members, got.Value.Set.Members) {
			c.add(DiffSetValue, db, key, "set members mismatch")
		}
	case want.Value.Hash != nil:
		c.compareHash(db, key, want.Value.Hash, got.Value.Hash)
	case want.Value.ZSet != nil:
		c.compareZSet(db, key, want.Value.ZSet, got.Value.ZSet)
	case want.Value.Stream != nil:
		c.compareStream(db, key, want.Value.Stream, got.Value.Stream)
	case want.Value.Function != nil:
		if got.Value.Function == nil || want.Value.Function.Lua != got.Value.Function.Lua {
			c.add(DiffFunction, db, key, "function library payload mismatch")
		}
	default:
		c.add(DiffUnsupportedValue, db, key, "value type %s has no semantic comparator", want.Type)
	}
}

func (c *comparator) compareHash(db int, key string, want, got *HashValue) {
	if got == nil {
		c.add(DiffHashValue, db, key, "hash missing")
		return
	}
	gotMap := make(map[string][]byte, len(got.Fields))
	for _, f := range got.Fields {
		gotMap[string(f.Field)] = f.Value
	}
	for _, wf := range want.Fields {
		gv, ok := gotMap[string(wf.Field)]
		if !ok {
			c.add(DiffHashValue, db, key, "field %q missing", safePrint(wf.Field))
			continue
		}
		if !bytesEqual(wf.Value, gv) {
			c.add(DiffHashValue, db, key, "field %q value mismatch", safePrint(wf.Field))
		}
	}
	if len(want.Fields) != len(got.Fields) {
		c.add(DiffHashValue, db, key, "field count %d -> %d", len(want.Fields), len(got.Fields))
	}
	wantExp := len(want.FieldExpiration)
	gotExp := len(got.FieldExpiration)
	if wantExp != gotExp {
		c.add(DiffHashFieldExpire, db, key, "HFE entry count %d -> %d", wantExp, gotExp)
	}
	for f, wv := range want.FieldExpiration {
		gv, ok := got.FieldExpiration[f]
		if !ok {
			c.add(DiffHashFieldExpire, db, key, "field %q expiration dropped", f)
			continue
		}
		if wv != gv {
			c.add(DiffHashFieldExpire, db, key, "field %q expiration %d -> %d", f, wv, gv)
		}
	}
}

func (c *comparator) compareZSet(db int, key string, want, got *ZSetValue) {
	if got == nil {
		c.add(DiffZSetValue, db, key, "zset missing")
		return
	}
	if len(want.Entries) != len(got.Entries) {
		c.add(DiffZSetValue, db, key, "member count %d -> %d", len(want.Entries), len(got.Entries))
	}
	gotMap := make(map[string]float64, len(got.Entries))
	for _, e := range got.Entries {
		gotMap[string(e.Member)] = e.Score
	}
	for _, we := range want.Entries {
		gs, ok := gotMap[string(we.Member)]
		if !ok {
			c.add(DiffZSetValue, db, key, "member %q missing", safePrint(we.Member))
			continue
		}
		// bit-for-bit score equality; NaN equals NaN here on purpose since
		// the on-wire IEEE754 bits must be preserved.
		if math.Float64bits(we.Score) != math.Float64bits(gs) {
			c.add(DiffZSetValue, db, key, "member %q score %v -> %v", safePrint(we.Member), we.Score, gs)
		}
	}
}

func (c *comparator) compareStream(db int, key string, want, got *StreamValue) {
	if got == nil {
		c.add(DiffStreamEntry, db, key, "stream missing")
		return
	}
	if want.Version != got.Version {
		c.add(DiffStreamVersion, db, key, "stream encoding version %d -> %d", want.Version, got.Version)
	}
	if want.Length != got.Length || want.LastID != got.LastID ||
		want.AddedEntriesCount != got.AddedEntriesCount ||
		!streamIDPtrEqual(want.FirstID, got.FirstID) ||
		!streamIDPtrEqual(want.MaxDeletedID, got.MaxDeletedID) {
		c.add(DiffStreamMetadata, db, key,
			"metadata len %d->%d last %s->%s first %s->%s maxdel %s->%s added %d->%d",
			want.Length, got.Length,
			want.LastID, got.LastID,
			streamIDPtrText(want.FirstID), streamIDPtrText(got.FirstID),
			streamIDPtrText(want.MaxDeletedID), streamIDPtrText(got.MaxDeletedID),
			want.AddedEntriesCount, got.AddedEntriesCount)
	}
	if len(want.Entries) != len(got.Entries) {
		c.add(DiffStreamEntry, db, key, "entry node count %d -> %d", len(want.Entries), len(got.Entries))
	} else {
		for i := range want.Entries {
			if !streamEntryEqual(want.Entries[i], got.Entries[i]) {
				c.add(DiffStreamEntry, db, key, "entry node %d mismatch (firstMsgId %s -> %s)",
					i, want.Entries[i].FirstMsgID, got.Entries[i].FirstMsgID)
			}
		}
	}
	c.compareStreamGroups(db, key, want.Groups, got.Groups)
}

func (c *comparator) compareStreamGroups(db int, key string, want, got []StreamGroup) {
	gotMap := make(map[string]StreamGroup, len(got))
	for _, g := range got {
		gotMap[g.Name] = g
	}
	for _, wg := range want {
		gg, ok := gotMap[wg.Name]
		if !ok {
			c.add(DiffStreamGroup, db, key, "group %q missing", wg.Name)
			continue
		}
		if wg.LastID != gg.LastID || wg.EntriesRead != gg.EntriesRead {
			c.add(DiffStreamGroup, db, key, "group %q metadata mismatch", wg.Name)
		}
		if len(wg.Pending) != len(gg.Pending) {
			c.add(DiffStreamPEL, db, key, "group %q global PEL size %d -> %d", wg.Name, len(wg.Pending), len(gg.Pending))
		} else {
			for i := range wg.Pending {
				if wg.Pending[i] != gg.Pending[i] {
					c.add(DiffStreamPEL, db, key, "group %q PEL[%d] %s != %s",
						wg.Name, i, wg.Pending[i].ID, gg.Pending[i].ID)
				}
			}
		}
		c.compareStreamConsumers(db, key, wg, gg)
	}
}

func (c *comparator) compareStreamConsumers(db int, key string, want, got StreamGroup) {
	gotConsumers := make(map[string]StreamConsumer, len(got.Consumers))
	for _, cs := range got.Consumers {
		gotConsumers[cs.Name] = cs
	}
	for _, wc := range want.Consumers {
		gc, ok := gotConsumers[wc.Name]
		if !ok {
			c.add(DiffStreamConsumer, db, key, "consumer %q/%q missing", want.Name, wc.Name)
			continue
		}
		if wc.SeenTime != gc.SeenTime || wc.ActiveTime != gc.ActiveTime {
			c.add(DiffStreamConsumer, db, key, "consumer %q/%q timing metadata mismatch", want.Name, wc.Name)
		}
		if len(wc.Pending) != len(gc.Pending) {
			c.add(DiffStreamPEL, db, key, "consumer %q/%q PEL size %d -> %d",
				want.Name, wc.Name, len(wc.Pending), len(gc.Pending))
			continue
		}
		for i, id := range wc.Pending {
			if id != gc.Pending[i] {
				c.add(DiffStreamPEL, db, key, "consumer %q/%q PEL[%d] mismatch",
					want.Name, wc.Name, i)
			}
		}
	}
}

func streamEntryEqual(a, b StreamEntry) bool {
	if a.FirstMsgID != b.FirstMsgID || !bytesMatrixEqual(a.Fields, b.Fields) {
		return false
	}
	if len(a.Msgs) != len(b.Msgs) {
		return false
	}
	for i := range a.Msgs {
		am, bm := a.Msgs[i], b.Msgs[i]
		if am.ID != bm.ID || am.Deleted != bm.Deleted || len(am.Fields) != len(bm.Fields) {
			return false
		}
		for j := range am.Fields {
			if !bytesEqual(am.Fields[j].Field, bm.Fields[j].Field) ||
				!bytesEqual(am.Fields[j].Value, bm.Fields[j].Value) {
				return false
			}
		}
	}
	return true
}

func streamIDPtrEqual(a, b *StreamID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func streamIDPtrText(id *StreamID) string {
	if id == nil {
		return "<nil>"
	}
	return id.String()
}

// String renders a stream id in the canonical ms-sequence form.
func (id StreamID) String() string {
	return fmt.Sprintf("%d-%d", id.Ms, id.Sequence)
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

func bytesMatrixEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func safePrint(b []byte) string {
	s := string(b)
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	return s
}
