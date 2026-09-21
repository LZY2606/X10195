package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/hdt3213/rdb/model"
)

// canonOptions controls which known-unrepresentable fields are excluded from
// comparison. They are only set when the encode stage recorded the matching
// structured loss, so nothing is skipped silently.
type canonOptions struct {
	ignoreFunctions bool
	ignoreLRU       bool
}

type cObj struct {
	Type     string      `json:"type"`
	DB       int         `json:"db"`
	Key      string      `json:"key"` // hex encoded raw key bytes
	ExpireMs int64       `json:"expireMs"`
	Idle     int64       `json:"idle"`
	Freq     int64       `json:"freq"`
	Value    string      `json:"value,omitempty"`  // hex payload for string/aux/functions
	Values   []string    `json:"values,omitempty"` // hex values, ordered for lists, sorted for sets
	Pairs    [][2]string `json:"pairs,omitempty"`  // sorted hex pairs for hash and zset
	FieldExp [][2]string `json:"fieldExp,omitempty"`
	Stream   *cStream    `json:"stream,omitempty"`
}

type cID [2]uint64

type cMsg struct {
	ID      cID         `json:"id"`
	Deleted bool        `json:"deleted"`
	Fields  [][2]string `json:"fields"` // sorted hex field/value pairs
}

type cNack struct {
	ID            cID    `json:"id"`
	DeliveryTime  uint64 `json:"deliveryTime"`
	DeliveryCount uint64 `json:"deliveryCount"`
}

type cConsumer struct {
	Name   string `json:"name"` // hex
	Seen   uint64 `json:"seen"`
	Active uint64 `json:"active"`
	PEL    []cID  `json:"pel,omitempty"`
}

type cGroup struct {
	Name      string      `json:"name"` // hex
	LastID    cID         `json:"lastId"`
	EntriesRead uint64    `json:"entriesRead"`
	PEL       []cNack     `json:"pel,omitempty"`
	Consumers []cConsumer `json:"consumers,omitempty"`
}

type cStream struct {
	Version           uint     `json:"version"`
	Length            uint64   `json:"length"`
	LastID            cID      `json:"lastId"`
	FirstID           cID      `json:"firstId"`
	MaxDeletedID      cID      `json:"maxDeletedId"`
	AddedEntriesCount uint64   `json:"addedEntriesCount"`
	Msgs              []cMsg   `json:"msgs"`
	Groups            []cGroup `json:"groups,omitempty"`
}

func hexBytes(b []byte) string { return hex.EncodeToString(b) }
func hexStr(s string) string   { return hex.EncodeToString([]byte(s)) }

func streamIDOf(id *model.StreamId) cID {
	if id == nil {
		return cID{0, 0}
	}
	return cID{id.Ms, id.Sequence}
}

func sortPairs(pairs [][2]string) {
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
}

func canonStreamOf(obj *model.StreamObject) *cStream {
	cs := &cStream{
		Version:           obj.Version,
		Length:            obj.Length,
		LastID:            streamIDOf(obj.LastId),
		FirstID:           streamIDOf(obj.FirstId),
		MaxDeletedID:      streamIDOf(obj.MaxDeletedId),
		AddedEntriesCount: obj.AddedEntriesCount,
	}
	for _, entry := range obj.Entries {
		for _, msg := range entry.Msgs {
			cm := cMsg{ID: streamIDOf(msg.Id), Deleted: msg.Deleted}
			for f, v := range msg.Fields {
				cm.Fields = append(cm.Fields, [2]string{hexStr(f), hexStr(v)})
			}
			sortPairs(cm.Fields)
			cs.Msgs = append(cs.Msgs, cm)
		}
	}
	sort.Slice(cs.Msgs, func(i, j int) bool {
		if cs.Msgs[i].ID[0] != cs.Msgs[j].ID[0] {
			return cs.Msgs[i].ID[0] < cs.Msgs[j].ID[0]
		}
		return cs.Msgs[i].ID[1] < cs.Msgs[j].ID[1]
	})
	for _, g := range obj.Groups {
		cg := cGroup{
			Name:        hexStr(g.Name),
			LastID:      streamIDOf(g.LastId),
			EntriesRead: g.EntriesRead,
		}
		for _, nack := range g.Pending {
			cg.PEL = append(cg.PEL, cNack{
				ID:            streamIDOf(nack.Id),
				DeliveryTime:  nack.DeliveryTime,
				DeliveryCount: nack.DeliveryCount,
			})
		}
		sort.Slice(cg.PEL, func(i, j int) bool {
			if cg.PEL[i].ID[0] != cg.PEL[j].ID[0] {
				return cg.PEL[i].ID[0] < cg.PEL[j].ID[0]
			}
			return cg.PEL[i].ID[1] < cg.PEL[j].ID[1]
		})
		for _, consumer := range g.Consumers {
			cc := cConsumer{
				Name:   hexStr(consumer.Name),
				Seen:   consumer.SeenTime,
				Active: consumer.ActiveTime,
			}
			for _, id := range consumer.Pending {
				cc.PEL = append(cc.PEL, streamIDOf(id))
			}
			sort.Slice(cc.PEL, func(i, j int) bool {
				if cc.PEL[i][0] != cc.PEL[j][0] {
					return cc.PEL[i][0] < cc.PEL[j][0]
				}
				return cc.PEL[i][1] < cc.PEL[j][1]
			})
			cg.Consumers = append(cg.Consumers, cc)
		}
		sort.Slice(cg.Consumers, func(i, j int) bool { return cg.Consumers[i].Name < cg.Consumers[j].Name })
		cs.Groups = append(cs.Groups, cg)
	}
	sort.Slice(cs.Groups, func(i, j int) bool { return cs.Groups[i].Name < cs.Groups[j].Name })
	return cs
}

// canonObj renders an object into a deterministic canonical JSON form. The
// second return value is false when the object must be excluded from the
// sequence comparison (db-size hints are compared separately).
func canonObj(o model.RedisObject, opts canonOptions) (string, bool) {
	if _, ok := o.(*model.FunctionsObject); ok && opts.ignoreFunctions {
		return "", false
	}
	if _, ok := o.(*model.DBSizeObject); ok {
		return "", false
	}
	c := &cObj{
		Type:     o.GetType(),
		DB:       o.GetDBIndex(),
		Key:      hexStr(o.GetKey()),
		ExpireMs: -1,
		Idle:     -1,
		Freq:     -1,
	}
	if exp := o.GetExpiration(); exp != nil {
		c.ExpireMs = exp.UnixNano() / int64(time.Millisecond)
	}
	if !opts.ignoreLRU {
		c.Idle = o.GetIdleTime()
		c.Freq = o.GetFreq()
	}
	switch obj := o.(type) {
	case *model.StringObject:
		c.Value = hexBytes(obj.Value)
	case *model.ListObject:
		for _, v := range obj.Values {
			c.Values = append(c.Values, hexBytes(v))
		}
	case *model.SetObject:
		for _, v := range obj.Members {
			c.Values = append(c.Values, hexBytes(v))
		}
		sort.Strings(c.Values)
	case *model.HashObject:
		for f, v := range obj.Hash {
			c.Pairs = append(c.Pairs, [2]string{hexStr(f), hexBytes(v)})
		}
		sortPairs(c.Pairs)
		for f, exp := range obj.FieldExpirations {
			c.FieldExp = append(c.FieldExp, [2]string{hexStr(f), strconv.FormatInt(exp, 10)})
		}
		sortPairs(c.FieldExp)
	case *model.ZSetObject:
		for _, e := range obj.Entries {
			c.Pairs = append(c.Pairs, [2]string{hexStr(e.Member), strconv.FormatUint(math.Float64bits(e.Score), 16)})
		}
		sortPairs(c.Pairs)
	case *model.StreamObject:
		c.Stream = canonStreamOf(obj)
	case *model.AuxObject:
		c.Value = hexStr(obj.Value)
	case *model.FunctionsObject:
		c.Value = hexStr(obj.FunctionsLua)
	default:
		c.Value = fmt.Sprintf("unsupported:%T", o)
	}
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Sprintf("canon-error:%v", err), true
	}
	return string(data), true
}

// maxDiffs bounds how many differences are reported per fixture.
const maxDiffs = 20

// compareSequences semantically compares two decoded object streams. Map and
// set orderings are normalized; db numbers, raw key bytes, expiration
// precision, stream ids, group/consumer/PEL ownership, function libraries and
// encoding-carried semantics (stream version, hash field expirations) are all
// compared exactly.
func compareSequences(first, second []model.RedisObject, opts canonOptions) []string {
	canonFirst := canonSequence(first, opts)
	canonSecond := canonSequence(second, opts)
	var diffs []string
	if len(canonFirst) != len(canonSecond) {
		diffs = append(diffs, fmt.Sprintf("object count differs: first decode has %d, second decode has %d",
			len(canonFirst), len(canonSecond)))
	}
	shared := len(canonFirst)
	if len(canonSecond) < shared {
		shared = len(canonSecond)
	}
	for i := 0; i < shared && len(diffs) < maxDiffs; i++ {
		if canonFirst[i] != canonSecond[i] {
			diffs = append(diffs, fmt.Sprintf("object[%d] differs:\n  first:  %s\n  second: %s",
				i, truncate(canonFirst[i], 400), truncate(canonSecond[i], 400)))
		}
	}
	return diffs
}

func canonSequence(objects []model.RedisObject, opts canonOptions) []string {
	var out []string
	for _, o := range objects {
		if c, keep := canonObj(o, opts); keep {
			out = append(out, c)
		}
	}
	return out
}

// compareDBHints verifies that every resize-db hint present in the first
// decode survives the round trip. Hints for empty dbs that the encoder
// structurally cannot emit are covered by the empty-db-dropped loss instead.
func compareDBHints(first, second []model.RedisObject, droppedEmptyDBs map[int]bool) []string {
	hintsOf := func(objects []model.RedisObject) map[int][2]uint64 {
		hints := map[int][2]uint64{}
		for _, o := range objects {
			if hint, ok := o.(*model.DBSizeObject); ok {
				hints[hint.DB] = [2]uint64{hint.KeyCount, hint.TTLCount}
			}
		}
		return hints
	}
	firstHints := hintsOf(first)
	secondHints := hintsOf(second)
	var diffs []string
	dbs := make([]int, 0, len(firstHints))
	for db := range firstHints {
		dbs = append(dbs, db)
	}
	sort.Ints(dbs)
	for _, db := range dbs {
		if droppedEmptyDBs[db] {
			continue
		}
		secondHint, ok := secondHints[db]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("db %d resize hint lost in re-encoded file", db))
			continue
		}
		if firstHints[db] != secondHint {
			diffs = append(diffs, fmt.Sprintf("db %d resize hint differs: first=%v second=%v", db, firstHints[db], secondHint))
		}
	}
	return diffs
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
