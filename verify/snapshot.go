package main

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// snapshot is the fully decoded, in-order representation of one RDB file.
type snapshot struct {
	flavor    string // "REDIS" or "VALKEY"
	version   int
	dbs       []int
	aux       []kvPair
	functions [][]byte // payloads of RDB_OPCODE_FUNCTION, compared byte-exact
	resize    map[int]dbResize
	objects   []model.RedisObject
}

type kvPair struct {
	key   string
	value string
}

type dbResize struct {
	keyCount uint64
	ttlCount uint64
}

type objectKey struct {
	db  int
	key string
}

func decodeSnapshot(data []byte) (*snapshot, *core.Decoder, error) {
	dec := core.NewDecoder(bytes.NewReader(data)).WithSpecialOpCode()
	snap := &snapshot{resize: make(map[int]dbResize)}
	var currentDB int
	err := dec.Parse(func(object model.RedisObject) bool {
		switch o := object.(type) {
		case *model.AuxObject:
			snap.aux = append(snap.aux, kvPair{key: o.Key, value: o.Value})
		case *model.FunctionsObject:
			snap.functions = append(snap.functions, []byte(o.FunctionsLua))
		case *model.DBSizeObject:
			snap.resize[currentDB] = dbResize{keyCount: o.KeyCount, ttlCount: o.TTLCount}
		default:
			snap.objects = append(snap.objects, object)
		}
		return true
	})
	if err != nil {
		return nil, dec, err
	}
	if dec.IsValkey() {
		snap.flavor = "VALKEY"
	} else {
		snap.flavor = "REDIS"
	}
	snap.version = dec.GetRDBVersion()
	snap.dbs = dec.GetDBIndexes()
	return snap, dec, nil
}

// optionsForObject reconstructs the per-object writer options (TTL / LRU / LFU).
func optionsForObject(obj model.RedisObject) []interface{} {
	var opts []interface{}
	if exp := obj.GetExpiration(); exp != nil {
		ms := uint64(exp.UnixNano()) / uint64(time.Millisecond)
		opts = append(opts, core.WithTTL(ms))
	}
	base := obj.GetBase()
	if base.IdleTime != nil {
		opts = append(opts, core.WithIdleTime(uint64(*base.IdleTime)))
	}
	if base.Freq != nil {
		opts = append(opts, core.WithFreq(uint8(*base.Freq)))
	}
	return opts
}

// reEncode serializes the snapshot back to RDB bytes using the project encoder.
// The header flavor (REDIS vs VALKEY) is preserved so Valkey hash2 and slot
// opcodes round-trip; the version number itself is whatever the encoder emits.
func (s *snapshot) reEncode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	var enc *core.Encoder
	if s.flavor == "VALKEY" {
		enc = core.NewEncoderValkey(buf)
	} else {
		enc = core.NewEncoder(buf)
	}
	if err := enc.WriteHeader(); err != nil {
		return nil, err
	}
	for _, a := range s.aux {
		if err := enc.WriteAux(a.key, a.value); err != nil {
			return nil, fmt.Errorf("write aux %q: %w", a.key, err)
		}
	}
	for _, payload := range s.functions {
		if err := enc.WriteFunctions(string(payload)); err != nil {
			return nil, fmt.Errorf("write functions: %w", err)
		}
	}
	// stable grouping by db index; objects inside a db keep decode order
	dbOrder := make([]int, 0, len(s.dbs))
	dbOrder = append(dbOrder, s.dbs...)
	sort.Ints(dbOrder)
	byDB := make(map[int][]model.RedisObject)
	for _, obj := range s.objects {
		byDB[obj.GetDBIndex()] = append(byDB[obj.GetDBIndex()], obj)
	}
	// objects on a db that never emitted SELECTDB still need one
	for db := range byDB {
		if _, ok := s.resize[db]; !ok {
			if res := indexOfInt(dbOrder, db); res < 0 {
				dbOrder = append(dbOrder, db)
			}
		}
	}
	sort.Ints(dbOrder)
	for _, db := range dbOrder {
		objs := byDB[db]
		var ttlCount uint64
		for _, obj := range objs {
			if obj.GetExpiration() != nil {
				ttlCount++
			}
		}
		if r, ok := s.resize[db]; ok {
			if err := enc.WriteDBHeader(uint(db), r.keyCount, r.ttlCount); err != nil {
				return nil, fmt.Errorf("write db %d header: %w", db, err)
			}
		} else {
			if err := enc.WriteDBHeader(uint(db), uint64(len(objs)), ttlCount); err != nil {
				return nil, fmt.Errorf("write db %d header: %w", db, err)
			}
		}
		for _, obj := range objs {
			if err := writeObject(enc, obj); err != nil {
				return nil, fmt.Errorf("db %d key %q: %w", db, obj.GetKey(), err)
			}
		}
	}
	if err := enc.WriteEnd(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func indexOfInt(list []int, v int) int {
	for i, x := range list {
		if x == v {
			return i
		}
	}
	return -1
}

func writeObject(enc *core.Encoder, obj model.RedisObject) error {
	opts := optionsForObject(obj)
	switch o := obj.(type) {
	case *model.StringObject:
		return enc.WriteStringObject(o.Key, o.Value, opts...)
	case *model.ListObject:
		return enc.WriteListObject(o.Key, o.Values, opts...)
	case *model.SetObject:
		return enc.WriteSetObject(o.Key, o.Members, opts...)
	case *model.HashObject:
		if len(o.FieldExpirations) > 0 {
			return enc.WriteHashMapObjectEx(o.Key, o.Hash, o.FieldExpirations, opts...)
		}
		return enc.WriteHashMapObject(o.Key, o.Hash, opts...)
	case *model.ZSetObject:
		return enc.WriteZSetObject(o.Key, o.Entries, opts...)
	case *model.StreamObject:
		return enc.WriteStreamObject(o.Key, o, opts...)
	case *model.ModuleTypeObject:
		return fmt.Errorf("module type %q cannot be re-encoded", o.ModuleType)
	default:
		return fmt.Errorf("object type %T cannot be re-encoded", obj)
	}
}

var _ io.Reader = (*bytes.Reader)(nil)
