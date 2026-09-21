package verify

// Pipeline: decode -> re-encode -> re-decode for a single fixture.
// Re-encoding always targets the flavor (Redis/Valkey) of the source header.
// Encoding representation may change (e.g. ziplist -> list), that is a
// semantic-preserving change and is reported separately by the comparator.

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// Decoded is the parsed model of an rdb byte stream.
type Decoded struct {
	Version int
	Valkey  bool
	Objects []model.RedisObject
}

// Decode fully parses rdb bytes with special opcodes (aux / dbsize /
// functions) enabled.
func Decode(data []byte) (*Decoded, error) {
	dec := core.NewDecoder(bytes.NewReader(data)).WithSpecialOpCode()
	var objs []model.RedisObject
	if err := dec.Parse(func(obj model.RedisObject) bool {
		objs = append(objs, obj)
		return true
	}); err != nil {
		return nil, err
	}
	return &Decoded{Version: dec.RDBVersion(), Valkey: dec.Valkey(), Objects: objs}, nil
}

// Reencode serializes the decoded objects back into rdb bytes. Objects are
// grouped by database in decoded order; aux fields and the function library
// blob are emitted first (matching Redis' own file layout).
func Reencode(d *Decoded) ([]byte, error) {
	var buf bytes.Buffer
	var enc *core.Encoder
	if d.Valkey {
		enc = core.NewEncoderValkey(&buf)
	} else {
		enc = core.NewEncoder(&buf)
	}
	if err := enc.WriteHeader(); err != nil {
		return nil, err
	}

	// 1. metadata: aux fields then function libraries, in original order.
	for _, obj := range d.Objects {
		switch obj.(type) {
		case *model.AuxObject, *model.FunctionsObject:
			if err := writeObject(enc, obj); err != nil {
				return nil, err
			}
		}
	}

	// 2. data-bearing objects grouped by db index.
	type dbGroup struct {
		db    int
		objs  []model.RedisObject
		keys  uint64
		ttls  uint64
	}
	groups := make([]*dbGroup, 0)
	byDB := make(map[int]*dbGroup)
	for _, obj := range d.Objects {
		switch obj.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
			continue
		}
		db := obj.GetDBIndex()
		g := byDB[db]
		if g == nil {
			g = &dbGroup{db: db}
			byDB[db] = g
			groups = append(groups, g)
		}
		g.objs = append(g.objs, obj)
		g.keys++
		if obj.GetExpiration() != nil {
			g.ttls++
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].db < groups[j].db })
	for _, g := range groups {
		if err := enc.WriteDBHeader(uint(g.db), g.keys, g.ttls); err != nil {
			return nil, err
		}
		for _, obj := range g.objs {
			if err := writeObject(enc, obj); err != nil {
				return nil, fmt.Errorf("db=%d key=%q: %w", g.db, obj.GetKey(), err)
			}
		}
	}

	if err := enc.WriteEnd(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ttlOptions(obj model.RedisObject) []interface{} {
	if exp := obj.GetExpiration(); exp != nil {
		ms := uint64(exp.UnixNano() / int64(time.Millisecond))
		return []interface{}{core.WithTTL(ms)}
	}
	return nil
}

func writeObject(enc *core.Encoder, obj model.RedisObject) error {
	switch x := obj.(type) {
	case *model.AuxObject:
		return enc.WriteAux(x.Key, x.Value)
	case *model.FunctionsObject:
		return enc.WriteFunctions(x.FunctionsLua)
	case *model.DBSizeObject:
		return nil // re-generated from actual db contents
	case *model.StringObject:
		return enc.WriteStringObject(x.Key, x.Value, ttlOptions(obj)...)
	case *model.ListObject:
		return enc.WriteListObject(x.Key, x.Values, ttlOptions(obj)...)
	case *model.SetObject:
		return enc.WriteSetObject(x.Key, x.Members, ttlOptions(obj)...)
	case *model.HashObject:
		if len(x.FieldExpirations) > 0 {
			return enc.WriteHashMapObjectEx(x.Key, x.Hash, x.FieldExpirations, ttlOptions(obj)...)
		}
		return enc.WriteHashMapObject(x.Key, x.Hash, ttlOptions(obj)...)
	case *model.ZSetObject:
		return enc.WriteZSetObject(x.Key, x.Entries, ttlOptions(obj)...)
	case *model.StreamObject:
		return enc.WriteStreamObject(x.Key, x, ttlOptions(obj)...)
	case *model.ModuleTypeObject:
		return fmt.Errorf("module type %q cannot be re-encoded", x.ModuleType)
	default:
		return fmt.Errorf("unsupported object type %T", obj)
	}
}
