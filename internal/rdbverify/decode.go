package rdbverify

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// snapshot is everything collected from parsing one RDB file.
type snapshot struct {
	header    headerInfo
	aux       []*model.AuxObject
	dbsizes   []*model.DBSizeObject
	objects   []model.RedisObject
	functions []*model.FunctionsObject
}

// decodeSnapshot parses path with special opcodes enabled so that AUX,
// RESIZEDB and function library records are visible.
func decodeSnapshot(path string) (*snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	hdr, err := parseHeader(data[:9])
	if err != nil {
		return nil, err
	}
	snap := &snapshot{header: hdr}
	dec := core.NewDecoder(bytes.NewReader(data)).WithSpecialOpCode()
	err = dec.Parse(func(obj model.RedisObject) bool {
		switch o := obj.(type) {
		case *model.AuxObject:
			snap.aux = append(snap.aux, o)
		case *model.DBSizeObject:
			snap.dbsizes = append(snap.dbsizes, o)
		case *model.FunctionsObject:
			snap.functions = append(snap.functions, o)
		default:
			snap.objects = append(snap.objects, o)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// groupedObjects groups keyed objects by DB index, preserving a sorted
// deterministic view for encoding and comparison.
func (s *snapshot) groupedObjects() map[int][]model.RedisObject {
	groups := make(map[int][]model.RedisObject)
	for _, obj := range s.objects {
		groups[obj.GetDBIndex()] = append(groups[obj.GetDBIndex()], obj)
	}
	for db := range groups {
		sort.SliceStable(groups[db], func(i, j int) bool {
			return groups[db][i].GetKey() < groups[db][j].GetKey()
		})
	}
	return groups
}

// reencode serialises the snapshot using this project's encoder.
//
// The encoder only supports REDIS 11 output and a subset of special records
// (no function libraries, no LRU/LFU opcodes). Such representational gaps are
// returned as structured losses attached to the result rather than swallowed;
// the gate decides whether they are acceptable.
func (s *snapshot) reencode() ([]byte, []*ExpectedLoss, error) {
	var losses []*ExpectedLoss
	buf := bytes.NewBuffer(nil)

	// Pick the matching encoder dialect. Valkey hash2 (field-expiration) only
	// round-trips with the Valkey encoder.
	valkey := s.header.magic == "VALKEY"
	enc := core.NewEncoder(buf)
	if valkey {
		enc = core.NewEncoderValkey(buf)
	}
	if err := enc.WriteHeader(); err != nil {
		return nil, nil, err
	}

	// AUX fields are re-emitted in decoded order.
	for _, aux := range s.aux {
		if err := enc.WriteAux(aux.GetKey(), aux.Value); err != nil {
			return nil, nil, fmt.Errorf("write aux %q: %w", aux.GetKey(), err)
		}
	}

	if len(s.functions) > 0 {
		// The encoder has no opcode 245 writer; dropping function libraries
		// would silently destroy data, so surface it as a structured loss and
		// skip the bytes instead of emitting a corrupt file.
		for _, fn := range s.functions {
			losses = append(losses, &ExpectedLoss{
				Code:   LossFunctionLibrary,
				Detail: fmt.Sprintf("function library payload (%d bytes) cannot be re-encoded", len(fn.FunctionsLua)),
			})
		}
	}

	groups := s.groupedObjects()
	dbIndexes := make([]int, 0, len(groups))
	for db := range groups {
		dbIndexes = append(dbIndexes, db)
	}
	sort.Ints(dbIndexes)

	for _, db := range dbIndexes {
		objs := groups[db]
		if len(objs) == 0 {
			losses = append(losses, &ExpectedLoss{
				Code:   LossEmptyDB,
				Detail: fmt.Sprintf("database %d contains no keyed objects and cannot be represented by the encoder", db),
			})
			continue
		}
		var ttlCount uint64
		for _, obj := range objs {
			if obj.GetExpiration() != nil {
				ttlCount++
			}
		}
		if err := enc.WriteDBHeader(uint(db), uint64(len(objs)), ttlCount); err != nil {
			return nil, nil, fmt.Errorf("write db %d header: %w", db, err)
		}
		for _, obj := range objs {
			if err := s.writeObject(enc, obj, &losses); err != nil {
				return nil, nil, err
			}
		}
	}

	if err := enc.WriteEnd(); err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), losses, nil
}

// writeObject dispatches one keyed object to the type-specific encoder method.
func (s *snapshot) writeObject(enc *core.Encoder, obj model.RedisObject, losses *[]*ExpectedLoss) error {
	var opts []interface{}
	if exp := obj.GetExpiration(); exp != nil {
		opts = append(opts, core.WithTTL(uint64(exp.UnixNano()/int64(time.Millisecond))))
	}
	if info, ok := obj.(model.EvictionInfo); ok {
		switch {
		case info.GetIdleTime() >= 0:
			*losses = append(*losses, &ExpectedLoss{
				Code:   LossEvictionMeta,
				Detail: fmt.Sprintf("key %q LRU idle=%d is not preserved by the encoder", obj.GetKey(), info.GetIdleTime()),
			})
		case info.GetFreq() >= 0:
			*losses = append(*losses, &ExpectedLoss{
				Code:   LossEvictionMeta,
				Detail: fmt.Sprintf("key %q LFU freq=%d is not preserved by the encoder", obj.GetKey(), info.GetFreq()),
			})
		}
	}

	switch o := obj.(type) {
	case *model.StringObject:
		return enc.WriteStringObject(o.GetKey(), o.Value, opts...)
	case *model.ListObject:
		return enc.WriteListObject(o.GetKey(), o.Values, opts...)
	case *model.SetObject:
		return enc.WriteSetObject(o.GetKey(), o.Members, opts...)
	case *model.HashObject:
		if len(o.FieldExpirations) > 0 {
			return enc.WriteHashMapObjectEx(o.GetKey(), o.Hash, o.FieldExpirations, opts...)
		}
		return enc.WriteHashMapObject(o.GetKey(), o.Hash, opts...)
	case *model.ZSetObject:
		return enc.WriteZSetObject(o.GetKey(), o.Entries, opts...)
	case *model.StreamObject:
		return enc.WriteStreamObject(o.GetKey(), o, opts...)
	case *model.ModuleTypeObject:
		return fmt.Errorf("%w: key %q module type %q", ErrUnsupportedEncoding, o.GetKey(), o.ModuleType)
	default:
		return fmt.Errorf("%w: key %q type %T", ErrUnsupportedEncoding, o.GetKey(), obj)
	}
}
