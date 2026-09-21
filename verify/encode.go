package main

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// loss kinds produced when the encoder cannot losslessly represent an input.
const (
	lossFunctionsDropped = "functions-dropped"
	lossEmptyDBDropped   = "empty-db-dropped"
	lossLRULFUDropped    = "lru-lfu-dropped"
)

// loss is a structured description of one semantic loss during re-encoding.
type loss struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// encodeResult carries the re-encoded bytes plus every loss that occurred.
type encodeResult struct {
	data            []byte
	losses          []loss
	droppedEmptyDBs map[int]bool
}

func (r *encodeResult) hasLoss(kind string) bool {
	for _, l := range r.losses {
		if l.Kind == kind {
			return true
		}
	}
	return false
}

type dbStat struct {
	keys uint64
	ttls uint64
}

func isDataObject(o model.RedisObject) bool {
	switch o.(type) {
	case *model.StringObject, *model.ListObject, *model.SetObject,
		*model.HashObject, *model.ZSetObject, *model.StreamObject:
		return true
	}
	return false
}

// encodeAll re-encodes decoded objects into a new rdb payload. Constructs the
// encoder cannot represent are reported as structured losses instead of being
// silently dropped.
func encodeAll(objects []model.RedisObject, valkey bool) (*encodeResult, error) {
	stats := map[int]*dbStat{}
	for _, o := range objects {
		if !isDataObject(o) {
			continue
		}
		db := o.GetDBIndex()
		if stats[db] == nil {
			stats[db] = &dbStat{}
		}
		stats[db].keys++
		if o.GetExpiration() != nil {
			stats[db].ttls++
		}
	}

	buf := bytes.NewBuffer(nil)
	var enc *core.Encoder
	if valkey {
		enc = core.NewEncoderValkey(buf)
	} else {
		enc = core.NewEncoder(buf)
	}
	if err := enc.WriteHeader(); err != nil {
		return nil, fmt.Errorf("write header: %v", err)
	}

	res := &encodeResult{droppedEmptyDBs: map[int]bool{}}
	currentDB := -1
	dbOpened := false
	pendingHints := map[int]*model.DBSizeObject{}
	var functionsDropped, lruLFUDropped int

	for _, o := range objects {
		switch obj := o.(type) {
		case *model.AuxObject:
			if dbOpened {
				return nil, fmt.Errorf("aux field %q appears after db section, cannot re-encode", obj.Key)
			}
			if err := enc.WriteAux(obj.Key, obj.Value); err != nil {
				return nil, fmt.Errorf("write aux %q: %v", obj.Key, err)
			}
		case *model.DBSizeObject:
			pendingHints[obj.DB] = obj
		case *model.FunctionsObject:
			functionsDropped++
		case *model.ModuleTypeObject:
			return nil, fmt.Errorf("cannot re-encode module object %q of type %q", obj.Key, obj.ModuleType)
		default:
			if !isDataObject(o) {
				return nil, fmt.Errorf("unsupported object type %T", o)
			}
			db := o.GetDBIndex()
			if db != currentDB {
				var keyCount, ttlCount uint64
				if hint, ok := pendingHints[db]; ok {
					keyCount, ttlCount = hint.KeyCount, hint.TTLCount
					delete(pendingHints, db)
				} else if st := stats[db]; st != nil {
					keyCount, ttlCount = st.keys, st.ttls
				}
				if err := enc.WriteDBHeader(uint(db), keyCount, ttlCount); err != nil {
					return nil, fmt.Errorf("write db %d header: %v", db, err)
				}
				currentDB = db
				dbOpened = true
			}
			if o.GetIdleTime() >= 0 || o.GetFreq() >= 0 {
				lruLFUDropped++
			}
			if err := writeDataObject(enc, o); err != nil {
				return nil, fmt.Errorf("write key %q: %v", o.GetKey(), err)
			}
		}
	}

	if len(pendingHints) > 0 {
		dbs := make([]int, 0, len(pendingHints))
		for db := range pendingHints {
			dbs = append(dbs, db)
			res.droppedEmptyDBs[db] = true
		}
		sort.Ints(dbs)
		res.losses = append(res.losses, loss{
			Kind:   lossEmptyDBDropped,
			Detail: fmt.Sprintf("dbs %v have resize hints but no keys; encoder cannot emit empty dbs", dbs),
		})
	}
	if functionsDropped > 0 {
		res.losses = append(res.losses, loss{
			Kind:   lossFunctionsDropped,
			Detail: fmt.Sprintf("%d function library object(s) dropped: encoder cannot emit RDB_OPCODE_FUNCTION", functionsDropped),
		})
	}
	if lruLFUDropped > 0 {
		res.losses = append(res.losses, loss{
			Kind:   lossLRULFUDropped,
			Detail: fmt.Sprintf("%d object(s) carry LRU/LFU metadata: encoder cannot emit idle/freq opcodes", lruLFUDropped),
		})
	}

	if err := enc.WriteEnd(); err != nil {
		return nil, fmt.Errorf("write end: %v", err)
	}
	res.data = buf.Bytes()
	return res, nil
}

func writeDataObject(enc *core.Encoder, o model.RedisObject) error {
	var opts []interface{}
	if exp := o.GetExpiration(); exp != nil {
		opts = append(opts, core.WithTTL(uint64(exp.UnixNano()/int64(time.Millisecond))))
	}
	switch obj := o.(type) {
	case *model.StringObject:
		return enc.WriteStringObject(obj.Key, obj.Value, opts...)
	case *model.ListObject:
		return enc.WriteListObject(obj.Key, obj.Values, opts...)
	case *model.SetObject:
		return enc.WriteSetObject(obj.Key, obj.Members, opts...)
	case *model.HashObject:
		if len(obj.FieldExpirations) > 0 {
			return enc.WriteHashMapObjectEx(obj.Key, obj.Hash, obj.FieldExpirations, opts...)
		}
		return enc.WriteHashMapObject(obj.Key, obj.Hash, opts...)
	case *model.ZSetObject:
		return enc.WriteZSetObject(obj.Key, obj.Entries, opts...)
	case *model.StreamObject:
		return enc.WriteStreamObject(obj.Key, obj, opts...)
	}
	return fmt.Errorf("unsupported object type %T", o)
}
