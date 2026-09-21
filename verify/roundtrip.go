package verify

import (
	"bytes"
	"fmt"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// decodeAll fully decodes one RDB payload including aux/dbsize/functions
// opcodes. The decoder never contacts anything external.
func decodeAll(data []byte) ([]model.RedisObject, error) {
	dec := core.NewDecoder(bytes.NewReader(data)).WithSpecialOpCode()
	var objs []model.RedisObject
	err := dec.Parse(func(object model.RedisObject) bool {
		objs = append(objs, object)
		return true
	})
	if err != nil {
		return nil, err
	}
	return objs, nil
}

// reencode serializes decoded objects back into RDB bytes. It preserves the
// original header flavor/version and emits the objects in parse order, which
// keeps every DB's key ordering deterministic.
func reencode(objs []model.RedisObject, flavor string, version int) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	var enc *core.Encoder
	if flavor == "valkey" {
		enc = core.NewEncoderValkey(buf)
	} else {
		enc = core.NewEncoder(buf)
		if version > 0 {
			enc.SetRDBVersion(version)
		}
	}
	if err := enc.WriteHeader(); err != nil {
		return nil, err
	}

	curDB := -1
	// RDB requires the function library payload to appear in the pre-DB aux
	// section, so buffer it and flush it before the first SELECTDB.
	var functionsPayload string
	functionsWritten := false
	flushFunctions := func() error {
		if functionsWritten || functionsPayload == "" {
			return nil
		}
		if err := enc.WriteFunctions(functionsPayload); err != nil {
			return err
		}
		functionsWritten = true
		return nil
	}
	// RESIZEDB hints are advisory; count per DB up front so emitted values are
	// accurate rather than zero-filled.
	type dbStat struct{ keys, ttls uint64 }
	stats := map[int]*dbStat{}
	var dbOrder []int
	for _, o := range objs {
		if isMetaObject(o) {
			continue
		}
		st, ok := stats[o.GetDBIndex()]
		if !ok {
			st = &dbStat{}
			stats[o.GetDBIndex()] = st
			dbOrder = append(dbOrder, o.GetDBIndex())
		}
		st.keys++
		if o.GetExpiration() != nil {
			st.ttls++
		}
	}
	statByDB := map[int]*dbStat{}
	for i, db := range dbOrder {
		_ = i
		statByDB[db] = stats[db]
	}

	for _, o := range objs {
		switch q := o.(type) {
		case *model.AuxObject:
			if err := enc.WriteAux(q.Key, q.Value); err != nil {
				return nil, err
			}
		case *model.FunctionsObject:
			functionsPayload = q.FunctionsLua
			if curDB < 0 {
				if err := flushFunctions(); err != nil {
					return nil, err
				}
			}
		case *model.DBSizeObject:
			continue
		default:
			db := o.GetDBIndex()
			if db != curDB {
				if err := flushFunctions(); err != nil {
					return nil, err
				}
				curDB = db
				st := statByDB[db]
				if err := enc.WriteDBHeader(uint(db), st.keys, st.ttls); err != nil {
					return nil, err
				}
			}
			if err := writeObject(enc, o); err != nil {
				return nil, fmt.Errorf("encode %s key %q: %w", o.GetType(), o.GetKey(), err)
			}
		}
	}
	if err := enc.WriteEnd(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func isMetaObject(o model.RedisObject) bool {
	switch o.(type) {
	case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
		return true
	}
	return false
}

func writeObject(enc *core.Encoder, o model.RedisObject) error {
	var opts []interface{}
	if e := o.GetExpiration(); e != nil {
		opts = append(opts, core.WithTTL(uint64(e.UnixNano()/int64(time.Millisecond))))
	}
	if ev, ok := o.(model.EvictionInfo); ok {
		if ev.GetIdleTime() >= 0 {
			opts = append(opts, core.WithIdle(uint64(ev.GetIdleTime())))
		}
		if ev.GetFreq() >= 0 {
			opts = append(opts, core.WithFreq(byte(ev.GetFreq())))
		}
	}
	switch q := o.(type) {
	case *model.StringObject:
		return enc.WriteStringObject(q.Key, q.Value, opts...)
	case *model.ListObject:
		return enc.WriteListObject(q.Key, q.Values, opts...)
	case *model.SetObject:
		return enc.WriteSetObject(q.Key, q.Members, opts...)
	case *model.HashObject:
		if len(q.FieldExpirations) > 0 {
			return enc.WriteHashMapObjectEx(q.Key, q.Hash, q.FieldExpirations, opts...)
		}
		return enc.WriteHashMapObject(q.Key, q.Hash, opts...)
	case *model.ZSetObject:
		return enc.WriteZSetObject(q.Key, q.Entries, opts...)
	case *model.StreamObject:
		return enc.WriteStreamObject(q.Key, q, opts...)
	default:
		return fmt.Errorf("encoder has no writer for %T", o)
	}
}
