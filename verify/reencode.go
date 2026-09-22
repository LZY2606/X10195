package verify

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// decodeFile fully decodes an RDB, returning version, valkey flag and objects.
func decodeFile(r io.Reader) (int, bool, []model.RedisObject, error) {
	dec := core.NewDecoder(r).WithSpecialOpCode()
	var objects []model.RedisObject
	err := dec.Parse(func(obj model.RedisObject) bool {
		objects = append(objects, obj)
		return true
	})
	return dec.GetRDBVersion(), dec.IsValkey(), objects, err
}

// reencode replays a decoded object stream into a fresh RDB. It keeps the
// original VALKEY/REDIS flavor but always uses the encoder's current header
// version, which is itself a reported (manifest-acknowledged) loss.
func reencode(objects []model.RedisObject, valkey bool) ([]byte, error) {
	var aux []*model.AuxObject
	var funcs []*model.FunctionsObject
	byDB := map[int][]model.RedisObject{}
	var dbOrder []int
	seen := map[int]struct{}{}
	for _, obj := range objects {
		switch o := obj.(type) {
		case *model.AuxObject:
			aux = append(aux, o)
			continue
		case *model.FunctionsObject:
			funcs = append(funcs, o)
			continue
		case *model.DBSizeObject:
			continue
		}
		db := obj.GetDBIndex()
		if _, ok := seen[db]; !ok {
			seen[db] = struct{}{}
			dbOrder = append(dbOrder, db)
		}
		byDB[db] = append(byDB[db], obj)
	}
	sort.Ints(dbOrder)

	buf := bytes.NewBuffer(nil)
	var enc *core.Encoder
	if valkey {
		enc = core.NewEncoderValkey(buf)
	} else {
		enc = core.NewEncoder(buf)
	}
	if err := enc.WriteHeader(); err != nil {
		return nil, err
	}
	for _, a := range aux {
		if err := enc.WriteAux(a.Key, a.Value); err != nil {
			return nil, err
		}
	}
	for _, f := range funcs {
		if err := enc.WriteFunctions(f.FunctionsLua); err != nil {
			return nil, err
		}
	}
	for _, db := range dbOrder {
		objs := byDB[db]
		ttlCount := 0
		for _, o := range objs {
			if o.GetExpiration() != nil {
				ttlCount++
			}
		}
		if err := enc.WriteDBHeader(uint(db), uint64(len(objs)), uint64(ttlCount)); err != nil {
			return nil, err
		}
		for _, obj := range objs {
			if err := writeObject(enc, obj); err != nil {
				return nil, err
			}
		}
	}
	if err := enc.WriteEnd(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func objectOptions(obj model.RedisObject) []interface{} {
	var opts []interface{}
	if exp := obj.GetExpiration(); exp != nil {
		opts = append(opts, core.WithTTL(uint64(exp.UnixNano()/int64(time.Millisecond))))
	}
	if ev, ok := obj.(model.EvictionInfo); ok {
		if idle := ev.GetIdleTime(); idle >= 0 {
			opts = append(opts, core.WithIdle(uint64(idle)))
		}
		if freq := ev.GetFreq(); freq >= 0 && freq <= 255 {
			opts = append(opts, core.WithFreq(uint8(freq)))
		}
	}
	return opts
}

func writeObject(enc *core.Encoder, obj model.RedisObject) error {
	opts := objectOptions(obj)
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
		return fmt.Errorf("module type %q re-encoding is not supported", o.ModuleType)
	default:
		return fmt.Errorf("unsupported object type %T", obj)
	}
}
