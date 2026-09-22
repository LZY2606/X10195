package verify

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// decodeAll decodes a whole RDB file, including aux fields, db size hints
// and function libraries.
func decodeAll(path string) ([]model.RedisObject, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	return decodeAllReader(f)
}

func decodeAllBytes(data []byte) ([]model.RedisObject, error) {
	return decodeAllReader(bytes.NewReader(data))
}

func decodeAllReader(r io.Reader) ([]model.RedisObject, error) {
	dec := core.NewDecoder(r).WithSpecialOpCode()
	var objects []model.RedisObject
	err := dec.Parse(func(object model.RedisObject) bool {
		objects = append(objects, object)
		return true
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// reencode writes the decoded objects back into a new RDB file. Function
// libraries have no encoder API and are dropped here; the compare phase
// reports the loss.
func reencode(objects []model.RedisObject, valkey bool) ([]byte, error) {
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
	for _, o := range objects {
		if aux, ok := o.(*model.AuxObject); ok {
			if err := enc.WriteAux(aux.Key, aux.Value); err != nil {
				return nil, err
			}
		}
	}

	type dbGroup struct {
		db   int
		objs []model.RedisObject
	}
	var groups []*dbGroup
	byDB := make(map[int]*dbGroup)
	for _, o := range objects {
		switch o.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
			continue
		}
		db := o.GetDBIndex()
		g := byDB[db]
		if g == nil {
			g = &dbGroup{db: db}
			byDB[db] = g
			groups = append(groups, g)
		}
		g.objs = append(g.objs, o)
	}
	for _, g := range groups {
		var keyCount, ttlCount uint64
		for _, o := range g.objs {
			keyCount++
			if o.GetExpiration() != nil {
				ttlCount++
			}
		}
		if err := enc.WriteDBHeader(uint(g.db), keyCount, ttlCount); err != nil {
			return nil, err
		}
		for _, o := range g.objs {
			if err := writeObject(enc, o); err != nil {
				return nil, err
			}
		}
	}
	if err := enc.WriteEnd(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeObject(enc *core.Encoder, o model.RedisObject) error {
	var opts []interface{}
	if exp := o.GetExpiration(); exp != nil {
		ms := exp.UnixMilli()
		if ms < 0 {
			return fmt.Errorf("key %q expires before epoch, cannot re-encode", o.GetKey())
		}
		opts = append(opts, core.WithTTL(uint64(ms)))
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
	default:
		return fmt.Errorf("encoder cannot represent object type %q (key %q)", o.GetType(), o.GetKey())
	}
}
