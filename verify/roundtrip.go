package main

import (
	"fmt"
	"io"
	"os"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// decodeObjects parses a whole rdb file into memory, including special
// opcodes (aux fields, resize-db hints and function libraries).
func decodeObjects(path string) ([]model.RedisObject, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	dec := core.NewDecoder(f).WithSpecialOpCode()
	var objects []model.RedisObject
	err = dec.Parse(func(object model.RedisObject) bool {
		objects = append(objects, object)
		return true
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// isDataObject reports whether o is a key-value object living inside a db
// (as opposed to aux / resize-db / functions metadata).
func isDataObject(o model.RedisObject) bool {
	switch o.GetType() {
	case model.AuxType, model.DBSizeType, model.FunctionsType:
		return false
	}
	return true
}

// encodeObjects re-encodes decoded objects into a new rdb stream. Metadata
// the encoder cannot represent (function libraries) is skipped here and
// reported as a structured expected-loss by the comparator instead.
func encodeObjects(objects []model.RedisObject, valkey bool, w io.Writer) error {
	var enc *core.Encoder
	if valkey {
		enc = core.NewEncoderValkey(w)
	} else {
		enc = core.NewEncoder(w)
	}

	keyCount := map[int]uint64{}
	ttlCount := map[int]uint64{}
	for _, o := range objects {
		if !isDataObject(o) {
			continue
		}
		keyCount[o.GetDBIndex()]++
		if o.GetExpiration() != nil {
			ttlCount[o.GetDBIndex()]++
		}
	}

	if err := enc.WriteHeader(); err != nil {
		return err
	}
	openedDB := map[int]bool{}
	for _, o := range objects {
		var err error
		switch obj := o.(type) {
		case *model.AuxObject:
			err = enc.WriteAux(obj.Key, obj.Value)
		case *model.DBSizeObject:
			if !openedDB[obj.DB] {
				openedDB[obj.DB] = true
				err = enc.WriteDBHeader(uint(obj.DB), obj.KeyCount, obj.TTLCount)
			}
		case *model.FunctionsObject:
			// The encoder has no functions support. The comparator detects
			// the dropped library and records a structured expected-loss.
		case *model.ModuleTypeObject:
			err = fmt.Errorf("module object %q cannot be re-encoded", obj.Key)
		default:
			db := o.GetDBIndex()
			if !openedDB[db] {
				openedDB[db] = true
				if err = enc.WriteDBHeader(uint(db), keyCount[db], ttlCount[db]); err != nil {
					return err
				}
			}
			err = encodeDataObject(enc, o)
		}
		if err != nil {
			return err
		}
	}
	return enc.WriteEnd()
}

func encodeDataObject(enc *core.Encoder, o model.RedisObject) error {
	var opts []interface{}
	if exp := o.GetExpiration(); exp != nil {
		opts = append(opts, core.WithTTL(uint64(exp.UnixNano()/int64(1e6))))
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
	return fmt.Errorf("no encoder for object type %T", o)
}
