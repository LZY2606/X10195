package verify

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// decodeResult captures one decode of an RDB stream.
type decodeResult struct {
	objects []model.RedisObject
	// flavor/version observed by the decoder ("redis"/"valkey").
	flavor  string
	version int
}

// decodeRDB decates an RDB stream with aux/dbsize/functions opcodes enabled.
func decodeRDB(r io.Reader) (*decodeResult, error) {
	dec := core.NewDecoder(r).WithSpecialOpCode()
	res := &decodeResult{}
	err := dec.Parse(func(o model.RedisObject) bool {
		res.objects = append(res.objects, o)
		return true
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// decodeFile decodes an RDB file from disk.
func decodeFile(path string) (*decodeResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return decodeRDB(file)
}

// encodeRDB re-encodes a decoded object set into RDB bytes.
//
// The encoder writes a fixed header version (REDIS0011 / VALKEY080), so the
// original RDB version and any function library payload are structural
// losses surfaced as blockers rather than being silently dropped.
func encodeRDB(decoded *decodeResult, valkey bool) ([]byte, error) {
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

	// Preserve aux fields, in their original order.
	for _, o := range decoded.objects {
		aux, ok := o.(*model.AuxObject)
		if !ok {
			continue
		}
		if err := enc.WriteAux(aux.Key, aux.Value); err != nil {
			return nil, fmt.Errorf("write aux %q: %w", aux.Key, err)
		}
	}

	type dbStat struct{ keys, ttls uint64 }
	dbStats := map[int]*dbStat{}
	dbOrder := make([]int, 0)
	for _, o := range decoded.objects {
		switch o.GetType() {
		case model.AuxType, model.DBSizeType, model.FunctionsType:
			continue
		}
		st, ok := dbStats[o.GetDBIndex()]
		if !ok {
			st = &dbStat{}
			dbStats[o.GetDBIndex()] = st
			dbOrder = append(dbOrder, o.GetDBIndex())
		}
		st.keys++
		if o.GetExpiration() != nil {
			st.ttls++
		}
	}

	writtenDB := map[int]bool{}
	lastDB := -1
	writeDB := func(db int) error {
		if writtenDB[db] {
			return nil
		}
		st := dbStats[db]
		if err := enc.WriteDBHeader(uint(db), st.keys, st.ttls); err != nil {
			return fmt.Errorf("write db %d header: %w", db, err)
		}
		writtenDB[db] = true
		return nil
	}

	for _, o := range decoded.objects {
		switch o.(type) {
		case *model.AuxObject, *model.DBSizeObject:
			continue
		case *model.FunctionsObject:
			return nil, fmt.Errorf("%s: encoder cannot represent function library payload", LossFunctionLibrary)
		}
		db := o.GetDBIndex()
		if db != lastDB {
			if err := writeDB(db); err != nil {
				return nil, err
			}
			lastDB = db
		}
		var opts []interface{}
		if o.GetExpiration() != nil {
			opts = append(opts, core.WithTTL(uint64(o.GetExpiration().UnixNano()/int64(time.Millisecond))))
		}
		var err error
		switch o := o.(type) {
		case *model.StringObject:
			err = enc.WriteStringObject(o.Key, o.Value, opts...)
		case *model.ListObject:
			err = enc.WriteListObject(o.Key, o.Values, opts...)
		case *model.SetObject:
			err = enc.WriteSetObject(o.Key, o.Members, opts...)
		case *model.HashObject:
			if len(o.FieldExpirations) > 0 {
				err = enc.WriteHashMapObjectEx(o.Key, o.Hash, o.FieldExpirations, opts...)
			} else {
				err = enc.WriteHashMapObject(o.Key, o.Hash, opts...)
			}
		case *model.ZSetObject:
			err = enc.WriteZSetObject(o.Key, o.Entries, opts...)
		case *model.StreamObject:
			err = enc.WriteStreamObject(o.Key, o, opts...)
		case *model.ModuleTypeObject:
			err = fmt.Errorf("encoder cannot represent module type %q", o.ModuleType)
		default:
			err = fmt.Errorf("encoder cannot represent %T", o)
		}
		if err != nil {
			return nil, fmt.Errorf("write %s key %q: %w", o.GetType(), o.GetKey(), err)
		}
	}

	if err := enc.WriteEnd(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
