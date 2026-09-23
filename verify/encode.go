package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// lossEntry is a structured record of a known lossy step. Losses are never
// silently swallowed: every loss is printed by the gate and tied to the
// fixture, the kind of loss and the affected subject.
type lossEntry struct {
	kind    string
	subject string
	detail  string
}

func (l lossEntry) Error() string {
	return fmt.Sprintf("%s %s: %s", l.kind, l.subject, l.detail)
}

// reencodeResult describes the outcome of the encode stage
type reencodeResult struct {
	path   string
	losses []lossEntry
}

// reencode writes the decoded objects into a new RDB file inside tmpDir.
// Anything the encoder cannot represent losslessly is either a structured
// expected-loss (recorded in the result) or a blocker error.
func reencode(d *decodedFixture, valkey bool, tmpDir, name string) (*reencodeResult, error) {
	var auxObjects []*model.AuxObject
	var functionPayloads []string
	dbSizes := map[int]*model.DBSizeObject{}
	var dbOrder []int
	dbObjects := map[int][]model.RedisObject{}
	for _, obj := range d.objects {
		switch o := obj.(type) {
		case *model.AuxObject:
			auxObjects = append(auxObjects, o)
		case *model.FunctionsObject:
			functionPayloads = append(functionPayloads, o.FunctionsLua)
		case *model.DBSizeObject:
			dbSizes[o.DB] = o
		default:
			db := obj.GetDBIndex()
			if _, ok := dbObjects[db]; !ok {
				dbOrder = append(dbOrder, db)
			}
			dbObjects[db] = append(dbObjects[db], obj)
		}
	}

	outPath := filepath.Join(tmpDir, name)
	out, err := os.Create(outPath)
	if err != nil {
		return nil, fmt.Errorf("create re-encoded rdb failed: %v", err)
	}
	defer func() { _ = out.Close() }()

	var enc *core.Encoder
	if valkey {
		enc = core.NewEncoderValkey(out)
	} else {
		enc = core.NewEncoder(out)
	}
	res := &reencodeResult{path: outPath}

	if err := enc.WriteHeader(); err != nil {
		return nil, err
	}
	for _, aux := range auxObjects {
		if err := enc.WriteAux(aux.Key, aux.Value); err != nil {
			return nil, fmt.Errorf("write aux %q failed: %v", aux.Key, err)
		}
	}
	for _, payload := range functionPayloads {
		if err := enc.WriteFunctions(payload); err != nil {
			return nil, fmt.Errorf("write functions failed: %v", err)
		}
	}
	for db, size := range dbSizes {
		if len(dbObjects[db]) == 0 {
			// the encoder state machine cannot express an empty db: a
			// SELECTDB/RESIZEDB pair must be followed by at least one object
			res.losses = append(res.losses, lossEntry{
				kind:    "empty-db-dropped",
				subject: fmt.Sprintf("db=%d", db),
				detail: fmt.Sprintf("resizedb hint keys=%d ttls=%d has no objects and cannot be re-encoded",
					size.KeyCount, size.TTLCount),
			})
		}
	}
	for _, db := range dbOrder {
		objs := dbObjects[db]
		keyCount, ttlCount := uint64(len(objs)), uint64(0)
		for _, obj := range objs {
			if obj.GetExpiration() != nil {
				ttlCount++
			}
		}
		if size, ok := dbSizes[db]; ok {
			keyCount, ttlCount = size.KeyCount, size.TTLCount
		}
		if err := enc.WriteDBHeader(uint(db), keyCount, ttlCount); err != nil {
			return nil, fmt.Errorf("write db %d header failed: %v", db, err)
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
	return res, nil
}

func writeObject(enc *core.Encoder, obj model.RedisObject) error {
	var opts []interface{}
	if exp := obj.GetExpiration(); exp != nil {
		ms := exp.UnixNano() / int64(time.Millisecond)
		if ms < 0 {
			return fmt.Errorf("blocker: key %q expires before epoch (%s), encoder cannot represent it",
				obj.GetKey(), exp.Format(time.RFC3339Nano))
		}
		opts = append(opts, core.WithTTL(uint64(ms)))
	}
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
	default:
		return fmt.Errorf("blocker: encoder cannot losslessly represent object type %T (key %q, db %d)",
			obj, obj.GetKey(), obj.GetDBIndex())
	}
}

// safeTempName converts a fixture relative path into a flat temp file name
func safeTempName(relPath string) string {
	name := strings.ReplaceAll(relPath, "/", "__")
	return name + ".reencoded.rdb"
}
