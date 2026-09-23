package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// decodeAll parses a rdb file and returns every object, including aux fields,
// db-size hints and function libraries (special opcodes enabled).
func decodeAll(path string) ([]model.RedisObject, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s failed: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	var objects []model.RedisObject
	dec := core.NewDecoder(f).WithSpecialOpCode()
	err = dec.Parse(func(object model.RedisObject) bool {
		objects = append(objects, object)
		return true
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// reencode writes the decoded objects into a new rdb file at outPath.
// Object kinds the encoder cannot represent (function libraries, module data)
// are skipped here; the semantic comparator detects and classifies the loss.
func reencode(objects []model.RedisObject, valkey bool, outPath string) error {
	if err := os.MkdirAll(dirOf(outPath), 0o755); err != nil {
		return err
	}
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	var enc *core.Encoder
	if valkey {
		enc = core.NewEncoderValkey(f)
	} else {
		enc = core.NewEncoder(f)
	}
	if err := enc.WriteHeader(); err != nil {
		_ = f.Close()
		return err
	}
	// aux fields must be written before any db header
	for _, obj := range objects {
		if aux, ok := obj.(*model.AuxObject); ok {
			if err := enc.WriteAux(aux.GetKey(), aux.Value); err != nil {
				_ = f.Close()
				return fmt.Errorf("write aux %q failed: %v", aux.GetKey(), err)
			}
		}
	}
	// group data objects by db, preserving first-appearance order
	var dbOrder []int
	byDB := make(map[int][]model.RedisObject)
	for _, obj := range objects {
		if !isDataObject(obj) {
			continue
		}
		db := obj.GetDBIndex()
		if _, ok := byDB[db]; !ok {
			dbOrder = append(dbOrder, db)
		}
		byDB[db] = append(byDB[db], obj)
	}
	for _, db := range dbOrder {
		objs := byDB[db]
		var ttlCount uint64
		for _, obj := range objs {
			if obj.GetExpiration() != nil {
				ttlCount++
			}
		}
		if err := enc.WriteDBHeader(uint(db), uint64(len(objs)), ttlCount); err != nil {
			_ = f.Close()
			return fmt.Errorf("write db header %d failed: %v", db, err)
		}
		for _, obj := range objs {
			if err := writeObject(enc, obj); err != nil {
				_ = f.Close()
				return fmt.Errorf("write object %q failed: %v", obj.GetKey(), err)
			}
		}
	}
	if err := enc.WriteEnd(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// isDataObject reports whether obj is a key-value object stored inside a db.
func isDataObject(obj model.RedisObject) bool {
	switch obj.(type) {
	case *model.StringObject, *model.ListObject, *model.SetObject,
		*model.HashObject, *model.ZSetObject, *model.StreamObject:
		return true
	}
	return false
}

// writeObject re-encodes a single data object, preserving its expiration.
func writeObject(enc *core.Encoder, obj model.RedisObject) error {
	var opts []interface{}
	if exp := obj.GetExpiration(); exp != nil {
		ms := exp.UnixMilli()
		if ms < 0 {
			return fmt.Errorf("expiration %v precedes unix epoch", exp)
		}
		opts = append(opts, core.WithTTL(uint64(ms)))
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
	}
	return fmt.Errorf("no encoder for object type %q", obj.GetType())
}

// typeNames summarizes decoded objects as sorted unique "type" or
// "type(encoding)" labels for the collector log.
func typeNames(objects []model.RedisObject) []string {
	set := make(map[string]struct{})
	for _, obj := range objects {
		label := obj.GetType()
		if enc := obj.GetEncoding(); enc != "" {
			label += "(" + enc + ")"
		}
		set[label] = struct{}{}
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// detectAspects tags a fixture with the semantic aspects that require
// independent verification results.
func detectAspects(objects []model.RedisObject, now time.Time) []string {
	set := make(map[string]struct{})
	for _, obj := range objects {
		switch obj.GetEncoding() {
		case model.ListPackEncoding, model.ListPackExEncoding, model.QuickList2Encoding:
			set["listpack"] = struct{}{}
		}
		if exp := obj.GetExpiration(); exp != nil {
			if exp.Before(now) {
				set["expired-key"] = struct{}{}
			} else {
				set["expiring-key"] = struct{}{}
			}
		}
		switch o := obj.(type) {
		case *model.StreamObject:
			set[fmt.Sprintf("stream-v%d", o.Version)] = struct{}{}
			if o.Length == 0 {
				set["empty-collection"] = struct{}{}
			}
		case *model.HashObject:
			if len(o.FieldExpirations) > 0 {
				set["hfe"] = struct{}{}
			}
			if len(o.Hash) == 0 {
				set["empty-collection"] = struct{}{}
			}
		case *model.ListObject:
			if len(o.Values) == 0 {
				set["empty-collection"] = struct{}{}
			}
		case *model.SetObject:
			if len(o.Members) == 0 {
				set["empty-collection"] = struct{}{}
			}
		case *model.ZSetObject:
			if len(o.Entries) == 0 {
				set["empty-collection"] = struct{}{}
			}
		case *model.FunctionsObject:
			set["functions"] = struct{}{}
		}
	}
	aspects := make([]string, 0, len(set))
	for aspect := range set {
		aspects = append(aspects, aspect)
	}
	sort.Strings(aspects)
	return aspects
}

func dirOf(path string) string {
	dir := ""
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == os.PathSeparator {
			dir = path[:i]
			break
		}
	}
	if dir == "" {
		return "."
	}
	return dir
}
