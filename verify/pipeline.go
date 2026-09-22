package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// objectDump is the semantic content of one decoded RDB file.
type objectDump struct {
	aux       [][2]string
	dbsize    map[int][2]uint64
	objects   []model.RedisObject
	functions []string
}

// decodeDump decodes one RDB file and collects all objects, including aux
// fields, resize-db hints and function libraries.
func decodeDump(path string) (*objectDump, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	dump := &objectDump{dbsize: make(map[int][2]uint64)}
	dec := core.NewDecoder(f).WithSpecialOpCode()
	err = dec.Parse(func(obj model.RedisObject) bool {
		switch o := obj.(type) {
		case *model.AuxObject:
			dump.aux = append(dump.aux, [2]string{o.Key, o.Value})
		case *model.DBSizeObject:
			dump.dbsize[o.DB] = [2]uint64{o.KeyCount, o.TTLCount}
		case *model.FunctionsObject:
			dump.functions = append(dump.functions, o.FunctionsLua)
		default:
			dump.objects = append(dump.objects, obj)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return dump, nil
}

// reencodeDump writes the semantic content of dump as a new RDB file. Objects
// the encoder cannot represent losslessly must be excluded by the caller and
// reported as structured expected-loss records.
func reencodeDump(dump *objectDump, valkey bool, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	var enc *core.Encoder
	if valkey {
		enc = core.NewEncoderValkey(f)
	} else {
		enc = core.NewEncoder(f)
	}
	if err := enc.WriteHeader(); err != nil {
		return err
	}
	aux := append([][2]string(nil), dump.aux...)
	sort.Slice(aux, func(i, j int) bool { return aux[i][0] < aux[j][0] })
	for _, kv := range aux {
		if err := enc.WriteAux(kv[0], kv[1]); err != nil {
			return err
		}
	}

	// group objects by db, preserving first-seen db order
	var dbOrder []int
	byDB := make(map[int][]model.RedisObject)
	for _, obj := range dump.objects {
		db := obj.GetDBIndex()
		if _, ok := byDB[db]; !ok {
			dbOrder = append(dbOrder, db)
		}
		byDB[db] = append(byDB[db], obj)
	}
	for _, db := range dbOrder {
		objects := byDB[db]
		var ttlCount uint64
		for _, obj := range objects {
			if obj.GetExpiration() != nil {
				ttlCount++
			}
		}
		if err := enc.WriteDBHeader(uint(db), uint64(len(objects)), ttlCount); err != nil {
			return err
		}
		for _, obj := range objects {
			if err := writeObject(enc, obj); err != nil {
				return fmt.Errorf("encode db=%d key=%q type=%s: %v",
					obj.GetDBIndex(), obj.GetKey(), obj.GetType(), err)
			}
		}
	}
	return enc.WriteEnd()
}

func ttlOptions(obj model.RedisObject) []interface{} {
	expiration := obj.GetExpiration()
	if expiration == nil {
		return nil
	}
	return []interface{}{core.WithTTL(uint64(expiration.UnixMilli()))}
}

func writeObject(enc *core.Encoder, obj model.RedisObject) error {
	opts := ttlOptions(obj)
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
		return fmt.Errorf("encoder cannot represent type %q", obj.GetType())
	}
}

// processFixture runs the full verification pipeline for one fixture and
// reports the first failing stage, if any.
func processFixture(root, tmpDir string, fx fixture) *fixtureResult {
	res := &fixtureResult{path: fx.path}
	fail := func(stage string, err error) *fixtureResult {
		res.failure = &stageError{stage: stage, err: err}
		return res
	}

	version, err := detectVersion(fx.abs)
	if err != nil {
		return fail(stageDecode, err)
	}
	res.version = version

	dump1, err := decodeDump(fx.abs)
	if err != nil {
		res.features = detectFeatures(nil, err)
		res.types = detectTypes(nil)
		return fail(stageDecode, err)
	}
	res.objects = len(dump1.objects)
	res.types = detectTypes(dump1)
	res.features = detectFeatures(dump1, nil)
	res.losses = detectExpectedLosses(dump1)

	valkey, err := isValkeyFile(fx.abs)
	if err != nil {
		return fail(stageDecode, err)
	}
	reencPath := filepath.Join(tmpDir, "reencode", strings.ReplaceAll(fx.path, "/", "__")+".reenc.rdb")
	if err := os.MkdirAll(filepath.Dir(reencPath), 0o755); err != nil {
		return fail(stageEncode, err)
	}
	if err := reencodeDump(dump1, valkey, reencPath); err != nil {
		return fail(stageEncode, err)
	}

	dump2, err := decodeDump(reencPath)
	if err != nil {
		return fail(stageRedecode, err)
	}

	if diffs := compareDumps(dump1, dump2); len(diffs) > 0 {
		return fail(stageCompare, fmt.Errorf("semantic mismatch: %s", strings.Join(diffs, "; ")))
	}

	for _, obj := range dump1.objects {
		if !aofConvertible(obj) {
			continue
		}
		if err := checkAOF(obj); err != nil {
			return fail(stageAOF, fmt.Errorf("db=%d key=%q type=%s: %v",
				obj.GetDBIndex(), obj.GetKey(), obj.GetType(), err))
		}
	}
	return res
}

func isValkeyFile(abs string) (bool, error) {
	f, err := os.Open(abs)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	magic := make([]byte, 6)
	n, err := f.Read(magic)
	if err != nil || n < len(magic) {
		return false, fmt.Errorf("cannot read rdb magic")
	}
	return string(magic) == "VALKEY", nil
}

func detectTypes(dump *objectDump) []string {
	set := make(map[string]bool)
	if dump != nil {
		for _, obj := range dump.objects {
			set[obj.GetType()] = true
		}
		if len(dump.functions) > 0 {
			set[model.FunctionsType] = true
		}
	}
	types := make([]string, 0, len(set))
	for t := range set {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// detectFeatures tags a fixture with the semantic categories that require
// independent results: listpack, stream v1/v2/v3, HFE, empty collections,
// expired keys, unknown opcodes and function libraries.
func detectFeatures(dump *objectDump, decodeErr error) []string {
	set := make(map[string]bool)
	if dump != nil {
		for _, obj := range dump.objects {
			switch obj.GetEncoding() {
			case model.ListPackEncoding, model.QuickList2Encoding:
				set["listpack"] = true
			case model.ListPackExEncoding, model.HashExEncoding:
				set["listpack"] = true
				set["hfe"] = true
			}
			switch o := obj.(type) {
			case *model.StreamObject:
				set[fmt.Sprintf("stream-v%d", o.Version)] = true
			case *model.HashObject:
				if len(o.FieldExpirations) > 0 {
					set["hfe"] = true
				}
			}
			switch obj.GetType() {
			case model.ListType, model.SetType, model.HashType, model.ZSetType:
				if obj.GetElemCount() == 0 {
					set["empty-collection"] = true
				}
			}
			if expiration := obj.GetExpiration(); expiration != nil && expiration.Before(time.Now()) {
				set["expired-key"] = true
			}
		}
		if len(dump.functions) > 0 {
			set["functions"] = true
		}
	}
	if decodeErr != nil {
		msg := decodeErr.Error()
		if strings.Contains(msg, "unknown") || strings.Contains(msg, "unsupported opcode") {
			set["unknown-opcode"] = true
		}
	}
	features := make([]string, 0, len(set))
	for f := range set {
		features = append(features, f)
	}
	sort.Strings(features)
	return features
}

// detectExpectedLosses reports the parts of a dump that the encoder cannot
// represent losslessly. These become structured expected-loss records
// instead of silently passing the second decode.
func detectExpectedLosses(dump *objectDump) []expectedLoss {
	var losses []expectedLoss
	if len(dump.functions) > 0 {
		losses = append(losses, expectedLoss{
			kind:   "functions",
			detail: "encoder cannot represent function libraries; excluded from re-encode comparison",
		})
	}
	var evictionKeys []string
	for _, obj := range dump.objects {
		if info, ok := obj.(model.EvictionInfo); ok {
			if info.GetIdleTime() >= 0 || info.GetFreq() >= 0 {
				evictionKeys = append(evictionKeys, obj.GetKey())
			}
		}
	}
	if len(evictionKeys) > 0 {
		sort.Strings(evictionKeys)
		losses = append(losses, expectedLoss{
			kind:   "eviction-metadata",
			detail: "encoder cannot represent LRU/LFU metadata for keys: " + strings.Join(evictionKeys, ","),
		})
	}
	return losses
}
