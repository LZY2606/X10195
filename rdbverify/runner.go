package rdbverify

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// Stage identifies a pipeline phase used by the negative-fixture manifest.
type Stage string

// Stages at which a negative fixture is expected to fail.
const (
	StageHeader       Stage = "header"
	StageDecode       Stage = "decode"
	StageEncode       Stage = "encode"
	StageSecondDecode Stage = "second-decode"
	StageSemantic     Stage = "semantic"
)

// DecodeResult is everything learned from one decode pass over an RDB.
type DecodeResult struct {
	Magic       string
	Version     int
	IsValkey    bool
	Snapshot    *Snapshot
	Objects     []model.RedisObject
	DBIndexes   []int
	Types       []string // unique object types (data keys only), sorted later
	Encodings   []string // unique encodings, sorted later
	ExpiredKeys []string // keys whose expiration is already in the past
	Persistent  int
	WithTTL     int
}

// DecodeRDB fully decodes path with special opcodes enabled.
func DecodeRDB(path string) (*DecodeResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fixture: %w", err)
	}
	magic, version, isValkey, err := readHeader(data)
	if err != nil {
		return nil, &StageError{Stage: StageHeader, Err: err}
	}
	dec := core.NewDecoder(bytes.NewReader(data)).WithSpecialOpCode()
	builder := NewSnapshotBuilder(magic, version)
	res := &DecodeResult{Magic: magic, Version: version, IsValkey: isValkey}
	typeSeen := make(map[string]struct{})
	encSeen := make(map[string]struct{})
	dbSeen := make(map[int]struct{})
	now := time.Now()
	err = dec.Parse(func(obj model.RedisObject) bool {
		builder.AddObject(obj)
		res.Objects = append(res.Objects, obj)
		if isDataObject(obj) {
			typeSeen[obj.GetType()] = struct{}{}
			if e := obj.GetEncoding(); e != "" {
				encSeen[e] = struct{}{}
			}
			dbSeen[obj.GetDBIndex()] = struct{}{}
			if exp := obj.GetExpiration(); exp != nil {
				res.WithTTL++
				if !exp.After(now) {
					res.ExpiredKeys = append(res.ExpiredKeys, obj.GetKey())
				}
			} else {
				res.Persistent++
			}
		}
		return true
	})
	if err != nil {
		return nil, &StageError{Stage: StageDecode, Err: err}
	}
	res.Snapshot = builder.Build()
	res.Types = sortedKeys(typeSeen)
	res.Encodings = sortedKeys(encSeen)
	for db := range dbSeen {
		res.DBIndexes = append(res.DBIndexes, db)
	}
	res.DBIndexes = sortedInts(res.DBIndexes)
	res.ExpiredKeys = sortedStrings(res.ExpiredKeys)
	return res, nil
}

func isDataObject(obj model.RedisObject) bool {
	switch obj.(type) {
	case *model.AuxObject, *model.DBSizeObject:
		return false
	}
	return true
}

// StageError attaches the pipeline stage at which a failure happened.
type StageError struct {
	Stage Stage
	Err   error
}

func (e *StageError) Error() string { return fmt.Sprintf("%s: %v", e.Stage, e.Err) }
func (e *StageError) Unwrap() error { return e.Err }

// Reencode performs decode -> re-encode -> second decode and returns the
// encoded bytes plus the second decode result. Failures carry the stage.
func Reencode(path string) (encoded []byte, first, second *DecodeResult, err error) {
	first, err = DecodeRDB(path)
	if err != nil {
		return nil, nil, nil, err
	}
	buf := bytes.NewBuffer(nil)
	if err = WriteObjects(buf, first); err != nil {
		return nil, first, nil, &StageError{Stage: StageEncode, Err: err}
	}
	encoded = buf.Bytes()
	second, err = DecodeRDBFromBytes(encoded)
	if err != nil {
		return encoded, first, nil, &StageError{Stage: StageSecondDecode, Err: err}
	}
	return encoded, first, second, nil
}

// DecodeRDBFromBytes is DecodeRDB without touching the filesystem.
func DecodeRDBFromBytes(data []byte) (*DecodeResult, error) {
	tmpFile, err := os.CreateTemp("", "rdbverify-*.rdb")
	if err != nil {
		return nil, err
	}
	name := tmpFile.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err = tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return nil, err
	}
	if err = tmpFile.Close(); err != nil {
		return nil, err
	}
	return DecodeRDB(name)
}

func readHeader(data []byte) (magic string, version int, isValkey bool, err error) {
	if len(data) < 9 {
		return "", 0, false, fmt.Errorf("file too short for RDB header")
	}
	switch {
	case bytes.Equal(data[:5], []byte("REDIS")):
		magic = "REDIS"
	case bytes.Equal(data[:6], []byte("VALKEY")):
		magic = "VALKEY"
		isValkey = true
	default:
		return "", 0, false, fmt.Errorf("file is not a RDB/VALKEY file")
	}
	versionStart := 5
	versionWidth := 4 // REDIS0011
	if isValkey {
		versionStart = 6
		versionWidth = 3 // VALKEY080
	}
	v := 0
	for i := versionStart; i < versionStart+versionWidth; i++ {
		ch := data[i]
		if ch < '0' || ch > '9' {
			return "", 0, false, fmt.Errorf("invalid version digits")
		}
		v = v*10 + int(ch-'0')
	}
	return magic, v, isValkey, nil
}

// WriteObjects re-serializes the decoded objects in their original order.
// Objects are grouped by DB with a header for every non-empty DB, and
// function libraries are rejected with an explicit blocker since the
// current encoder has no opcode 245 writer.
func WriteObjects(w io.Writer, dec *DecodeResult) error {
	var enc *core.Encoder
	if dec.IsValkey {
		enc = core.NewEncoderValkey(w)
	} else {
		enc = core.NewEncoder(w)
	}
	if err := enc.WriteHeader(); err != nil {
		return err
	}

	// preserve aux field order: sorted for determinism
	auxKeys := make([]string, 0, len(dec.Snapshot.Aux))
	for k := range dec.Snapshot.Aux {
		auxKeys = append(auxKeys, k)
	}
	for _, k := range auxKeys {
		if err := enc.WriteAux(k, dec.Snapshot.Aux[k]); err != nil {
			return err
		}
	}

	currentDB := -1
	dbKeyCount := map[int]int{}
	dbTTLCount := map[int]int{}
	for _, obj := range dec.Objects {
		if _, ok := obj.(*model.AuxObject); ok {
			continue
		}
		if dbsize, ok := obj.(*model.DBSizeObject); ok {
			dbKeyCount[dbsize.DB] = int(dbsize.KeyCount)
			dbTTLCount[dbsize.DB] = int(dbsize.TTLCount)
			continue
		}
		db := obj.GetDBIndex()
		if db != currentDB {
			kc := dbKeyCount[db]
			tc := dbTTLCount[db]
			if kc == 0 {
				kc = countKeys(dec.Objects, db)
			}
			if tc == 0 {
				tc = countTTL(dec.Objects, db)
			}
			if err := enc.WriteDBHeader(uint(db), uint64(kc), uint64(tc)); err != nil {
				return err
			}
			currentDB = db
		}
		if err := writeOne(enc, obj); err != nil {
			return err
		}
	}
	return enc.WriteEnd()
}

func countKeys(objs []model.RedisObject, db int) int {
	n := 0
	for _, o := range objs {
		if o.GetDBIndex() == db && isDataObject(o) {
			n++
		}
	}
	return n
}

func countTTL(objs []model.RedisObject, db int) int {
	n := 0
	for _, o := range objs {
		if o.GetDBIndex() == db && isDataObject(o) && o.GetExpiration() != nil {
			n++
		}
	}
	return n
}

func writeOne(enc *core.Encoder, obj model.RedisObject) error {
	opts := []interface{}{}
	if exp := obj.GetExpiration(); exp != nil {
		opts = append(opts, core.WithTTL(uint64(exp.UnixNano()/1e6)))
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
	case *model.FunctionsObject:
		return fmt.Errorf("encoder cannot serialize function libraries (RDB opcode 245)")
	case *model.ModuleTypeObject:
		return fmt.Errorf("encoder cannot serialize module type %q", o.ModuleType)
	default:
		return fmt.Errorf("encoder cannot serialize object type %T", obj)
	}
}
