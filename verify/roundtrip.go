package verify

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// rawDoc holds every object decoded from one rdb file in read order.
type rawDoc struct {
	magic     string
	version   int
	aux       [][2]string // aux key/value pairs in read order
	functions []string    // function library payloads in read order
	objects   []model.RedisObject
}

// decodeAll parses the whole file and collects every object, including
// aux fields and function libraries.
func decodeAll(path string) (*rawDoc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return decodeBytes(data)
}

// decodeBytes decodes a complete in-memory rdb file.
func decodeBytes(data []byte) (*rawDoc, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("file too short for rdb header (%d bytes)", len(data))
	}
	magic := string(data[:5])
	if magic != "REDIS" && magic != "VALKEY" {
		return nil, fmt.Errorf("unknown magic %q", magic)
	}
	version := 0
	for _, c := range data[5:9] {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("invalid version digit %q", string(c))
		}
		version = version*10 + int(c-'0')
	}
	doc := &rawDoc{magic: magic, version: version}
	dec := core.NewDecoder(bytes.NewReader(data)).WithSpecialOpCode()
	err := dec.Parse(func(object model.RedisObject) bool {
		switch o := object.(type) {
		case *model.AuxObject:
			doc.aux = append(doc.aux, [2]string{o.Key, o.Value})
		case *model.FunctionsObject:
			doc.functions = append(doc.functions, o.FunctionsLua)
		case *model.DBSizeObject:
			// resize hints are recomputed on re-encode, ignore here
		default:
			doc.objects = append(doc.objects, object)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return doc, nil
}

// encodeDoc re-encodes every object of doc into a new rdb file.
// Function libraries are silently skipped here; the compare stage turns
// them into a structured expected-loss record. Object types without any
// encoder support (e.g. modules) make the encode stage fail.
func encodeDoc(doc *rawDoc, out *bytes.Buffer) error {
	var enc *core.Encoder
	if doc.magic == "VALKEY" {
		enc = core.NewEncoderValkey(out)
	} else {
		enc = core.NewEncoder(out)
	}
	if err := enc.WriteHeader(); err != nil {
		return err
	}
	for _, kv := range doc.aux {
		if err := enc.WriteAux(kv[0], kv[1]); err != nil {
			return fmt.Errorf("write aux %q: %w", kv[0], err)
		}
	}
	dbOrder := make([]int, 0)
	byDB := make(map[int][]model.RedisObject)
	for _, obj := range doc.objects {
		db := obj.GetDBIndex()
		if _, ok := byDB[db]; !ok {
			dbOrder = append(dbOrder, db)
		}
		byDB[db] = append(byDB[db], obj)
	}
	sort.Ints(dbOrder)
	for _, db := range dbOrder {
		objects := byDB[db]
		var ttlCount uint64
		for _, obj := range objects {
			if obj.GetExpiration() != nil {
				ttlCount++
			}
		}
		if err := enc.WriteDBHeader(uint(db), uint64(len(objects)), ttlCount); err != nil {
			return fmt.Errorf("write db header %d: %w", db, err)
		}
		for _, obj := range objects {
			if err := writeObject(enc, obj); err != nil {
				return fmt.Errorf("write object %q: %w", obj.GetKey(), err)
			}
		}
	}
	return enc.WriteEnd()
}

func writeObject(enc *core.Encoder, obj model.RedisObject) error {
	var opts []interface{}
	if exp := obj.GetExpiration(); exp != nil {
		opts = append(opts, core.WithTTL(uint64(exp.UnixNano()/int64(time.Millisecond))))
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
		return fmt.Errorf("no encoder support for object type %q", obj.GetType())
	}
}

// runFixture runs decode -> encode -> redecode -> compare -> aof for one fixture.
func runFixture(opts Options, fixture *Fixture) *FixtureResult {
	res := &FixtureResult{
		Path:    fixture.Path,
		Magic:   fixture.Magic,
		Version: fixture.Version,
	}
	fail := func(stage string, err error) *FixtureResult {
		res.FailedStage = stage
		res.Err = err
		if stage == StageDecode && isUnknownOpcodeErr(err) {
			res.Categories = append(res.Categories, CatUnknownOpcode)
		}
		return res
	}

	doc1, err := decodeAll(fixture.AbsPath)
	if err != nil {
		return fail(StageDecode, err)
	}
	res.ObjectCount = len(doc1.objects)
	res.Types = objectTypes(doc1.objects)
	fixture.Types = res.Types
	res.Categories = append(res.Categories, categorize(doc1, opts.Now)...)

	encBuf := bytes.NewBuffer(nil)
	if err := encodeDoc(doc1, encBuf); err != nil {
		return fail(StageEncode, err)
	}

	doc2, err := decodeBytes(encBuf.Bytes())
	if err != nil {
		return fail(StageRedecode, err)
	}

	diffs, losses := compareDocs(doc1, doc2)
	res.Losses = losses
	if len(diffs) > 0 {
		return fail(StageCompare, fmt.Errorf("%d semantic difference(s): %s",
			len(diffs), joinLimit(diffs, 5)))
	}

	if err := checkAOF(fixture.AbsPath, doc1, opts.TmpDir); err != nil {
		return fail(StageAOF, err)
	}
	return res
}

func objectTypes(objects []model.RedisObject) []string {
	set := make(map[string]struct{})
	for _, obj := range objects {
		set[obj.GetType()] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func categorize(doc *rawDoc, now time.Time) []string {
	set := make(map[string]struct{})
	for _, obj := range doc.objects {
		enc := obj.GetEncoding()
		if enc == model.ListPackEncoding || enc == model.ListPackExEncoding || enc == model.QuickList2Encoding {
			set[CatListpack] = struct{}{}
		}
		if exp := obj.GetExpiration(); exp != nil && exp.Before(now) {
			set[CatExpiredKeys] = struct{}{}
		}
		switch o := obj.(type) {
		case *model.StreamObject:
			switch o.Version {
			case 1:
				set[CatStreamV1] = struct{}{}
			case 2:
				set[CatStreamV2] = struct{}{}
			case 3:
				set[CatStreamV3] = struct{}{}
			}
		case *model.HashObject:
			if len(o.FieldExpirations) > 0 || enc == model.HashExEncoding || enc == model.ListPackExEncoding {
				set[CatHFE] = struct{}{}
			}
		}
		switch obj.GetType() {
		case model.SetType, model.HashType, model.ListType, model.ZSetType:
			if obj.GetElemCount() == 0 {
				set[CatEmptySet] = struct{}{}
			}
		}
	}
	if len(doc.objects) == 0 {
		set[CatEmptySet] = struct{}{} // empty database
	}
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func isUnknownOpcodeErr(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "unknown opcode") ||
		strings.Contains(msg, "unknown type flag") ||
		strings.Contains(msg, "unsupported opcode")
}

func joinLimit(items []string, max int) string {
	if len(items) <= max {
		return fmt.Sprintf("%v", items)
	}
	return fmt.Sprintf("%v ... (%d more)", items[:max], len(items)-max)
}

var _ = bufio.NewReader // keep bufio import if needed later
