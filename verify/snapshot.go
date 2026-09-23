package verify

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// Flavor identifies the magic header family of an RDB file.
type Flavor string

const (
	// FlavorRedis marks files starting with the REDIS magic.
	FlavorRedis Flavor = "REDIS"
	// FlavorValkey marks files starting with the VALKEY magic.
	FlavorValkey Flavor = "VALKEY"
)

// Snapshot is the decoded semantic content of one RDB file.
// Objects are grouped by database and sorted deterministically. Auxiliary
// metadata and function libraries are kept separately because they are global.
type Snapshot struct {
	Path      string
	Flavor    Flavor
	Version   int
	Aux       []*model.AuxObject
	Functions []*model.FunctionsObject
	// DBs maps db index to the data objects held by that db, sorted by raw key.
	DBs map[int][]model.RedisObject
}

// headerInfo parses the first 9 bytes of an RDB file without decoding it.
type headerInfo struct {
	flavor  Flavor
	version int
}

// readHeader reads the magic header from raw rdb bytes.
func readHeader(raw []byte) (headerInfo, error) {
	if len(raw) < 9 {
		return headerInfo{}, fmt.Errorf("file too short for rdb header: %d bytes", len(raw))
	}
	switch {
	case bytes.HasPrefix(raw, []byte("REDIS")):
	case bytes.HasPrefix(raw, []byte("VALKEY")):
	default:
		return headerInfo{}, fmt.Errorf("file is not a RDB file")
	}
	info := headerInfo{flavor: Flavor(raw[:5])}
	v, err := strconv.Atoi(string(raw[5:9]))
	if err != nil {
		return headerInfo{}, fmt.Errorf("%q is not valid version number", strings.TrimSpace(string(raw[5:9])))
	}
	info.version = v
	return info, nil
}

// DecodeFile reads and fully decodes an rdb file, preserving auxiliary
// objects, db size hints, functions and the exact db/key/expiration metadata.
func DecodeFile(path string) (*Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s failed: %w", path, err)
	}
	info, err := readHeader(raw)
	if err != nil {
		return nil, err
	}
	snap := &Snapshot{
		Path:    path,
		Flavor:  info.flavor,
		Version: info.version,
		DBs:     make(map[int][]model.RedisObject),
	}
	dec := core.NewDecoder(bytes.NewReader(raw)).WithSpecialOpCode()
	err = dec.Parse(func(object model.RedisObject) bool {
		switch o := object.(type) {
		case *model.AuxObject:
			snap.Aux = append(snap.Aux, o)
		case *model.FunctionsObject:
			snap.Functions = append(snap.Functions, o)
		case *model.DBSizeObject:
			// resize hint only, semantic content is represented by the objects
			return true
		default:
			db := o.GetDBIndex()
			snap.DBs[db] = append(snap.DBs[db], o)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	for db := range snap.DBs {
		objs := snap.DBs[db]
		sort.Slice(objs, func(i, j int) bool {
			return objs[i].GetKey() < objs[j].GetKey()
		})
		snap.DBs[db] = objs
	}
	return snap, nil
}

// KeyCount returns the total number of data objects across all databases.
func (s *Snapshot) KeyCount() int {
	n := 0
	for _, objs := range s.DBs {
		n += len(objs)
	}
	return n
}

// TypeSignature returns the sorted set of type/encoding labels held by the
// snapshot, for deterministic collector output.
func (s *Snapshot) TypeSignature() []string {
	seen := make(map[string]struct{})
	for _, objs := range s.DBs {
		for _, o := range objs {
			seen[o.GetType()+"/"+o.GetEncoding()] = struct{}{}
		}
	}
	if len(s.Functions) > 0 {
		seen["functions/functions"] = struct{}{}
	}
	if len(s.Aux) > 0 {
		seen["aux"] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for label := range seen {
		out = append(out, label)
	}
	sort.Strings(out)
	return out
}
