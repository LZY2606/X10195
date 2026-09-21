package main

import (
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// feature names reported independently for every fixture.
const (
	featureListpack        = "listpack"
	featureStreamV1        = "stream-v1"
	featureStreamV2        = "stream-v2"
	featureStreamV3        = "stream-v3"
	featureHFE             = "hfe"
	featureEmptyCollection = "empty-collection"
	featureExpiredKey      = "expired-key"
	featureUnknownOpcode   = "unknown-opcode"
)

// allFeatures lists every feature the gate tracks, in report order.
var allFeatures = []string{
	featureListpack,
	featureStreamV1,
	featureStreamV2,
	featureStreamV3,
	featureHFE,
	featureEmptyCollection,
	featureExpiredKey,
	featureUnknownOpcode,
}

// decodedFixture holds the result of the first decode pass.
type decodedFixture struct {
	objects   []model.RedisObject
	types     []string
	encodings []string
	features  []string
}

// decodeAll decodes every object, including aux/db-size/functions opcodes.
func decodeAll(r io.Reader) ([]model.RedisObject, error) {
	dec := core.NewDecoder(r).WithSpecialOpCode()
	var objects []model.RedisObject
	err := dec.Parse(func(object model.RedisObject) bool {
		objects = append(objects, object)
		return true
	})
	if err != nil {
		return objects, err
	}
	return objects, nil
}

// decodeFixture reads a fixture file and collects objects plus statistics.
func decodeFixture(fx fixture) (*decodedFixture, error) {
	f, err := os.Open(fx.Abs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	objects, err := decodeAll(f)
	d := &decodedFixture{objects: objects}
	d.types, d.encodings, d.features = describeObjects(objects)
	if err != nil {
		if isUnknownOpcodeErr(err) {
			d.features = appendUnique(d.features, featureUnknownOpcode)
			sort.Strings(d.features)
		}
		return d, err
	}
	return d, nil
}

func isUnknownOpcodeErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "unknown type flag") || strings.Contains(msg, "unsupported opcode")
}

// describeObjects computes the sorted unique type/encoding lists and the
// feature set exercised by a decoded object stream.
func describeObjects(objects []model.RedisObject) (types, encodings, features []string) {
	typeSet := map[string]bool{}
	encodingSet := map[string]bool{}
	featureSet := map[string]bool{}
	now := time.Now()
	for _, o := range objects {
		typeSet[o.GetType()] = true
		if enc := o.GetEncoding(); enc != "" {
			encodingSet[enc] = true
		}
		if enc := o.GetEncoding(); enc == model.ListPackEncoding || enc == model.ListPackExEncoding {
			featureSet[featureListpack] = true
		}
		if exp := o.GetExpiration(); exp != nil && exp.Before(now) {
			featureSet[featureExpiredKey] = true
		}
		switch obj := o.(type) {
		case *model.ListObject:
			if len(obj.Values) == 0 {
				featureSet[featureEmptyCollection] = true
			}
		case *model.SetObject:
			if len(obj.Members) == 0 {
				featureSet[featureEmptyCollection] = true
			}
		case *model.HashObject:
			if len(obj.Hash) == 0 {
				featureSet[featureEmptyCollection] = true
			}
			if len(obj.FieldExpirations) > 0 {
				featureSet[featureHFE] = true
			}
		case *model.ZSetObject:
			if len(obj.Entries) == 0 {
				featureSet[featureEmptyCollection] = true
			}
		case *model.StreamObject:
			switch obj.Version {
			case 1:
				featureSet[featureStreamV1] = true
			case 2:
				featureSet[featureStreamV2] = true
			case 3:
				featureSet[featureStreamV3] = true
			}
			if obj.Length == 0 {
				featureSet[featureEmptyCollection] = true
			}
		}
	}
	for t := range typeSet {
		types = append(types, t)
	}
	sort.Strings(types)
	for e := range encodingSet {
		encodings = append(encodings, e)
	}
	sort.Strings(encodings)
	for _, f := range allFeatures {
		if featureSet[f] {
			features = append(features, f)
		}
	}
	return types, encodings, features
}

func appendUnique(list []string, item string) []string {
	for _, existing := range list {
		if existing == item {
			return list
		}
	}
	return append(list, item)
}
