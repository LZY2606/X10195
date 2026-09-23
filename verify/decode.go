package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// decodedFixture holds every object decoded from a fixture, including aux
// fields, db size hints and function libraries, in stream order.
type decodedFixture struct {
	objects  []model.RedisObject
	features map[string]bool
}

func (d *decodedFixture) sortedFeatures() []string {
	features := make([]string, 0, len(d.features))
	for f := range d.features {
		features = append(features, f)
	}
	sort.Strings(features)
	return features
}

// decodeFixture decodes an RDB file with special opcodes enabled so aux
// fields, resize-db hints and function libraries are preserved.
func decodeFixture(path string) (*decodedFixture, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	d := &decodedFixture{features: map[string]bool{}}
	dec := core.NewDecoder(f).WithSpecialOpCode()
	err = dec.Parse(func(obj model.RedisObject) bool {
		d.objects = append(d.objects, obj)
		tagObjectFeatures(d.features, obj)
		return true
	})
	if err != nil {
		if strings.Contains(err.Error(), "opcode") {
			d.features["unknown-opcode"] = true
		}
		return d, err
	}
	// a silent partial decode is not a success: every byte of the fixture
	// must have been consumed
	info, statErr := f.Stat()
	if statErr != nil {
		return d, statErr
	}
	if read := int64(dec.GetReadCount()); read != info.Size() {
		return d, fmt.Errorf("decoder consumed %d of %d bytes: %d trailing undecoded bytes",
			read, info.Size(), info.Size()-read)
	}
	dataObjects := 0
	for _, obj := range d.objects {
		switch obj.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
		default:
			dataObjects++
		}
	}
	if dataObjects == 0 {
		d.features["empty-database"] = true
	}
	return d, nil
}

func tagObjectFeatures(features map[string]bool, obj model.RedisObject) {
	switch obj.GetEncoding() {
	case model.ListPackEncoding, model.ListPackExEncoding, model.QuickList2Encoding:
		features["listpack"] = true
	}
	if obj.GetExpiration() != nil && obj.GetExpiration().Before(time.Now()) {
		features["expired-key"] = true
	}
	if ev, ok := obj.(model.EvictionInfo); ok {
		if ev.GetIdleTime() >= 0 || ev.GetFreq() >= 0 {
			features["lru-lfu"] = true
		}
	}
	switch o := obj.(type) {
	case *model.StreamObject:
		switch o.Version {
		case 1:
			features["stream-v1"] = true
		case 2:
			features["stream-v2"] = true
		case 3:
			features["stream-v3"] = true
		}
	case *model.HashObject:
		for _, exp := range o.FieldExpirations {
			if exp != 0 {
				features["hfe"] = true
				break
			}
		}
		if len(o.FieldExpirations) > 0 && !features["hfe"] {
			features["hfe"] = true
		}
	case *model.FunctionsObject:
		features["functions"] = true
	}
	switch obj.(type) {
	case *model.ListObject, *model.SetObject, *model.HashObject, *model.ZSetObject:
		if obj.GetElemCount() == 0 {
			features["empty-collection"] = true
		}
	}
}
