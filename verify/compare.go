package main

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/hdt3213/rdb/model"
)

// compareObjects semantically compares two decoded object sequences.
// Map and set order is normalized; DB index, key bytes, expiration (ms
// precision), stream ids, group/consumer/PEL ownership and hash field
// expirations must match exactly. It returns a list of human readable diffs.
func compareObjects(orig, recon []model.RedisObject) []string {
	c := &comparator{}
	origData, origAux, origDBSize := splitObjects(orig)
	reconData, reconAux, reconDBSize := splitObjects(recon)
	c.compareAux(origAux, reconAux)
	c.compareDBSize(origDBSize, reconDBSize)
	c.compareData(origData, reconData)
	return c.diffs
}

type comparator struct {
	diffs []string
}

func (c *comparator) diff(format string, args ...interface{}) {
	c.diffs = append(c.diffs, fmt.Sprintf(format, args...))
}

// splitObjects separates data objects from aux and dbsize metadata.
// FunctionsObject must have been filtered out by the caller (expected loss).
func splitObjects(objs []model.RedisObject) (data []model.RedisObject, aux []*model.AuxObject, dbSize map[int]*model.DBSizeObject) {
	dbSize = map[int]*model.DBSizeObject{}
	for _, o := range objs {
		switch obj := o.(type) {
		case *model.AuxObject:
			aux = append(aux, obj)
		case *model.DBSizeObject:
			dbSize[obj.DB] = obj
		default:
			data = append(data, o)
		}
	}
	return data, aux, dbSize
}

func (c *comparator) compareAux(orig, recon []*model.AuxObject) {
	c.diff("placeholder")
	_ = orig
	_ = recon
}
