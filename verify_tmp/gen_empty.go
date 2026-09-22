//go:build ignore

package main

import (
	"os"

	"github.com/hdt3213/rdb/core"
)

// Generates a valid REDIS0011 fixture containing empty list/set/hash/zset
// collections, so the gate has an independent result for empty containers.
func main() {
	f, err := os.Create(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	enc := core.NewEncoder(f)
	if err := enc.WriteHeader(); err != nil {
		panic(err)
	}
	if err := enc.WriteDBHeader(0, 4, 0); err != nil {
		panic(err)
	}
	if err := enc.WriteListObject("empty_list", nil); err != nil {
		panic(err)
	}
	if err := enc.WriteSetObject("empty_set", nil); err != nil {
		panic(err)
	}
	if err := enc.WriteHashMapObject("empty_hash", map[string][]byte{}); err != nil {
		panic(err)
	}
	if err := enc.WriteZSetObject("empty_zset", nil); err != nil {
		panic(err)
	}
	if err := enc.WriteEnd(); err != nil {
		panic(err)
	}
}
