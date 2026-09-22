package core

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestProbeIssue27LP(t *testing.T) {
	data, _ := os.ReadFile("../cases/stream_listpacks_1.rdb")
	i := bytes.Index(data, []byte("trim"))
	base := i - 1 + 5
	dec := NewDecoder(bytes.NewReader(data[base:]))
	n, _, _ := dec.readLength()
	for k := uint64(0); k < n; k++ {
		dec.readString()
		lp, err := dec.readString()
		if err != nil { t.Fatal(err) }
		d := &Decoder{}
		cursor := 6
		count, _ := d.readListPackEntryAsInt(lp, &cursor)
		deleted, _ := d.readListPackEntryAsInt(lp, &cursor)
		fn, _ := d.readListPackEntryAsInt(lp, &cursor)
		for i := int64(0); i < fn; i++ { d.readListPackEntryAsString(lp, &cursor) }
		d.readListPackEntryAsInt(lp, &cursor)
		total := count + deleted
		for r := int64(0); r < total; r++ {
			flag, _ := d.readListPackEntryAsInt(lp, &cursor)
			d.readListPackEntryAsInt(lp, &cursor); d.readListPackEntryAsInt(lp, &cursor)
			same := flag&2 != 0
			nf := fn
			if !same { nf, _ = d.readListPackEntryAsInt(lp, &cursor) }
			fieldsElems := nf
			if !same { fieldsElems = 1 + 2*nf }
			_ = fieldsElems
			for j := int64(0); j < nf; j++ {
				if same { d.readListPackEntryAsString(lp, &cursor) } else {
					d.readListPackEntryAsString(lp, &cursor); d.readListPackEntryAsString(lp, &cursor)
				}
			}
			trail, terr := d.readListPackEntryAsInt(lp, &cursor)
			expect := int64(3)
			if same { expect += nf } else { expect += 1+2*nf }
			if terr != nil || trail != expect {
				fmt.Printf("node%d rec%d flag=%d same=%v nf=%d trail=%d expect=%d err=%v cursor=%d len=%d\n",
					k, r, flag, same, nf, trail, expect, terr, cursor, len(lp))
			}
		}
		if cursor != len(lp) { fmt.Printf("node%d END cursor=%d len=%d\n", k, cursor, len(lp)) }
	}
}
