package core

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/hdt3213/rdb/model"
)

// extract original stream node listpacks using decoder internals
func extractOrig(t *testing.T, path string) [][]byte {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	d := NewDecoder(f)
	if err := d.checkHeader(); err != nil {
		t.Fatal(err)
	}
	// scan until stream v2 type byte, rough approach: parse with callback and re-read ourselves is hard.
	// Instead: decode to model, then re-read file via a custom raw parse.
	var lps [][]byte
	dbIndex := 0
	for {
		b, err := d.readByte()
		if err != nil {
			t.Fatal(err)
		}
		if b == opCodeEOF {
			break
		}
		if b == opCodeSelectDB {
			n, _, err := d.readLength()
			if err != nil {
				t.Fatal(err)
			}
			dbIndex = int(n)
			continue
		}
		if b == opCodeAux {
			if _, err := d.readString(); err != nil { t.Fatal(err) }
			if _, err := d.readString(); err != nil { t.Fatal(err) }
			continue
		}
		if b == opCodeResizeDB {
			if _, _, err := d.readLength(); err != nil { t.Fatal(err) }
			if _, _, err := d.readLength(); err != nil { t.Fatal(err) }
			continue
		}
		if b == opCodeExpireTimeMs {
			if err := d.readFull(d.buffer); err != nil { t.Fatal(err) }
		}
		if _, err := d.readString(); err != nil { t.Fatal(err) } // key
		switch b {
		case typeStreamListPacks, typeStreamListPacks2, typeStreamListPacks3:
			n, _, err := d.readLength()
			if err != nil { t.Fatal(err) }
			for i := uint64(0); i < n; i++ {
				if _, err := d.readString(); err != nil { t.Fatal(err) } // header
				lp, err := d.readString()
				if err != nil { t.Fatal(err) }
				lps = append(lps, lp)
			}
			// skip rest of stream: easiest bail
			_ = dbIndex
			return lps
		default:
			t.Fatalf("unexpected type %d at db %d", b, dbIndex)
		}
	}
	return lps
}

func TestDbgIssue27(t *testing.T) {
	orig := extractOrig(t, "../cases/issue27.rdb")
	t.Log("orig nodes", len(orig))
	t.Logf("first orig lp hex: %x", orig[0][:80])

	f, _ := os.Open("../cases/issue27.rdb")
	defer f.Close()
	dec := NewDecoder(f).WithSpecialOpCode()
	var objs []model.RedisObject
	if err := dec.Parse(func(o model.RedisObject) bool { objs = append(objs, o); return true }); err != nil {
		t.Fatalf("parse orig: %v (readcount=%d)", err, dec.GetReadCount())
	}
	var stream *model.StreamObject
	for _, o := range objs {
		if s, ok := o.(*model.StreamObject); ok {
			stream = s
		}
	}
	for idx, entry := range stream.Entries {
		var buf bytes.Buffer
		enc := NewEncoder(&buf)
		enc.state = writtenObjectState
		if err := enc.writeStreamEntryContent(entry); err != nil {
			t.Fatal(err)
		}
		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		lp, err := d.readString()
		if err != nil {
			t.Fatalf("node %d readString: %v", idx, err)
		}
		o := orig[idx]
		// compare headers
		fmtLen := func(x []byte) (int, int) {
			return int(binary.LittleEndian.Uint32(x[:4])), int(binary.LittleEndian.Uint16(x[4:6]))
		}
		ot, on := fmtLen(o)
		gt, gn := fmtLen(lp)
		if ot != len(o) { t.Fatalf("orig node %d bad total %d vs %d", idx, ot, len(o)) }
		if gt != len(lp) { t.Fatalf("gen node %d bad total %d vs %d", idx, gt, len(lp)) }
		if on != gn { t.Fatalf("node %d num %d vs %d", idx, on, gn) }
		if !bytes.Equal(o, lp) {
			// find first diff
			m := len(o)
			if len(lp) < m { m = len(lp) }
			for k := 6; k < m; k++ {
				if o[k] != lp[k] {
					t.Fatalf("node %d first byte diff at %d: orig=%02x gen=%02b\\norig context: %x\\ngen  context: %x", idx, k, o[k], lp[k], o[k:min(k+24,len(o))], lp[k:min(k+24,len(lp))])
				}
			}
			t.Fatalf("node %d length diff %d vs %d", idx, len(o), len(lp))
		}
	}
}

func min(a, b int) int { if a < b { return a }; return b }
