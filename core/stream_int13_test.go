package core

import (
	"bytes"
	"testing"

	"github.com/hdt3213/rdb/model"
)

// TestStreamListpackInt13Deltas guards the int13 listpack integer encoding
// used for stream ms/seq deltas: the range must be -4096..4095 with zig-zag
// sign handling, both when encoding and decoding.
func TestStreamListpackInt13Deltas(t *testing.T) {
	diffs := []uint64{0, 100, 4095, 4096, 8191, 8192, 10000, 32767, 32768}
	for _, diff := range diffs {
		base := uint64(1640995200000)
		stream := &model.StreamObject{
			BaseObject: &model.BaseObject{Key: "s"},
			Version:    1,
			Length:     1,
			LastId:     &model.StreamId{Ms: base + diff, Sequence: 0},
			Entries: []*model.StreamEntry{{
				FirstMsgId: &model.StreamId{Ms: base, Sequence: 0},
				Fields:     []string{"f"},
				Msgs: []*model.StreamMessage{{
					Id:     &model.StreamId{Ms: base + diff, Sequence: 0},
					Fields: map[string]string{"f": "v"},
				}},
			}},
		}
		var buf bytes.Buffer
		enc := NewEncoder(&buf)
		if err := enc.WriteHeader(); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteDBHeader(0, 1, 0); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteStreamObject("s", stream); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteEnd(); err != nil {
			t.Fatal(err)
		}
		dec := NewDecoder(&buf)
		var got *model.StreamObject
		if err := dec.Parse(func(o model.RedisObject) bool {
			if st, ok := o.(*model.StreamObject); ok {
				got = st
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
		ms := got.Entries[0].Msgs[0].Id.Ms
		if ms != base+diff {
			t.Errorf("delta %d roundtrip: got %d want %d", diff, ms, base+diff)
		}
	}
}
