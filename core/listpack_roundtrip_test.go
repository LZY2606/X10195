package core

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/hdt3213/rdb/model"
)

// TestEncodeListPackIntRoundTrip exercises every integer width and every
// signed/unsigned boundary used by stream listpacks. A wrong int13 zigzag or
// wrong int7/int16 ranges previously made re-encoded streams un-decodable or
// silently dropped messages (see issue27.rdb).
func TestEncodeListPackIntRoundTrip(t *testing.T) {
	values := []int64{
		0, 1, 2, 126, 127, 128, 253, 254, 4095, 4096, 8191, 8192,
		32767, 32768, 8388607, 8388608, 2147483647, 2147483648,
		-1, -2, -127, -128, -254, -255, -4095, -4096,
		-4097, -8191, -8192, -32767, -32768, -1 << 23, -1 << 31,
		1 << 40, -(1 << 40),
	}
	enc := NewEncoder(bytes.NewBuffer(nil))
	dec := NewDecoder(bytes.NewReader(nil))
	for _, want := range values {
		encoded := enc.encodeListPackInt(want)
		buf := append(append([]byte{}, encoded...), enc.encodeBacklen(uint32(len(encoded)))...)
		cursor := 0
		_, got, _, err := dec.readListPackEntry(buf, &cursor)
		if err != nil {
			t.Fatalf("value %d encoded as %x failed to decode: %v", want, encoded, err)
		}
		if got != want {
			t.Fatalf("value %d encoded as %x decoded back as %d", want, encoded, got)
		}
		if cursor != len(buf) {
			t.Fatalf("value %d (%x): cursor %d did not consume %d bytes", want, encoded, cursor, len(buf))
		}
	}
}

// TestEncodeBacklenRoundTrip makes sure the emitted prevlen bytes agree with
// getBackLen thresholds used by the reader at every size boundary.
func TestEncodeBacklenRoundTrip(t *testing.T) {
	enc := NewEncoder(bytes.NewBuffer(nil))
	boundaries := []uint32{0, 1, 127, 128, 253, 254, 16382, 16383, 16777214, 16777215,
		2147483646, 2147483647, 0xFFFFFFFF}
	for _, n := range boundaries {
		encoded := enc.encodeBacklen(n)
		if uint32(len(encoded)) != getBackLen(n) {
			t.Fatalf("backlen length for %d: wrote %d bytes but getBackLen says %d (%x)",
				n, len(encoded), getBackLen(n), encoded)
		}
		got, consumed, err := decodeBacklen(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("backlen %x failed to decode: %v", encoded, err)
		}
		if got != n {
			t.Fatalf("backlen %x decoded to %d, want %d", encoded, got, n)
		}
		if consumed != uint32(len(encoded)) {
			t.Fatalf("backlen %x consumed %d bytes, want %d", encoded, consumed, len(encoded))
		}
	}
}

// TestStreamIssue27Regression round-trips a stream node whose sequence
// differences wrap to -1 across millisecond boundaries, the exact shape that
// broke the old int13/backlen encoder.
func TestStreamIssue27Regression(t *testing.T) {
	// exercise negative seq diffs and a wide ms diff in one node
	if err := roundTripStreamNode([][2]int64{
		{0, 0}, {0, 1}, {1, -1}, {1, 0}, {5000, -1}, {5000, 0},
	}); err != nil {
		t.Fatal(err)
	}
}

// roundTripStreamNode builds a stream node with messages identified relative
// to a first ID (msDiff, seqDiff pairs), encodes and re-decodes it, and checks
// every message ID survives exactly.
func roundTripStreamNode(diffs [][2]int64) error {
	first := &model.StreamId{Ms: 1 << 40, Sequence: 100}
	en := &model.StreamEntry{FirstMsgId: first, Fields: []string{"f"}}
	wantIDs := make([]*model.StreamId, 0, len(diffs))
	for _, d := range diffs {
		id := &model.StreamId{
			Ms:       uint64(int64(first.Ms) + d[0]),
			Sequence: uint64(int64(first.Sequence) + d[1]),
		}
		wantIDs = append(wantIDs, id)
		en.Msgs = append(en.Msgs, &model.StreamMessage{Id: id, Fields: map[string]string{"f": "v"}})
	}
	st := &model.StreamObject{
		BaseObject: &model.BaseObject{Key: "k"},
		Version:    1, Entries: []*model.StreamEntry{en},
		Length: uint64(len(diffs)), LastId: wantIDs[len(wantIDs)-1],
	}
	buf := bytes.NewBuffer(nil)
	enc := NewEncoder(buf)
	if err := enc.WriteHeader(); err != nil {
		return err
	}
	if err := enc.WriteDBHeader(0, 1, 0); err != nil {
		return err
	}
	if err := enc.WriteStreamObject("k", st); err != nil {
		return err
	}
	if err := enc.WriteEnd(); err != nil {
		return err
	}
	dec := NewDecoder(bytes.NewReader(buf.Bytes())).WithSpecialOpCode()
	var got *model.StreamObject
	if err := dec.Parse(func(o model.RedisObject) bool {
		if s, ok := o.(*model.StreamObject); ok {
			got = s
		}
		return true
	}); err != nil {
		return err
	}
	if len(got.Entries) != 1 || len(got.Entries[0].Msgs) != len(wantIDs) {
		return fmt.Errorf("message count mismatch")
	}
	for i, want := range wantIDs {
		gid := got.Entries[0].Msgs[i].Id
		if gid.Ms != want.Ms || gid.Sequence != want.Sequence {
			return fmt.Errorf("msg %d: got %d-%d want %d-%d", i, gid.Ms, gid.Sequence, want.Ms, want.Sequence)
		}
	}
	return nil
}
