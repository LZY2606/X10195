package core

import "testing"

func TestLpIntRoundtrip(t *testing.T) {
	enc := NewEncoder(nil)
	for _, v := range []int64{-3853, 4339, -8192, 8191, -32768, 32767, -1, 0, 127, 128, 8388608, -8388608} {
		b := enc.encodeListPackInt(v)
		// build minimal listpack
		lp := make([]byte, 6)
		lp = append(lp, b...)
		n := int(getBackLen(uint32(len(b))))
		for i := 0; i < n; i++ {
			lp = append(lp, 0)
		}
		lp = append(lp, 0xFF)
		dec := &Decoder{}
		cur := 6
		_, got, _, err := dec.readListPackEntry(lp, &cur)
		if err != nil {
			t.Fatalf("v=%d bytes=%x err=%v", v, b, err)
		}
		if got != v {
			t.Errorf("v=%d encoded=%x decoded=%d", v, b, got)
		}
	}
}
