package core

func readListPackEntryForTest(buf []byte, cursor *int) ([]byte, int64, uint32, error) {
	return dec2{}.rd(buf, cursor)
}
type dec2 struct{}
func (dec2) rd(buf []byte, cursor *int) ([]byte, int64, uint32, error) {
	d := &Decoder{}
	return d.readListPackEntry(buf, cursor)
}
