package core

import (
	"bytes"
	"fmt"
	"io"
)

// decodeSingleListPackInt wraps an encoded integer entry with a minimal listpack
// envelope (4-byte total size, 2-byte element count, one entry, 0xFF terminator)
// and reads it back with the production decoder.
func decodeSingleListPackInt(content []byte) (int64, error) {
	// Compute backlen for the content and append it after the content.
	enc := NewEncoder(bytes.NewBuffer(nil))
	backlen := enc.encodeBacklen(uint32(len(content)))
	entry := append(append([]byte{}, content...), backlen...)

	total := uint32(6 + len(entry) + 1)
	buf := make([]byte, 0, total)
	buf = append(buf, byte(total), byte(total>>8), byte(total>>16), byte(total>>24))
	buf = append(buf, 1, 0) // one element
	buf = append(buf, entry...)
	buf = append(buf, 0xFF)

	dec := NewDecoder(bytes.NewReader(buf))
	entries, _, err := dec.readListPack()
	if err != nil {
		return 0, err
	}
	if len(entries) != 1 {
		return 0, fmt.Errorf("expected 1 entry, got %d", len(entries))
	}
	cursor := 6
	_, v, _, err := dec.readListPackEntry(buf, &cursor)
	return v, err
}

// decodeBacklen reads a prevlen field using the listpack scheme from
// lpEncodeBacklen: 0xxxxxxx is a one-byte value; multi-byte forms are
// 10xxxxxx / 110xxxxx / 1110xxxx / 11110xxx followed by the remaining
// little-endian bytes of the value.
func decodeBacklen(r io.Reader) (value uint32, consumed uint32, err error) {
	var first [1]byte
	if _, err = io.ReadFull(r, first[:]); err != nil {
		return 0, 0, err
	}
	b := first[0]
	switch {
	case b < 0x80:
		return uint32(b), 1, nil
	case b < 0xC0:
		var rest [1]byte
		if _, err = io.ReadFull(r, rest[:]); err != nil {
			return 0, 0, err
		}
		return uint32(b&0x3F)<<8 | uint32(rest[0]), 2, nil
	case b < 0xE0:
		var rest [2]byte
		if _, err = io.ReadFull(r, rest[:]); err != nil {
			return 0, 0, err
		}
		return uint32(b&0x1F)<<16 | uint32(rest[0])<<8 | uint32(rest[1]), 3, nil
	case b < 0xF0:
		var rest [3]byte
		if _, err = io.ReadFull(r, rest[:]); err != nil {
			return 0, 0, err
		}
		return uint32(b&0x0F)<<24 | uint32(rest[0])<<16 | uint32(rest[1])<<8 | uint32(rest[2]), 4, nil
	default:
		var rest [4]byte
		if _, err = io.ReadFull(r, rest[:]); err != nil {
			return 0, 0, err
		}
		// 0xF0 prefix followed by four big-endian value bytes as emitted by
		// encodeBacklen (values up to 2^32-1).
		return uint32(rest[0])<<24 | uint32(rest[1])<<16 | uint32(rest[2])<<8 | uint32(rest[3]), 5, nil
	}
}
