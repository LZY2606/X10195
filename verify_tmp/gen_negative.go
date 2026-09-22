//go:build ignore

package main

import (
	"encoding/binary"
	"os"
)

// Generates a deliberately invalid RDB fixture: a valid REDIS0011 header,
// type byte 246 (reserved/unimplemented) followed by a key string, then
// EOF. The parser's opcode switch does not recognize 246 so it falls
// through to readObject, which reports "unknown type flag" during decode.
func main() {
	var b []byte
	b = append(b, []byte("REDIS0011")...)
	b = append(b, 246)                // unimplemented type/opcode (read before the key)
	b = append(b, 0x03, 'k', 'e', 'y') // length-3 string "key"
	b = append(b, 255)                // RDB_OPCODE_EOF
	crc := make([]byte, 8)
	binary.LittleEndian.PutUint64(crc, 0)
	b = append(b, crc...)
	if err := os.WriteFile(os.Args[1], b, 0o644); err != nil {
		panic(err)
	}
}
