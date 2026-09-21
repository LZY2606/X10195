// Command fixturegen produces the tiny, hand-crafted RDB fixtures that complete
// the gate's required coverage. Generated binaries are checked in so `verify.sh`
// needs no Redis process and no network access; rerun this command to regenerate
// them byte-for-byte.
//
// Usage:
//
//	go run ./cases/fixturegen -out-dir cases
//
// Current fixtures:
//
//   - unknown_opcode.rdb: a valid REDIS0011 shell whose body contains opcode
//     242, a byte this parser deliberately does not understand. It is the
//     gate's negative fixture for "unknown opcode" handling.
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	outDir := flag.String("out-dir", "cases", "directory receiving generated .rdb files")
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(err)
	}
	targets := map[string][]byte{
		"unknown_opcode.rdb": unknownOpcodeRDB(),
	}
	for name, data := range targets {
		path := filepath.Join(*outDir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			fatal(err)
		}
		fmt.Printf("wrote %s (%d bytes)\n", path, len(data))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "fixturegen:", err)
	os.Exit(1)
}

// unknownOpcodeRDB builds:
//
//	REDIS0011  F2  00  'k'  FF  <8-byte CRC64>
//
// After a bogus "key" framing, byte F2 = 242 is handed to the value-type
// dispatcher. It is neither a recognized RDB type nor a special opcode, so
// decoding must fail with an "unknown type flag" error at the decode stage.
// The trailing EOF (FF) and CRC bytes make the file otherwise look like a
// complete RDB, ensuring the failure is specifically about opcode handling.
func unknownOpcodeRDB() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("REDIS0011")
	buf.WriteByte(0xF2) // value/opcode 242: intentionally unsupported
	buf.WriteByte(0x00) // length 1 for the following key bytes
	buf.WriteString("k")
	buf.WriteByte(0xFF) // RDB_OPCODE_EOF
	crc := make([]byte, 8)
	binary.LittleEndian.PutUint64(crc, 0) // content is rejected before CRC use
	buf.Write(crc)
	return buf.Bytes()
}
