package rdbverify

import (
	"fmt"
	"io"
	"strconv"
)

// readFull is io.ReadAll for a fixed buffer kept tiny.
func readFull(r io.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err == io.EOF && n == len(buf) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// parseHeader splits a 9-byte RDB header into magic and version.
func parseHeader(header []byte) (headerInfo, error) {
	if len(header) != 9 {
		return headerInfo{}, fmt.Errorf("rdb header must be 9 bytes, got %d", len(header))
	}
	for _, magic := range []string{"REDIS", "VALKEY"} {
		prefix := []byte(magic)
		ok := true
		for i := 0; i < len(prefix); i++ {
			if header[i] != prefix[i] {
				ok = false
				break
			}
		}
		if ok {
			version, err := strconv.Atoi(string(header[len(prefix):]))
			if err != nil {
				return headerInfo{}, fmt.Errorf("invalid rdb version %q: %w", string(header[len(prefix):]), err)
			}
			return headerInfo{magic: magic, version: version}, nil
		}
	}
	return headerInfo{}, fmt.Errorf("not an RDB file: %q", string(header))
}

// String renders the dialect/version pair, e.g. "REDIS11" or "VALKEY80".
func (h headerInfo) String() string {
	return fmt.Sprintf("%s%d", h.magic, h.version)
}
