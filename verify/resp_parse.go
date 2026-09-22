package verify

import (
	"errors"
	"strconv"
)

// parseRESP parses a stream of RESP2 arrays of bulk strings, the exact shape
// produced by helper.CmdLinesToResp. It validates every declared length so
// that truncated or mis-framed conversion output is rejected structurally.
func parseRESP(data []byte) ([][][]byte, error) {
	p := &respReader{data: data}
	var cmds [][][]byte
	for p.pos < len(p.data) {
		cmd, err := p.readArray()
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

type respReader struct {
	data []byte
	pos  int
}

func (r *respReader) readLine() ([]byte, error) {
	start := r.pos
	for r.pos < len(r.data) {
		if r.data[r.pos] == '\r' {
			if r.pos+1 >= len(r.data) || r.data[r.pos+1] != '\n' {
				return nil, errors.New("lone CR without LF")
			}
			line := r.data[start:r.pos]
			r.pos += 2
			return line, nil
		}
		r.pos++
	}
	return nil, errors.New("unexpected end of RESP stream")
}

func (r *respReader) readArray() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, errors.New("expected array header '*'")
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 {
		return nil, errors.New("invalid array length")
	}
	cmd := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		arg, err := r.readBulk()
		if err != nil {
			return nil, err
		}
		cmd = append(cmd, arg)
	}
	return cmd, nil
}

func (r *respReader) readBulk() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '$' {
		return nil, errors.New("expected bulk header '$'")
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, errors.New("invalid bulk length")
	}
	if n < 0 {
		return nil, nil // null bulk
	}
	end := r.pos + n
	if end+2 > len(r.data) {
		return nil, errors.New("bulk string exceeds stream")
	}
	bulk := r.data[r.pos:end]
	if r.data[end] != '\r' || r.data[end+1] != '\n' {
		return nil, errors.New("bulk string missing CRLF")
	}
	r.pos = end + 2
	return bulk, nil
}

