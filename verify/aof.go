package verify

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/helper"
	"github.com/hdt3213/rdb/model"
)

// checkAOF converts every object that supports AOF conversion into RESP
// form and validates the structure of the result: the bytes must parse as
// well formed RESP arrays of bulk strings, every command must carry its
// key, and expirations must produce an expire command.
func checkAOF(rel string, objects []model.RedisObject) ([]Loss, []Blocker) {
	var losses []Loss
	var blockers []Blocker
	for _, o := range objects {
		switch o.(type) {
		case *model.StringObject, *model.ListObject, *model.SetObject,
			*model.HashObject, *model.ZSetObject, *model.StreamObject:
		default:
			continue
		}
		label := fmt.Sprintf("db=%d type=%s key=%q", o.GetDBIndex(), o.GetType(), o.GetKey())
		cmds := helper.ObjectToCmd(o)
		if len(cmds) == 0 {
			if o.GetElemCount() == 0 {
				losses = append(losses, Loss{
					Fixture: rel,
					Phase:   PhaseAOF,
					Kind:    "aof-unrepresentable",
					Object:  label,
					Detail:  "AOF conversion produced no commands",
					Reason:  "empty collections and empty streams have no RESP command representation",
					Count:   1,
				})
				continue
			}
			blockers = append(blockers, Blocker{
				Fixture: rel,
				Phase:   PhaseAOF,
				Detail:  fmt.Sprintf("AOF conversion produced no commands for non-empty object %s", label),
			})
			continue
		}
		parsed, err := parseRESP(helper.CmdLinesToResp(cmds))
		if err != nil {
			blockers = append(blockers, Blocker{
				Fixture: rel,
				Phase:   PhaseAOF,
				Detail:  fmt.Sprintf("malformed RESP for %s: %v", label, err),
			})
			continue
		}
		if len(parsed) != len(cmds) {
			blockers = append(blockers, Blocker{
				Fixture: rel,
				Phase:   PhaseAOF,
				Detail:  fmt.Sprintf("RESP command count mismatch for %s: %d != %d", label, len(parsed), len(cmds)),
			})
			continue
		}
		for i, cmd := range parsed {
			if len(cmd) == 0 || len(cmd[0]) == 0 {
				blockers = append(blockers, Blocker{
					Fixture: rel,
					Phase:   PhaseAOF,
					Detail:  fmt.Sprintf("command %d for %s has no command name", i, label),
				})
				continue
			}
			if len(cmd) < 2 || string(cmd[1]) != o.GetKey() {
				blockers = append(blockers, Blocker{
					Fixture: rel,
					Phase:   PhaseAOF,
					Detail: fmt.Sprintf("command %d (%s) for %s does not carry the object key",
						i, cmd[0], label),
				})
			}
		}
		if o.GetExpiration() != nil {
			last := parsed[len(parsed)-1]
			if len(last) == 0 || !strings.Contains(strings.ToUpper(string(last[0])), "EXPIRE") {
				blockers = append(blockers, Blocker{
					Fixture: rel,
					Phase:   PhaseAOF,
					Detail:  fmt.Sprintf("object %s has expiration but no expire command was generated", label),
				})
			}
		}
	}
	return losses, blockers
}

// parseRESP strictly parses a buffer of RESP2 arrays of bulk strings and
// returns the decoded commands. Trailing garbage is an error.
func parseRESP(data []byte) ([][][]byte, error) {
	var cmds [][][]byte
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		cmd, err := parseRESPCommand(r)
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

func parseRESPCommand(r *bytes.Reader) ([][]byte, error) {
	line, err := readRESPLine(r)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 {
		return nil, fmt.Errorf("invalid array length %q", line)
	}
	cmd := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		header, err := readRESPLine(r)
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(header, "$") {
			return nil, fmt.Errorf("expected bulk string header, got %q", header)
		}
		size, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk length %q", header)
		}
		if size == -1 {
			cmd = append(cmd, nil)
			continue
		}
		if size < 0 {
			return nil, fmt.Errorf("invalid bulk length %q", header)
		}
		buf := make([]byte, size+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		if buf[size] != '\r' || buf[size+1] != '\n' {
			return nil, fmt.Errorf("bulk string not terminated with CRLF")
		}
		cmd = append(cmd, buf[:size])
	}
	return cmd, nil
}

func readRESPLine(r *bytes.Reader) (string, error) {
	var sb strings.Builder
	for {
		c, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if c == '\r' {
			c2, err := r.ReadByte()
			if err != nil {
				return "", err
			}
			if c2 != '\n' {
				return "", fmt.Errorf("expected LF after CR")
			}
			return sb.String(), nil
		}
		sb.WriteByte(c)
	}
}

func readFull(r *bytes.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
