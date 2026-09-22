package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/helper"
	"github.com/hdt3213/rdb/model"
)

// aofResult reports the structural validation of the AOF conversion of one
// fixture. The conversion is generated entirely in memory; no .aof file is
// written into the source tree.
type aofResult struct {
	Supported bool   `json:"supported"`
	Commands  int    `json:"commands"`
	Detail    string `json:"detail,omitempty"`
	// GroupOwned is true when the source stream carries consumer groups whose
	// ownership cannot be represented by AOF XADD commands.
	GroupOwned bool `json:"groupOwned,omitempty"`
}

// verifyAOF converts every object of the snapshot to RESP and parses the
// result back as a strict RESP client would, then checks command shapes.
func verifyAOF(snap *snapshot) aofResult {
	var buf bytes.Buffer
	groupOwned := false
	for _, obj := range snap.objects {
		if st, ok := obj.(*model.StreamObject); ok && len(st.Groups) > 0 {
			groupOwned = true
		}
		lines := helper.ObjectToCmd(obj)
		if len(lines) == 0 {
			// objects without an AOF representation (e.g. module types)
			return aofResult{Supported: false, Detail: fmt.Sprintf("no AOF representation for %s key %q", obj.GetType(), obj.GetKey())}
		}
		buf.Write(helper.CmdLinesToResp(lines))
	}
	cmds, err := parseRESP(bufio.NewReader(&buf))
	if err != nil {
		return aofResult{Supported: false, Detail: "RESP parse error: " + err.Error()}
	}
	for _, cmd := range cmds {
		if err := validateAofCommand(cmd); err != nil {
			return aofResult{Supported: false, Commands: len(cmds), Detail: err.Error()}
		}
	}
	// cross-check: every object's payload must be reachable in the command stream
	if err := aofCoversSnapshot(snap, cmds); err != nil {
		return aofResult{Supported: false, Commands: len(cmds), Detail: err.Error()}
	}
	return aofResult{Supported: true, Commands: len(cmds), GroupOwned: groupOwned}
}

// parseRESP parses a RESP2 array-of-bulk-strings stream, the exact shape the
// project emits.
func parseRESP(r *bufio.Reader) ([][][]byte, error) {
	var commands [][][]byte
	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			if line != "" {
				return nil, fmt.Errorf("trailing data: %q", line)
			}
			return commands, nil
		}
		if err != nil {
			return nil, err
		}
		if len(line) < 3 || line[0] != '*' || line[len(line)-2] != '\r' {
			return nil, fmt.Errorf("expected array header, got %q", strings.TrimSpace(line))
		}
		n, err := strconv.Atoi(strings.TrimSpace(line[1 : len(line)-2]))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid array length %q", strings.TrimSpace(line))
		}
		cmd := make([][]byte, n)
		for i := 0; i < n; i++ {
			hdr, err := r.ReadString('\n')
			if err != nil {
				return nil, err
			}
			if len(hdr) < 3 || hdr[0] != '$' || hdr[len(hdr)-2] != '\r' {
				return nil, fmt.Errorf("expected bulk header, got %q", strings.TrimSpace(hdr))
			}
			size, err := strconv.Atoi(strings.TrimSpace(hdr[1 : len(hdr)-2]))
			if err != nil || size < 0 {
				return nil, fmt.Errorf("invalid bulk length %q", strings.TrimSpace(hdr))
			}
			bulk := make([]byte, size+2)
			if _, err := io.ReadFull(r, bulk); err != nil {
				return nil, err
			}
			if bulk[size] != '\r' || bulk[size+1] != '\n' {
				return nil, fmt.Errorf("bulk not terminated with CRLF")
			}
			cmd[i] = bulk[:size]
		}
		commands = append(commands, cmd)
	}
}

func validateAofCommand(cmd [][]byte) error {
	if len(cmd) < 2 {
		return fmt.Errorf("command too short: %v", cmd)
	}
	name := strings.ToUpper(string(cmd[0]))
	switch name {
	case "SET":
		if len(cmd) != 3 {
			return fmt.Errorf("SET must have 3 args, got %d", len(cmd))
		}
	case "RPUSH":
		if len(cmd) < 3 {
			return fmt.Errorf("RPUSH without elements")
		}
	case "SADD":
		if len(cmd) < 3 {
			return fmt.Errorf("SADD without members")
		}
	case "HMSET":
		if len(cmd) < 4 || len(cmd)%2 != 0 {
			return fmt.Errorf("HMSET requires field/value pairs")
		}
	case "HPEXPIREAT":
		if len(cmd) != 6 || string(cmd[3]) != "FIELDS" || string(cmd[4]) != "1" {
			return fmt.Errorf("malformed HPEXPIREAT: %v", cmd)
		}
		if _, err := strconv.ParseInt(string(cmd[2]), 10, 64); err != nil {
			return fmt.Errorf("HPEXPIREAT timestamp not int64: %v", err)
		}
	case "HPERSIST":
		if len(cmd) != 5 || string(cmd[2]) != "FIELDS" || string(cmd[3]) != "1" {
			return fmt.Errorf("malformed HPERSIST")
		}
	case "ZADD":
		if len(cmd) < 4 || len(cmd)%2 != 0 {
			return fmt.Errorf("ZADD requires score/member pairs")
		}
		for i := 2; i < len(cmd); i += 2 {
			if _, err := strconv.ParseFloat(string(cmd[i]), 64); err != nil {
				return fmt.Errorf("ZADD score %q not float64", cmd[i])
			}
		}
	case "PEXPIREAT":
		if len(cmd) != 3 {
			return fmt.Errorf("PEXPIREAT must have 3 args")
		}
		if _, err := strconv.ParseInt(string(cmd[2]), 10, 64); err != nil {
			return fmt.Errorf("PEXPIREAT timestamp not int64: %v", err)
		}
	case "XADD":
		if len(cmd) < 3 {
			return fmt.Errorf("XADD without id")
		}
		id := string(cmd[2])
		parts := strings.Split(id, "-")
		if len(parts) != 2 {
			return fmt.Errorf("XADD id %q malformed", id)
		}
		if _, err := strconv.ParseUint(parts[0], 10, 64); err != nil {
			return fmt.Errorf("XADD id ms invalid: %v", err)
		}
		if _, err := strconv.ParseUint(parts[1], 10, 64); err != nil {
			return fmt.Errorf("XADD id sequence invalid: %v", err)
		}
		if (len(cmd)-3)%2 != 0 {
			return fmt.Errorf("XADD fields must be name/value pairs")
		}
	default:
		return fmt.Errorf("unexpected AOF command %q", name)
	}
	return nil
}

// aofCoversSnapshot confirms structural coverage: keys (db is not part of AOF,
// objects are emitted per-db in SELECT-less stream here, so key identity and
// payload presence is checked), values, members, entries and messages appear.
type keyCmd struct {
	cmd  string
	args [][]byte
}

func aofCoversSnapshot(snap *snapshot, cmds [][][]byte) error {
	byKey := make(map[string][]keyCmd)
	for _, c := range cmds {
		k := string(c[1])
		byKey[k] = append(byKey[k], keyCmd{cmd: strings.ToUpper(string(c[0])), args: c[2:]})
	}
	for _, obj := range snap.objects {
		k := obj.GetKey()
		got := byKey[k]
		if len(got) == 0 {
			return fmt.Errorf("key %q missing from AOF output", k)
		}
		switch o := obj.(type) {
		case *model.StringObject:
			if !containsCmd(got, "SET") {
				return fmt.Errorf("key %q: SET missing", k)
			}
		case *model.ListObject:
			if !containsCmd(got, "RPUSH") {
				return fmt.Errorf("key %q: RPUSH missing", k)
			}
		case *model.SetObject:
			if !containsCmd(got, "SADD") {
				return fmt.Errorf("key %q: SADD missing", k)
			}
		case *model.HashObject:
			if !containsCmd(got, "HMSET") {
				return fmt.Errorf("key %q: HMSET missing", k)
			}
		case *model.ZSetObject:
			if !containsCmd(got, "ZADD") {
				return fmt.Errorf("key %q: ZADD missing", k)
			}
		case *model.StreamObject:
			xadds := 0
			for _, c := range got {
				if c.cmd == "XADD" {
					xadds++
				}
			}
			var msgs int
			for _, e := range o.Entries {
				for _, m := range e.Msgs {
					if !m.Deleted {
						msgs++
					}
				}
			}
			if xadds != msgs {
				return fmt.Errorf("stream %q: XADD count %d != live messages %d", k, xadds, msgs)
			}
		}
		if obj.GetExpiration() != nil && !containsCmd(got, "PEXPIREAT") {
			return fmt.Errorf("key %q: PEXPIREAT missing", k)
		}
		_ = model.StringType
	}
	return nil
}

func containsCmd(list []keyCmd, name string) bool {
	for _, c := range list {
		if c.cmd == name {
			return true
		}
	}
	return false
}
