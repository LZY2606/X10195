package verify

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/helper"
	"github.com/hdt3213/rdb/model"
)

// respCommand is one parsed RESP inline command.
type respCommand struct {
	Name string
	Args [][]byte
}

// aofInspection is the structural result of converting one fixture to AOF.
type aofInspection struct {
	// Supported counts number of data objects the converter emits commands for.
	Supported int
	// Skipped lists object types the converter does not translate.
	Skipped []string
	// Commands is the fully parsed RESP stream; empty on parse error.
	Commands []respCommand
}

func parseRESP(data []byte) ([]respCommand, error) {
	r := bufio.NewReader(bytes.NewReader(data))
	var commands []respCommand
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if len(line) == 0 && err.Error() == "EOF" {
				break
			}
			if strings.Contains(err.Error(), "EOF") && len(line) == 0 {
				break
			}
			if len(line) == 0 {
				break
			}
			return nil, fmt.Errorf("truncated RESP stream: %w", err)
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			continue
		}
		if line[0] != '*' {
			return nil, fmt.Errorf("expected RESP array header, got %q", safeLine(line))
		}
		argc, err := strconv.Atoi(string(line[1:]))
		if err != nil || argc <= 0 {
			return nil, fmt.Errorf("invalid RESP array count %q", safeLine(line))
		}
		cmd := respCommand{Args: make([][]byte, 0, argc)}
		for i := 0; i < argc; i++ {
			header, err := r.ReadBytes('\n')
			if err != nil && !strings.Contains(err.Error(), "EOF") {
				return nil, fmt.Errorf("truncated bulk string header: %w", err)
			}
			header = bytes.TrimRight(header, "\r\n")
			if len(header) == 0 || header[0] != '$' {
				return nil, fmt.Errorf("expected RESP bulk string header, got %q", safeLine(header))
			}
			n, err := strconv.Atoi(string(header[1:]))
			if err != nil || n < 0 {
				return nil, fmt.Errorf("invalid RESP bulk length %q", safeLine(header))
			}
			buf := make([]byte, n+2)
			if _, err := readFull(r, buf); err != nil {
				return nil, fmt.Errorf("truncated bulk string body: %w", err)
			}
			if buf[n] != '\r' || buf[n+1] != '\n' {
				return nil, fmt.Errorf("bulk string missing CRLF terminator")
			}
			cmd.Args = append(cmd.Args, buf[:n])
		}
		if len(cmd.Args) == 0 {
			return nil, fmt.Errorf("RESP command without arguments")
		}
		cmd.Name = strings.ToUpper(string(cmd.Args[0]))
		commands = append(commands, cmd)
	}
	return commands, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
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

func safeLine(line []byte) string {
	if len(line) > 64 {
		return string(line[:64]) + "..."
	}
	return string(line)
}

var knownCommands = map[string]struct{}{
	"SET": {}, "RPUSH": {}, "SADD": {}, "HMSET": {}, "ZADD": {},
	"PEXPIREAT": {}, "XADD": {}, "HPEXPIREAT": {}, "HPERSIST": {},
}

// inspectAOF converts objects to AOF RESP bytes and checks the structure.
func inspectAOF(objects []model.RedisObject) (*aofInspection, []Diff) {
	out := &aofInspection{}
	var raw bytes.Buffer
	skipSeen := map[string]struct{}{}
	for _, obj := range objects {
		switch obj.GetType() {
		case model.AuxType, model.DBSizeType:
			continue
		case model.FunctionsType:
			skipSeen["functions"] = struct{}{}
			continue
		}
		cmdLines := helper.ObjectToCmd(obj)
		if len(cmdLines) == 0 {
			skipSeen[obj.GetType()] = struct{}{}
			continue
		}
		out.Supported++
		raw.Write(helper.CmdLinesToResp(cmdLines))
	}
	for k := range skipSeen {
		out.Skipped = append(out.Skipped, k)
	}

	var diffs []Diff
	commands, err := parseRESP(raw.Bytes())
	if err != nil {
		diffs = append(diffs, Diff{Msg: "AOF RESP parse failed: " + err.Error()})
		return out, diffs
	}
	out.Commands = commands

	for _, cmd := range commands {
		if _, ok := knownCommands[cmd.Name]; !ok {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("AOF uses unknown command %q", cmd.Name)})
		}
		if len(cmd.Args) < 2 {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("AOF command %s missing key argument", cmd.Name)})
		}
	}

	// Structural coverage: every data object must either produce commands or
	// be explicitly reported as skipped by the converter.
	coveredKeys := map[string]struct{}{}
	for _, cmd := range commands {
		if len(cmd.Args) >= 2 {
			coveredKeys[string(cmd.Args[1])] = struct{}{}
		}
	}
	for _, obj := range objects {
		switch obj.GetType() {
		case model.AuxType, model.DBSizeType, model.FunctionsType:
			continue
		}
		if _, ok := coveredKeys[obj.GetKey()]; !ok {
			diffs = append(diffs, Diff{Msg: fmt.Sprintf("AOF conversion lost key %q (type %s)", obj.GetKey(), obj.GetType())})
		}
	}
	return out, diffs
}
