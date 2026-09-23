package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/helper"
	"github.com/hdt3213/rdb/model"
)

// runAOFStage converts the fixture to AOF (pure Go, no Redis process) and
// validates the structure of the conversion result: the output must be a
// well formed sequence of RESP commands with known command names and valid
// arity, and the command multiset must equal the commands derived from the
// decoded objects.
func runAOFStage(fx *fixture, d *decodedFixture, tmpDir string) error {
	outPath := filepath.Join(tmpDir, strings.ReplaceAll(fx.relPath, "/", "__")+".aof")
	if err := helper.ToAOF(fx.absPath, outPath); err != nil {
		return fmt.Errorf("aof conversion failed: %v", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		return fmt.Errorf("read aof output failed: %v", err)
	}
	commands, err := parseRESPCommands(data)
	if err != nil {
		return fmt.Errorf("aof structure invalid: %v", err)
	}
	for i, cmd := range commands {
		if err := validateCommand(cmd); err != nil {
			return fmt.Errorf("aof command %d invalid: %v", i, err)
		}
	}
	expected := expectedCommands(d.objects)
	actual := make([]string, 0, len(commands))
	for _, cmd := range commands {
		actual = append(actual, canonicalCommand(cmd))
	}
	sort.Strings(actual)
	sort.Strings(expected)
	if len(actual) != len(expected) {
		return fmt.Errorf("aof command count mismatch: got %d commands, expected %d from decoded objects",
			len(actual), len(expected))
	}
	for i := range actual {
		if actual[i] != expected[i] {
			return fmt.Errorf("aof command mismatch at sorted index %d: got %q expected %q",
				i, actual[i], expected[i])
		}
	}
	return nil
}

// expectedCommands derives the canonical command multiset from decoded
// objects using the same ObjectToCmd mapping as the converter.
func expectedCommands(objects []model.RedisObject) []string {
	var cmds []string
	for _, obj := range objects {
		switch obj.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
			continue // special objects produce no AOF commands
		}
		for _, cmdLine := range helper.ObjectToCmd(obj) {
			cmds = append(cmds, canonicalCommand(cmdLine))
		}
	}
	return cmds
}

// parseRESPCommands parses a buffer as a strict sequence of RESP arrays of
// bulk strings. Any trailing garbage or malformed frame is an error.
func parseRESPCommands(data []byte) ([][][]byte, error) {
	var commands [][][]byte
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		cmd, err := parseRESPCommand(r)
		if err != nil {
			return nil, err
		}
		commands = append(commands, cmd)
	}
	return commands, nil
}

func readRESPLine(r *bytes.Reader) (string, error) {
	var line []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return "", fmt.Errorf("unexpected end of input")
		}
		line = append(line, b)
		if b == '\n' {
			break
		}
	}
	if !strings.HasSuffix(string(line), "\r\n") {
		return "", fmt.Errorf("line not terminated by CRLF: %q", line)
	}
	return string(line[:len(line)-2]), nil
}

func parseRESPCommand(r *bytes.Reader) ([][]byte, error) {
	line, err := readRESPLine(r)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected RESP array, got %q", line)
	}
	argc, err := strconv.Atoi(line[1:])
	if err != nil || argc <= 0 {
		return nil, fmt.Errorf("invalid RESP array length %q", line)
	}
	cmd := make([][]byte, 0, argc)
	for i := 0; i < argc; i++ {
		header, err := readRESPLine(r)
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(header, "$") {
			return nil, fmt.Errorf("expected bulk string, got %q", header)
		}
		length, err := strconv.Atoi(header[1:])
		if err != nil || length < 0 {
			return nil, fmt.Errorf("invalid bulk length %q", header)
		}
		buf := make([]byte, length+2)
		if _, err := r.Read(buf); err != nil {
			return nil, fmt.Errorf("unexpected end of bulk string")
		}
		if !bytes.HasSuffix(buf, []byte("\r\n")) {
			return nil, fmt.Errorf("bulk string not terminated by CRLF")
		}
		cmd = append(cmd, buf[:length])
	}
	return cmd, nil
}

// validateCommand checks the command name and arity of one AOF command.
func validateCommand(cmd [][]byte) error {
	if len(cmd) == 0 {
		return fmt.Errorf("empty command")
	}
	name := strings.ToUpper(string(cmd[0]))
	argc := len(cmd)
	switch name {
	case "SET":
		if argc != 3 {
			return fmt.Errorf("SET expects 2 args, got %d", argc-1)
		}
	case "RPUSH", "SADD":
		if argc < 3 {
			return fmt.Errorf("%s expects at least 2 args, got %d", name, argc-1)
		}
	case "HMSET", "ZADD":
		if argc < 4 || argc%2 != 0 {
			return fmt.Errorf("%s expects an even number of pair args, got %d", name, argc-1)
		}
	case "XADD":
		if argc < 5 || (argc-3)%2 != 0 {
			return fmt.Errorf("XADD expects key, id and field pairs, got %d args", argc-1)
		}
	case "PEXPIREAT":
		if argc != 3 {
			return fmt.Errorf("PEXPIREAT expects 2 args, got %d", argc-1)
		}
	case "HPEXPIREAT":
		if argc < 6 || string(cmd[3]) != "FIELDS" {
			return fmt.Errorf("HPEXPIREAT expects key ms FIELDS num field..., got %d args", argc-1)
		}
	case "HPERSIST":
		if argc < 5 || string(cmd[2]) != "FIELDS" {
			return fmt.Errorf("HPERSIST expects key FIELDS num field..., got %d args", argc-1)
		}
	default:
		return fmt.Errorf("unexpected command %q", name)
	}
	return nil
}

// canonicalCommand normalizes a command so that semantically irrelevant
// argument order (set members, hash pairs, zset pairs, stream field pairs)
// does not affect comparison, while list order, key bytes, stream ids and
// expiration timestamps stay significant.
func canonicalCommand(cmd [][]byte) string {
	if len(cmd) == 0 {
		return ""
	}
	name := strings.ToUpper(string(cmd[0]))
	args := cmd[1:]
	join := func(parts [][]byte) string {
		strs := make([]string, 0, len(parts))
		for _, p := range parts {
			strs = append(strs, strconv.Quote(string(p)))
		}
		return strings.Join(strs, " ")
	}
	sortParts := func(parts [][]byte) [][]byte {
		sorted := append([][]byte(nil), parts...)
		sort.Slice(sorted, func(i, j int) bool { return string(sorted[i]) < string(sorted[j]) })
		return sorted
	}
	pairs := func(parts [][]byte) [][]byte {
		merged := make([][]byte, 0, len(parts)/2)
		for i := 0; i+1 < len(parts); i += 2 {
			merged = append(merged, append(append([]byte(nil), parts[i]...), '\x00'))
			merged[len(merged)-1] = append(merged[len(merged)-1], parts[i+1]...)
		}
		return merged
	}
	switch name {
	case "SADD":
		return name + " " + string(args[0]) + " " + join(sortParts(args[1:]))
	case "HMSET", "ZADD":
		return name + " " + string(args[0]) + " " + join(sortParts(pairs(args[1:])))
	case "XADD":
		// XADD key id field value [field value ...]
		return name + " " + string(args[0]) + " " + string(args[1]) + " " + join(sortParts(pairs(args[2:])))
	default:
		return name + " " + join(args)
	}
}
