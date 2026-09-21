package rdbverify

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/helper"
	"github.com/hdt3213/rdb/model"
)

// AOFFinding is a structural observation about an AOF conversion result.
type AOFFinding struct {
	Level  string `json:"level"` // "error" or "warn"
	Code   string `json:"code"`
	Key    string `json:"key,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// AOFReport describes the structural validation of the AOF output.
type AOFReport struct {
	Supported    bool        `json:"supported"`
	CommandCount int         `json:"commandCount"`
	Commands     []string    `json:"commands"` // command names, in order
	Finding      *AOFFinding `json:"finding,omitempty"`
}

var knownAOFCommands = map[string]struct{}{
	"SET":        {},
	"RPUSH":      {},
	"SADD":       {},
	"HMSET":      {},
	"HPEXPIREAT": {},
	"HPERSIST":   {},
	"ZADD":       {},
	"PEXPIREAT":  {},
	"XADD":       {},
	"SELECT":     {},
}

// CheckAOF converts every data object of the decode result to RESP, parses
// the generated RESP back and validates the command structure. It never
// invokes a Redis process.
func CheckAOF(dec *DecodeResult) *AOFReport {
	report := &AOFReport{Supported: true}
	for _, obj := range dec.Objects {
		if _, ok := obj.(*model.AuxObject); ok {
			continue
		}
		if _, ok := obj.(*model.DBSizeObject); ok {
			continue
		}
		cmds := helper.ObjectToCmd(obj, helper.WithLexOrder())
		for _, cmd := range cmds {
			report.CommandCount++
			if len(cmd) == 0 {
				report.Finding = &AOFFinding{Level: "error", Code: "aof-empty-command",
					Key: obj.GetKey(), Detail: "converter produced an empty command line"}
				report.Supported = false
				return report
			}
			name := strings.ToUpper(string(cmd[0]))
			report.Commands = append(report.Commands, name)
			if _, ok := knownAOFCommands[name]; !ok {
				report.Finding = &AOFFinding{Level: "error", Code: "aof-unknown-command",
					Key: obj.GetKey(), Detail: fmt.Sprintf("unknown command %q", name)}
				report.Supported = false
				return report
			}
			if err := validateCommandStructure(obj, name, cmd); err != nil {
				report.Finding = &AOFFinding{Level: "error", Code: "aof-structure",
					Key: obj.GetKey(), Detail: err.Error()}
				report.Supported = false
				return report
			}
		}
		structuralLoss := aofStructuralLoss(obj)
		if structuralLoss != "" {
			// Stream groups/consumers/PELs cannot be expressed as AOF commands
			// via this converter; report it as a structured, expected loss.
			report.Finding = &AOFFinding{Level: "warn", Code: "aof-lossy-construct",
				Key: obj.GetKey(), Detail: structuralLoss}
		}
	}
	if err := validateRespRoundtrip(dec); err != nil {
		report.Finding = &AOFFinding{Level: "error", Code: "aof-resp", Detail: err.Error()}
		report.Supported = false
	}
	return report
}

// validateCommandStructure enforces per-command argument shape.
func validateCommandStructure(obj model.RedisObject, name string, cmd [][]byte) error {
	key := obj.GetKey()
	switch name {
	case "SET":
		if len(cmd) != 3 || string(cmd[1]) != key {
			return fmt.Errorf("SET expects key + value")
		}
	case "RPUSH", "SADD":
		if len(cmd) < 2 || string(cmd[1]) != key {
			return fmt.Errorf("%s expects key + at least one element", name)
		}
	case "HMSET":
		if len(cmd) < 4 || (len(cmd)-2)%2 != 0 {
			return fmt.Errorf("HMSET expects key + field/value pairs")
		}
	case "ZADD":
		if len(cmd) < 4 || (len(cmd)-2)%2 != 0 {
			return fmt.Errorf("ZADD expects key + score/member pairs")
		}
		for i := 2; i < len(cmd); i += 2 {
			if _, err := strconv.ParseFloat(string(cmd[i]), 64); err != nil {
				return fmt.Errorf("ZADD score %q is not a float64: %v", string(cmd[i]), err)
			}
		}
	case "PEXPIREAT":
		if len(cmd) != 3 {
			return fmt.Errorf("PEXPIREAT expects key + ms timestamp")
		}
		if _, err := strconv.ParseInt(string(cmd[2]), 10, 64); err != nil {
			return fmt.Errorf("PEXPIREAT timestamp %q invalid", string(cmd[2]))
		}
	case "HPEXPIREAT":
		if len(cmd) != 6 || string(cmd[3]) != "FIELDS" || string(cmd[4]) != "1" {
			return fmt.Errorf("HPEXPIREAT expects key ms FIELDS 1 field")
		}
	case "HPERSIST":
		if len(cmd) != 5 || string(cmd[2]) != "FIELDS" || string(cmd[3]) != "1" {
			return fmt.Errorf("HPERSIST expects key FIELDS 1 field")
		}
	case "XADD":
		if len(cmd) < 4 || (len(cmd)-3)%2 != 0 {
			return fmt.Errorf("XADD expects key id field/value pairs")
		}
		if _, err := parseStreamIDText(string(cmd[2])); err != nil {
			return fmt.Errorf("XADD id %q invalid: %v", string(cmd[2]), err)
		}
	case "SELECT":
		if len(cmd) != 2 {
			return fmt.Errorf("SELECT expects db index")
		}
	}
	return nil
}

func parseStreamIDText(s string) (struct{ ms, seq uint64 }, error) {
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return struct{ ms, seq uint64 }{}, fmt.Errorf("not ms-seq")
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return struct{ ms, seq uint64 }{}, err
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return struct{ ms, seq uint64 }{}, err
	}
	return struct{ ms, seq uint64 }{ms, seq}, nil
}

// aofStructuralLoss reports constructs the AOF converter cannot represent.
func aofStructuralLoss(obj model.RedisObject) string {
	switch o := obj.(type) {
	case *model.StreamObject:
		var parts []string
		if len(o.Groups) > 0 {
			parts = append(parts, fmt.Sprintf("%d consumer group(s)", len(o.Groups)))
		}
		for _, g := range o.Groups {
			if len(g.Pending) > 0 {
				parts = append(parts, fmt.Sprintf("group %q PEL %d", g.Name, len(g.Pending)))
			}
			if len(g.Consumers) > 0 {
				parts = append(parts, fmt.Sprintf("group %q %d consumer(s)", g.Name, len(g.Consumers)))
			}
		}
		if len(parts) > 0 {
			return "stream AOF conversion omits: " + strings.Join(parts, ", ")
		}
	case *model.FunctionsObject:
		return "AOF converter cannot emit function libraries"
	}
	return ""
}

// validateRespRoundtrip serializes all objects and re-parses the RESP,
// verifying framing integrity and that every command roundtrips exactly.
func validateRespRoundtrip(dec *DecodeResult) error {
	var buf bytes.Buffer
	var expected [][][]byte
	for _, obj := range dec.Objects {
		if _, ok := obj.(*model.AuxObject); ok {
			continue
		}
		if _, ok := obj.(*model.DBSizeObject); ok {
			continue
		}
		cmds := helper.ObjectToCmd(obj, helper.WithLexOrder())
		buf.Write(helper.CmdLinesToResp(cmds))
		expected = append(expected, cmds...)
	}
	parsed, err := parseRESP(buf.Bytes())
	if err != nil {
		return err
	}
	if len(parsed) != len(expected) {
		return fmt.Errorf("resp command count mismatch: generated %d, reparsed %d", len(expected), len(parsed))
	}
	for i := range expected {
		if len(parsed[i]) != len(expected[i]) {
			return fmt.Errorf("resp command %d arg count mismatch", i)
		}
		for j := range expected[i] {
			if !bytes.Equal(parsed[i][j], expected[i][j]) {
				return fmt.Errorf("resp command %d arg %d mismatch", i, j)
			}
		}
	}
	return nil
}

// parseRESP parses inline RESP2 arrays (*N / $len / bulk), enough for AOF.
func parseRESP(data []byte) ([][][]byte, error) {
	r := bytes.NewReader(data)
	var cmds [][][]byte
	for {
		line, err := readLine(r)
		if err != nil {
			break
		}
		if len(line) == 0 || line[0] != '*' {
			return nil, fmt.Errorf("expected array prefix, got %q", safePrint(line))
		}
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, fmt.Errorf("bad array count: %v", err)
		}
		cmd := make([][]byte, n)
		for i := 0; i < n; i++ {
			hdr, err := readLine(r)
			if err != nil {
				return nil, err
			}
			if len(hdr) == 0 || hdr[0] != '$' {
				return nil, fmt.Errorf("expected bulk prefix, got %q", safePrint(hdr))
			}
			size, err := strconv.Atoi(string(hdr[1:]))
			if err != nil {
				return nil, fmt.Errorf("bad bulk length: %v", err)
			}
			bulk := make([]byte, size)
			if _, err := r.Read(bulk); err != nil {
				return nil, err
			}
			trailer, err := readLine(r)
			if err != nil || string(trailer) != "" {
				return nil, fmt.Errorf("bad bulk trailer")
			}
			cmd[i] = bulk
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

func readLine(r *bytes.Reader) ([]byte, error) {
	var line []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\r' {
			nxt, err := r.ReadByte()
			if err != nil || nxt != '\n' {
				return nil, fmt.Errorf("expected CRLF")
			}
			return line, nil
		}
		line = append(line, b)
	}
}

// sortedCommandSummary returns unique sorted command names for the report.
func sortedCommandSummary(cmds []string) []string {
	seen := map[string]struct{}{}
	for _, c := range cmds {
		seen[c] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
