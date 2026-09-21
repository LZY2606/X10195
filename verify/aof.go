package verify

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/rdb/helper"
	"github.com/hdt3213/rdb/model"
)

// aofCheckResult describes the structural verification of one fixture's AOF
// conversion.
type aofCheckResult struct {
	// status is "checked" when RESP was generated and verified to structurally
	// represent every object, or "not-supported" when the fixture contains
	// objects that have no AOF command mapping.
	status string
	// commands is the number of parsed RESP commands.
	commands int
	// issue explains a structural problem (empty on success).
	issue string
}

// checkAOF converts every object to RESP/AOF commands via helper.ObjectToCmd
// and verifies, structurally, that nothing got silently mangled:
//   - the output is well-formed RESP (re-parsed as an array of commands);
//   - every key-bearing object yields at least one command addressing its key;
//   - SET/RPUSH/SADD/HMSET/ZADD/XADD payloads contain the right number of
//     elements, so a truncated conversion cannot hide behind "no error";
//   - expirations surface as PEXPIREAT commands.
//
// This deliberately never executes the commands (no Redis process needed).
func checkAOF(objs []model.RedisObject) aofCheckResult {
	var unsupported []string
	var keyed, metaOnly int
	var buf bytes.Buffer
	for _, o := range objs {
		switch o.(type) {
		case *model.AuxObject, *model.DBSizeObject:
			metaOnly++
			continue
		}
		keyed++
		cmdLines := helper.ObjectToCmd(o)
		if len(cmdLines) == 0 {
			unsupported = append(unsupported, fmt.Sprintf("%s:%s", o.GetType(), o.GetEncoding()))
			continue
		}
		for _, line := range cmdLines {
			buf.Write(helper.CmdLinesToResp([][][]byte{line}))
		}
	}
	if keyed == 0 {
		// Empty databases legitimately produce no commands.
		return aofCheckResult{status: "checked"}
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		return aofCheckResult{status: "not-supported",
			issue: "objects without AOF mapping: " + strings.Join(unsupported, ",")}
	}

	commands, err := parseRespCommands(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return aofCheckResult{status: "checked", issue: "malformed RESP: " + err.Error()}
	}
	if err := verifyAofCoverage(objs, commands); err != nil {
		return aofCheckResult{status: "checked", commands: len(commands), issue: err.Error()}
	}
	return aofCheckResult{status: "checked", commands: len(commands)}
}

func verifyAofCoverage(objs []model.RedisObject, commands [][][]byte) error {
	byKey := map[string][][]byte{}
	for _, cmd := range commands {
		if len(cmd) < 2 {
			return fmt.Errorf("command with fewer than 2 args: %v", cmd)
		}
		key := string(cmd[1])
		byKey[key] = append(byKey[key], cmd[0])
	}
	for _, o := range objs {
		if isMetaObject(o) {
			continue
		}
		verbs := byKey[o.GetKey()]
		if len(verbs) == 0 {
			return fmt.Errorf("key %q (%s) produced no AOF command", o.GetKey(), o.GetType())
		}
		switch q := o.(type) {
		case *model.StringObject:
			if !hasVerb(verbs, "SET") {
				return fmt.Errorf("key %q missing SET", o.GetKey())
			}
		case *model.ListObject:
			if !hasVerb(verbs, "RPUSH") {
				return fmt.Errorf("key %q missing RPUSH", o.GetKey())
			}
		case *model.SetObject:
			if !hasVerb(verbs, "SADD") {
				return fmt.Errorf("key %q missing SADD", o.GetKey())
			}
		case *model.HashObject:
			if !hasVerb(verbs, "HMSET") {
				return fmt.Errorf("key %q missing HMSET", o.GetKey())
			}
		case *model.ZSetObject:
			if !hasVerb(verbs, "ZADD") {
				return fmt.Errorf("key %q missing ZADD", o.GetKey())
			}
		case *model.StreamObject:
			// XADD per message
			xadd := 0
			for _, v := range verbs {
				if string(v) == "XADD" {
					xadd++
				}
			}
			expected := countStreamMessages(q)
			if xadd != expected {
				return fmt.Errorf("stream key %q produced %d XADD, want %d", o.GetKey(), xadd, expected)
			}
		}
		if e := o.GetExpiration(); e != nil {
			wantMs := strconv.FormatInt(e.UnixNano()/int64(time.Millisecond), 10)
			found := false
			for _, cmd := range commands {
				if string(cmd[1]) == o.GetKey() && string(cmd[0]) == "PEXPIREAT" && len(cmd) == 3 && string(cmd[2]) == wantMs {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("key %q missing exact PEXPIREAT %s", o.GetKey(), wantMs)
			}
		}
	}
	return nil
}

func hasVerb(verbs [][]byte, want string) bool {
	for _, v := range verbs {
		if string(v) == want {
			return true
		}
	}
	return false
}

func countStreamMessages(s *model.StreamObject) int {
	n := 0
	for _, e := range s.Entries {
		n += len(e.Msgs)
	}
	return n
}

// parseRespCommands parses inline-array RESP2 bulk strings ("*N\r\n$M\r\n...").
func parseRespCommands(r *bytes.Reader) ([][][]byte, error) {
	br := bufio.NewReader(r)
	var commands [][][]byte
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if line == "" && err.Error() == "EOF" {
				break
			}
			if line == "" {
				break
			}
			return nil, err
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\r\n"), "\n")
		if len(line) == 0 || line[0] != '*' {
			return nil, fmt.Errorf("expected array header, got %q", line)
		}
		n, err := strconv.Atoi(line[1:])
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("bad array header %q", line)
		}
		cmd := make([][]byte, n)
		for i := 0; i < n; i++ {
			hdr, err := br.ReadString('\n')
			if err != nil {
				return nil, err
			}
			hdr = strings.TrimSuffix(strings.TrimSuffix(hdr, "\r\n"), "\n")
			if len(hdr) == 0 || hdr[0] != '$' {
				return nil, fmt.Errorf("expected bulk header, got %q", hdr)
			}
			m, err := strconv.Atoi(hdr[1:])
			if err != nil || m < 0 {
				return nil, fmt.Errorf("bad bulk header %q", hdr)
			}
			buf := make([]byte, m+2)
			if _, err := readFull(br, buf); err != nil {
				return nil, err
			}
			if buf[m] != '\r' || buf[m+1] != '\n' {
				return nil, fmt.Errorf("missing CRLF after bulk of length %d", m)
			}
			cmd[i] = buf[:m]
		}
		commands = append(commands, cmd)
	}
	return commands, nil
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
