package main

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/hdt3213/rdb/helper"
	"github.com/hdt3213/rdb/model"
)

// aofConvertible reports whether helper.ObjectToCmd supports the object type.
func aofConvertible(obj model.RedisObject) bool {
	switch obj.GetType() {
	case model.StringType, model.ListType, model.SetType,
		model.HashType, model.ZSetType, model.StreamType:
		return true
	}
	return false
}

// checkAOF converts one object to AOF (RESP) form and validates the structure
// of the produced commands: command names, raw key bytes, element counts and
// per-type payload, plus the trailing PEXPIREAT for expiring objects.
func checkAOF(obj model.RedisObject) error {
	cmds := helper.ObjectToCmd(obj)
	if len(cmds) == 0 {
		return fmt.Errorf("aof conversion produced no commands")
	}
	raw := helper.CmdLinesToResp(cmds)
	parsed, err := parseRESPCommands(raw)
	if err != nil {
		return fmt.Errorf("aof output is not valid RESP: %v", err)
	}
	if len(parsed) != len(cmds) {
		return fmt.Errorf("aof RESP round trip changed command count: %d != %d", len(parsed), len(cmds))
	}
	for i := range cmds {
		if len(parsed[i]) != len(cmds[i]) {
			return fmt.Errorf("aof RESP round trip changed arg count of command %d", i)
		}
		for j := range cmds[i] {
			if !bytes.Equal(parsed[i][j], cmds[i][j]) {
				return fmt.Errorf("aof RESP round trip altered command %d arg %d", i, j)
			}
		}
	}
	if err := checkCommandStructure(obj, parsed); err != nil {
		return err
	}
	if expiration := obj.GetExpiration(); expiration != nil {
		last := parsed[len(parsed)-1]
		if len(last) != 3 || string(last[0]) != "PEXPIREAT" {
			return fmt.Errorf("aof output missing trailing PEXPIREAT for expiring key")
		}
		if !bytes.Equal(last[1], []byte(obj.GetKey())) {
			return fmt.Errorf("aof PEXPIREAT key mismatch")
		}
		if string(last[2]) != strconv.FormatInt(expiration.UnixMilli(), 10) {
			return fmt.Errorf("aof PEXPIREAT precision loss: got %s want %d", last[2], expiration.UnixMilli())
		}
	}
	return nil
}

func checkCommandStructure(obj model.RedisObject, cmds [][][]byte) error {
	key := []byte(obj.GetKey())
	first := cmds[0]
	if len(first) < 2 || !bytes.Equal(first[1], key) {
		return fmt.Errorf("aof first command does not address the object key")
	}
	switch o := obj.(type) {
	case *model.StringObject:
		if len(first) != 3 || string(first[0]) != "SET" || !bytes.Equal(first[2], o.Value) {
			return fmt.Errorf("aof SET structure mismatch")
		}
	case *model.ListObject:
		if string(first[0]) != "RPUSH" || len(first) != 2+len(o.Values) {
			return fmt.Errorf("aof RPUSH structure mismatch")
		}
		for i, v := range o.Values {
			if !bytes.Equal(first[2+i], v) {
				return fmt.Errorf("aof RPUSH element %d mismatch", i)
			}
		}
	case *model.SetObject:
		if string(first[0]) != "SADD" || len(first) != 2+len(o.Members) {
			return fmt.Errorf("aof SADD structure mismatch")
		}
		want := make(map[string]bool)
		for _, m := range o.Members {
			want[string(m)] = true
		}
		for _, arg := range first[2:] {
			if !want[string(arg)] {
				return fmt.Errorf("aof SADD carries unknown member %q", arg)
			}
			delete(want, string(arg))
		}
		if len(want) > 0 {
			return fmt.Errorf("aof SADD misses %d members", len(want))
		}
	case *model.HashObject:
		if string(first[0]) != "HMSET" || len(first) != 2+2*len(o.Hash) {
			return fmt.Errorf("aof HMSET structure mismatch")
		}
		for i := 2; i+1 < len(first); i += 2 {
			field, value := string(first[i]), first[i+1]
			want, ok := o.Hash[field]
			if !ok || !bytes.Equal(want, value) {
				return fmt.Errorf("aof HMSET field %q mismatch", field)
			}
		}
		if len(o.FieldExpirations) > 0 {
			if err := checkHashFieldExpireCmds(o, cmds[1:]); err != nil {
				return err
			}
		}
	case *model.ZSetObject:
		if string(first[0]) != "ZADD" || len(first) != 2+2*len(o.Entries) {
			return fmt.Errorf("aof ZADD structure mismatch")
		}
		want := make(map[string]float64)
		for _, e := range o.Entries {
			want[e.Member] = e.Score
		}
		for i := 2; i+1 < len(first); i += 2 {
			score, err := strconv.ParseFloat(string(first[i]), 64)
			if err != nil {
				return fmt.Errorf("aof ZADD score %q not parseable", first[i])
			}
			member := string(first[i+1])
			w, ok := want[member]
			if !ok || w != score {
				return fmt.Errorf("aof ZADD member %q score mismatch", member)
			}
			delete(want, member)
		}
		if len(want) > 0 {
			return fmt.Errorf("aof ZADD misses %d members", len(want))
		}
	case *model.StreamObject:
		want := make(map[string]map[string]string)
		for _, entry := range o.Entries {
			for _, msg := range entry.Msgs {
				want[fmt.Sprintf("%d-%d", msg.Id.Ms, msg.Id.Sequence)] = msg.Fields
			}
		}
		seen := 0
		for _, cmd := range cmds {
			if len(cmd) < 3 || string(cmd[0]) != "XADD" {
				continue
			}
			if !bytes.Equal(cmd[1], key) {
				return fmt.Errorf("aof XADD key mismatch")
			}
			fields, ok := want[string(cmd[2])]
			if !ok {
				return fmt.Errorf("aof XADD carries unknown message id %q", cmd[2])
			}
			if len(cmd[3:]) != 2*len(fields) {
				return fmt.Errorf("aof XADD message %q field count mismatch", cmd[2])
			}
			got := make(map[string]string)
			for i := 3; i+1 < len(cmd); i += 2 {
				got[string(cmd[i])] = string(cmd[i+1])
			}
			for field, value := range fields {
				if got[field] != value {
					return fmt.Errorf("aof XADD message %q field %q mismatch", cmd[2], field)
				}
			}
			seen++
		}
		if seen != len(want) {
			return fmt.Errorf("aof stream message count mismatch: %d != %d", seen, len(want))
		}
	}
	return nil
}

// checkHashFieldExpireCmds verifies HPEXPIREAT/HPERSIST commands cover every
// field expiration of an HFE hash.
func checkHashFieldExpireCmds(o *model.HashObject, cmds [][][]byte) error {
	key := []byte(o.GetKey())
	covered := make(map[string]bool)
	for _, cmd := range cmds {
		if len(cmd) < 5 || !bytes.Equal(cmd[1], key) {
			return fmt.Errorf("aof hash field expiration command malformed")
		}
		name := string(cmd[0])
		switch name {
		case "HPEXPIREAT":
			if len(cmd) != 6 || string(cmd[3]) != "FIELDS" || string(cmd[4]) != "1" {
				return fmt.Errorf("aof HPEXPIREAT structure mismatch")
			}
			field := string(cmd[5])
			want, ok := o.FieldExpirations[field]
			if !ok || want == 0 {
				return fmt.Errorf("aof HPEXPIREAT references field %q without expiration", field)
			}
			if string(cmd[2]) != strconv.FormatInt(want, 10) {
				return fmt.Errorf("aof HPEXPIREAT field %q expiration precision loss", field)
			}
			covered[field] = true
		case "HPERSIST":
			if len(cmd) != 5 || string(cmd[2]) != "FIELDS" || string(cmd[3]) != "1" {
				return fmt.Errorf("aof HPERSIST structure mismatch")
			}
			field := string(cmd[4])
			if exp, ok := o.FieldExpirations[field]; !ok || exp != 0 {
				return fmt.Errorf("aof HPERSIST references field %q with expiration", field)
			}
			covered[field] = true
		default:
			return fmt.Errorf("unexpected aof command %q for HFE hash", name)
		}
	}
	for field := range o.FieldExpirations {
		if !covered[field] {
			return fmt.Errorf("aof output misses field expiration for %q", field)
		}
	}
	return nil
}

// parseRESPCommands parses a sequence of RESP multi-bulk commands.
func parseRESPCommands(raw []byte) ([][][]byte, error) {
	var cmds [][][]byte
	pos := 0
	readLine := func() ([]byte, error) {
		idx := bytes.Index(raw[pos:], []byte("\r\n"))
		if idx < 0 {
			return nil, fmt.Errorf("unterminated line at offset %d", pos)
		}
		line := raw[pos : pos+idx]
		pos += idx + 2
		return line, nil
	}
	for pos < len(raw) {
		if raw[pos] != '*' {
			return nil, fmt.Errorf("expected multi-bulk at offset %d", pos)
		}
		pos++
		line, err := readLine()
		if err != nil {
			return nil, err
		}
		argc, err := strconv.Atoi(string(line))
		if err != nil || argc < 0 {
			return nil, fmt.Errorf("invalid multi-bulk length %q", line)
		}
		cmd := make([][]byte, 0, argc)
		for i := 0; i < argc; i++ {
			if pos >= len(raw) || raw[pos] != '$' {
				return nil, fmt.Errorf("expected bulk string at offset %d", pos)
			}
			pos++
			line, err := readLine()
			if err != nil {
				return nil, err
			}
			size, err := strconv.Atoi(string(line))
			if err != nil || size < 0 {
				return nil, fmt.Errorf("invalid bulk length %q", line)
			}
			if pos+size+2 > len(raw) {
				return nil, fmt.Errorf("truncated bulk string at offset %d", pos)
			}
			cmd = append(cmd, raw[pos:pos+size])
			pos += size
			if !bytes.Equal(raw[pos:pos+2], []byte("\r\n")) {
				return nil, fmt.Errorf("missing CRLF after bulk string at offset %d", pos)
			}
			pos += 2
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

