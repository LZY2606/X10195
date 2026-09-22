package verify

import (
	"fmt"
	"strconv"

	"github.com/hdt3213/rdb/helper"
	"github.com/hdt3213/rdb/model"
)

// aofStats summarizes the RESP/AOF conversion of one fixture.
type aofStats struct {
	commands    map[string]int
	totalArgs   int
	keys        map[string]bool
	streamKeys  map[string]bool
	streamXADDs map[string]int
	// streamGroupMetadata is true when the source stream carries groups,
	// consumers or PEL entries that the AOF converter cannot express.
	streamGroupMetadata bool
}

// checkAOF converts every object to RESP command lines (in memory, no Redis
// process involved), parses the emitted RESP back and asserts structural
// validity: every command is a well-formed multi-bulk with matching header
// lengths and declared bulk sizes, each data object produces at least one
// command, and expirations become PEXPIREAT commands with the exact ms value.
func checkAOF(decoded *decodeResult) (*aofStats, []string) {
	stats := &aofStats{
		commands:    map[string]int{},
		keys:        map[string]bool{},
		streamKeys:  map[string]bool{},
		streamXADDs: map[string]int{},
	}
	var dataObjects int
	var respBuf []byte
	for _, o := range decoded.objects {
		switch o.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
			continue
		}
		dataObjects++
		stats.keys[o.GetKey()] = true
		if s, ok := o.(*model.StreamObject); ok {
			stats.streamKeys[s.GetKey()] = true
			if len(s.Groups) > 0 {
				stats.streamGroupMetadata = true
			}
		}
		lines := helper.ObjectToCmd(o)
		if len(lines) == 0 {
			return stats, []string{fmt.Sprintf("aof: object %q (%s) produced no commands", o.GetKey(), o.GetType())}
		}
		for _, line := range lines {
			respBuf = append(respBuf, helper.CmdLinesToResp([]helper.CmdLine{line})...)
		}
	}

	// Parse the generated RESP bytes back and verify structure.
	parsed, err := parseRESP(respBuf)
	if err != nil {
		return stats, []string{fmt.Sprintf("aof: malformed RESP output: %v", err)}
	}
	if len(parsed) == 0 && dataObjects > 0 {
		return stats, []string{"aof: expected at least one RESP command"}
	}

	var diffs []string
	seenDataKeys := map[string]bool{}
	for _, cmd := range parsed {
		if len(cmd) == 0 {
			diffs = append(diffs, "aof: empty command line")
			continue
		}
		name := string(cmd[0])
		stats.commands[name]++
		stats.totalArgs += len(cmd)
		if len(cmd) < 2 && name != "SELECT" {
			diffs = append(diffs, fmt.Sprintf("aof: command %s missing key argument", name))
		}
		if len(cmd) >= 2 {
			key := string(cmd[1])
			switch name {
			case "SET", "RPUSH", "SADD", "HMSET", "ZADD", "XADD":
				seenDataKeys[key] = true
			}
			if name == "XADD" {
				stats.streamXADDs[key]++
			}
		}
	}

	// Every convertible object's key must appear (functions intentionally do not).
	for _, o := range decoded.objects {
		switch o.(type) {
		case *model.AuxObject, *model.DBSizeObject, *model.FunctionsObject:
			continue
		}
		if !seenDataKeys[o.GetKey()] {
			diffs = append(diffs, fmt.Sprintf("aof: key %q not represented in command stream", o.GetKey()))
		}
	}

	// Expirations must survive as PEXPIREAT with millisecond precision.
	expireCount := 0
	for _, cmd := range parsed {
		if string(cmd[0]) == "PEXPIREAT" {
			expireCount++
			if len(cmd) != 3 {
				diffs = append(diffs, "aof: malformed PEXPIREAT command")
				continue
			}
			ms, err := strconv.ParseInt(string(cmd[2]), 10, 64)
			if err != nil {
				diffs = append(diffs, fmt.Sprintf("aof: PEXPIREAT %q has non-numeric time", string(cmd[1])))
				continue
			}
			key := string(cmd[1])
			var origin int64
			for _, o := range decoded.objects {
				if o.GetKey() == key && o.GetExpiration() != nil {
					origin = o.GetExpiration().UnixNano() / 1e6
				}
			}
			if origin != 0 && origin != ms {
				diffs = append(diffs, fmt.Sprintf("aof: PEXPIREAT for %q precision lost: %d != %d", key, origin, ms))
			}
		}
	}
	wantExpires := 0
	for _, o := range decoded.objects {
		if o.GetExpiration() != nil {
			wantExpires++
		}
	}
	if wantExpires != expireCount {
		diffs = append(diffs, fmt.Sprintf("aof: expiration commands %d != objects with TTL %d", expireCount, wantExpires))
	}

	// Streams with group metadata necessarily lose it in AOF form: surface as
	// a structured loss rather than pretending XADD is a complete conversion.
	if stats.streamGroupMetadata {
		diffs = append(diffs, "aof: stream groups/consumers/PEL are not expressible by XADD conversion")
	}
	return stats, diffs
}
