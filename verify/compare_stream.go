package verify

import (
	"fmt"
	"sort"

	"github.com/hdt3213/rdb/model"
)

// compareStreams performs a strict comparison of stream semantics:
// version metadata, entry listpack structure, message ids, fields, last/first
// ids, counters and the complete consumer group / consumer / PEL ownership.
func compareStreams(prefix string, a, b *model.StreamObject) []string {
	var diffs []string
	if a.Version != b.Version {
		diffs = append(diffs, fmt.Sprintf("%s stream version: %d != %d", prefix, a.Version, b.Version))
	}
	if a.Length != b.Length {
		diffs = append(diffs, fmt.Sprintf("%s length: %d != %d", prefix, a.Length, b.Length))
	}
	if !idEqual(a.LastId, b.LastId) {
		diffs = append(diffs, fmt.Sprintf("%s lastId: %s != %s", prefix, fmtID(a.LastId), fmtID(b.LastId)))
	}
	if a.Version >= 2 {
		if !idEqual(a.FirstId, b.FirstId) {
			diffs = append(diffs, fmt.Sprintf("%s firstId: %s != %s", prefix, fmtID(a.FirstId), fmtID(b.FirstId)))
		}
		if !idEqual(a.MaxDeletedId, b.MaxDeletedId) {
			diffs = append(diffs, fmt.Sprintf("%s maxDeletedId: %s != %s", prefix, fmtID(a.MaxDeletedId), fmtID(b.MaxDeletedId)))
		}
		if a.AddedEntriesCount != b.AddedEntriesCount {
			diffs = append(diffs, fmt.Sprintf("%s addedEntriesCount: %d != %d", prefix, a.AddedEntriesCount, b.AddedEntriesCount))
		}
	}
	diffs = append(diffs, compareStreamEntries(prefix, a.Entries, b.Entries)...)
	diffs = append(diffs, compareStreamGroups(prefix, a.Groups, b.Groups)...)
	return diffs
}

func idEqual(a, b *model.StreamId) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Ms == b.Ms && a.Sequence == b.Sequence
}

func fmtID(id *model.StreamId) string {
	if id == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d-%d", id.Ms, id.Sequence)
}

// flattenEntries expands listpack nodes into an ordered list of messages,
// preserving master field names and deleted markers.
func flattenEntries(entries []*model.StreamEntry) []flatMsg {
	var out []flatMsg
	for _, entry := range entries {
		master := append([]string(nil), entry.Fields...)
		for _, msg := range entry.Msgs {
			out = append(out, flatMsg{
				firstMsg: entry.FirstMsgId,
				master:   master,
				msg:      msg,
			})
		}
	}
	return out
}

type flatMsg struct {
	firstMsg *model.StreamId
	master   []string
	msg      *model.StreamMessage
}

func compareStreamEntries(prefix string, a, b []*model.StreamEntry) []string {
	am := flattenEntries(a)
	bm := flattenEntries(b)
	var diffs []string
	if len(am) != len(bm) {
		diffs = append(diffs, fmt.Sprintf("%s message count: %d != %d", prefix, len(am), len(bm)))
	}
	n := len(am)
	if len(bm) < n {
		n = len(bm)
	}
	for i := 0; i < n; i++ {
		x, y := am[i], bm[i]
		if !idEqual(x.msg.Id, y.msg.Id) {
			diffs = append(diffs, fmt.Sprintf("%s msg[%d] id: %s != %s", prefix, i, fmtID(x.msg.Id), fmtID(y.msg.Id)))
		}
		if x.msg.Deleted != y.msg.Deleted {
			diffs = append(diffs, fmt.Sprintf("%s msg[%d] (%s) deleted: %v != %v", prefix, i, fmtID(x.msg.Id), x.msg.Deleted, y.msg.Deleted))
		}
		if !idEqual(x.firstMsg, y.firstMsg) {
			diffs = append(diffs, fmt.Sprintf("%s msg[%d] (%s) listpack first id: %s != %s", prefix, i, fmtID(x.msg.Id), fmtID(x.firstMsg), fmtID(y.firstMsg)))
		}
		if !stringSliceEqual(x.master, y.master) {
			diffs = append(diffs, fmt.Sprintf("%s msg[%d] (%s) master fields changed", prefix, i, fmtID(x.msg.Id)))
		}
		if !stringMapEqual(x.msg.Fields, y.msg.Fields) {
			diffs = append(diffs, fmt.Sprintf("%s msg[%d] (%s) fields changed", prefix, i, fmtID(x.msg.Id)))
		}
	}
	return diffs
}

func compareStreamGroups(prefix string, a, b []*model.StreamGroup) []string {
	var diffs []string
	if len(a) != len(b) {
		diffs = append(diffs, fmt.Sprintf("%s group count: %d != %d", prefix, len(a), len(b)))
	}
	an := groupNames(a)
	bn := groupNames(b)
	if !stringSliceEqual(an, bn) {
		diffs = append(diffs, fmt.Sprintf("%s group names: %v != %v", prefix, an, bn))
	}
	bg := map[string]*model.StreamGroup{}
	for _, g := range b {
		bg[g.Name] = g
	}
	for _, g := range a {
		gb, ok := bg[g.Name]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("%s group %q missing after re-encode", prefix, g.Name))
			continue
		}
		gp := prefix + " group " + g.Name
		if !idEqual(g.LastId, gb.LastId) {
			diffs = append(diffs, fmt.Sprintf("%s lastId: %s != %s", gp, fmtID(g.LastId), fmtID(gb.LastId)))
		}
		if g.EntriesRead != gb.EntriesRead {
			diffs = append(diffs, fmt.Sprintf("%s entriesRead: %d != %d", gp, g.EntriesRead, gb.EntriesRead))
		}
		diffs = append(diffs, comparePEL(gp, g.Pending, gb.Pending)...)
		diffs = append(diffs, compareConsumers(gp, g.Consumers, gb.Consumers)...)
	}
	return diffs
}

func groupNames(gs []*model.StreamGroup) []string {
	names := make([]string, 0, len(gs))
	for _, g := range gs {
		names = append(names, g.Name)
	}
	sort.Strings(names)
	return names
}

func comparePEL(prefix string, a, b []*model.StreamNAck) []string {
	var diffs []string
	if len(a) != len(b) {
		diffs = append(diffs, fmt.Sprintf("%s PEL size: %d != %d", prefix, len(a), len(b)))
	}
	bb := map[string]*model.StreamNAck{}
	for _, n := range b {
		bb[fmtID(n.Id)] = n
	}
	for _, n := range a {
		nb, ok := bb[fmtID(n.Id)]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("%s PEL entry %s missing", prefix, fmtID(n.Id)))
			continue
		}
		if n.DeliveryTime != nb.DeliveryTime {
			diffs = append(diffs, fmt.Sprintf("%s PEL entry %s deliveryTime: %d != %d", prefix, fmtID(n.Id), n.DeliveryTime, nb.DeliveryTime))
		}
		if n.DeliveryCount != nb.DeliveryCount {
			diffs = append(diffs, fmt.Sprintf("%s PEL entry %s deliveryCount: %d != %d", prefix, fmtID(n.Id), n.DeliveryCount, nb.DeliveryCount))
		}
	}
	return diffs
}

func compareConsumers(prefix string, a, b []*model.StreamConsumer) []string {
	var diffs []string
	if len(a) != len(b) {
		diffs = append(diffs, fmt.Sprintf("%s consumer count: %d != %d", prefix, len(a), len(b)))
	}
	bb := map[string]*model.StreamConsumer{}
	for _, c := range b {
		bb[c.Name] = c
	}
	for _, c := range a {
		cb, ok := bb[c.Name]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("%s consumer %q missing", prefix, c.Name))
			continue
		}
		cp := prefix + " consumer " + c.Name
		if c.SeenTime != cb.SeenTime {
			diffs = append(diffs, fmt.Sprintf("%s seenTime: %d != %d", cp, c.SeenTime, cb.SeenTime))
		}
		if c.ActiveTime != cb.ActiveTime {
			diffs = append(diffs, fmt.Sprintf("%s activeTime: %d != %d", cp, c.ActiveTime, cb.ActiveTime))
		}
		ap := pendingIDs(c.Pending)
		bp := pendingIDs(cb.Pending)
		if !stringSliceEqual(ap, bp) {
			diffs = append(diffs, fmt.Sprintf("%s PEL ownership: %v != %v", cp, ap, bp))
		}
	}
	return diffs
}

func pendingIDs(ids []*model.StreamId) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, fmtID(id))
	}
	sort.Strings(out)
	return out
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringMapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

