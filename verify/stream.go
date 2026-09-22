package main

import (
	"fmt"
	"sort"

	"github.com/hdt3213/rdb/model"
)

// compareStream checks every stream identity-sensitive element: the radix node
// layout (entries), message ids, field payloads, deleted flags, length/last-id
// metadata, and full group/consumer/PEL ownership. Every identity mismatch is a
// semantic (blocking) loss; only the stream encoding version byte is benign.
func compareStream(a, b *model.StreamObject) (semantic, encoding []Loss) {
	if a.Version != b.Version {
		encoding = append(encoding, Loss{
			Code:   "stream-version-change",
			Detail: fmt.Sprintf("stream encoding v%d -> v%d", a.Version, b.Version),
		})
	}
	if a.Length != b.Length {
		semantic = append(semantic, Loss{Code: "stream-length-mismatch", Detail: fmt.Sprintf("length %d -> %d", a.Length, b.Length)})
	}
	if !streamIDEqual(a.LastId, b.LastId) {
		semantic = append(semantic, Loss{Code: "stream-lastid-mismatch", Detail: fmt.Sprintf("lastId %s -> %s", formatID(a.LastId), formatID(b.LastId))})
	}
	if !streamIDEqual(a.FirstId, b.FirstId) {
		semantic = append(semantic, Loss{Code: "stream-firstid-mismatch", Detail: fmt.Sprintf("firstId %s -> %s", formatID(a.FirstId), formatID(b.FirstId))})
	}
	if !streamIDEqual(a.MaxDeletedId, b.MaxDeletedId) {
		semantic = append(semantic, Loss{Code: "stream-maxdeleted-mismatch", Detail: fmt.Sprintf("maxDeletedId %s -> %s", formatID(a.MaxDeletedId), formatID(b.MaxDeletedId))})
	}
	if a.AddedEntriesCount != b.AddedEntriesCount {
		semantic = append(semantic, Loss{Code: "stream-addedcount-mismatch", Detail: fmt.Sprintf("addedEntriesCount %d -> %d", a.AddedEntriesCount, b.AddedEntriesCount)})
	}

	// flatten messages keyed by exact id; field maps normalize per-message order
	msA := flattenMessages(a)
	msB := flattenMessages(b)
	if len(msA) != len(msB) {
		semantic = append(semantic, Loss{Code: "stream-message-count-mismatch", Detail: fmt.Sprintf("messages %d -> %d", len(msA), len(msB))})
	}
	for id, ma := range msA {
		mb, ok := msB[id]
		if !ok {
			semantic = append(semantic, Loss{Code: "stream-message-missing", Detail: "message id " + id + " missing"})
			continue
		}
		if ma.Deleted != mb.Deleted {
			semantic = append(semantic, Loss{Code: "stream-deleted-mismatch", Detail: "message " + id + " deleted flag differs"})
		}
		if !fieldMapEqual(ma.Fields, mb.Fields) {
			semantic = append(semantic, Loss{Code: "stream-fields-mismatch", Detail: "message " + id + " fields differ"})
		}
	}
	for id := range msB {
		if _, ok := msA[id]; !ok {
			semantic = append(semantic, Loss{Code: "stream-message-added", Detail: "message id " + id + " synthesized"})
		}
	}

	// groups: name ownership, lastId, entries-read, PEL ownership, consumers
	ga := groupMap(a.Groups)
	gb := groupMap(b.Groups)
	if len(ga) != len(gb) {
		semantic = append(semantic, Loss{Code: "stream-group-count-mismatch", Detail: fmt.Sprintf("groups %d -> %d", len(ga), len(gb))})
	}
	for name, x := range ga {
		y, ok := gb[name]
		if !ok {
			semantic = append(semantic, Loss{Code: "stream-group-missing", Detail: "group " + name})
			continue
		}
		if !streamIDEqual(x.LastId, y.LastId) {
			semantic = append(semantic, Loss{Code: "stream-group-lastid-mismatch", Detail: fmt.Sprintf("group %s lastId %s -> %s", name, formatID(x.LastId), formatID(y.LastId))})
		}
		if x.EntriesRead != y.EntriesRead {
			semantic = append(semantic, Loss{Code: "stream-group-entriesread-mismatch", Detail: fmt.Sprintf("group %s entriesRead %d -> %d", name, x.EntriesRead, y.EntriesRead)})
		}
		if !pelEqual(x.Pending, y.Pending) {
			semantic = append(semantic, Loss{Code: "stream-pel-mismatch", Detail: fmt.Sprintf("group %s PEL entries differ", name)})
		}
		compareConsumers(name, x.Consumers, y.Consumers, &semantic)
	}
	for name := range gb {
		if _, ok := ga[name]; !ok {
			semantic = append(semantic, Loss{Code: "stream-group-added", Detail: "group " + name + " synthesized"})
		}
	}
	return semantic, encoding
}

func flattenMessages(s *model.StreamObject) map[string]*model.StreamMessage {
	m := make(map[string]*model.StreamMessage)
	for _, entry := range s.Entries {
		for _, msg := range entry.Msgs {
			m[formatID(msg.Id)] = msg
		}
	}
	return m
}

func fieldMapEqual(a, b map[string]string) bool {
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

func groupMap(groups []*model.StreamGroup) map[string]*model.StreamGroup {
	m := make(map[string]*model.StreamGroup, len(groups))
	for _, g := range groups {
		m[g.Name] = g
	}
	return m
}

func pelEqual(a, b []*model.StreamNAck) bool {
	if len(a) != len(b) {
		return false
	}
	xa := make(map[string]*model.StreamNAck, len(a))
	xb := make(map[string]*model.StreamNAck, len(b))
	for _, p := range a {
		xa[formatID(p.Id)] = p
	}
	for _, p := range b {
		xb[formatID(p.Id)] = p
	}
	for id, pa := range xa {
		pb, ok := xb[id]
		if !ok || pa.DeliveryTime != pb.DeliveryTime || pa.DeliveryCount != pb.DeliveryCount {
			return false
		}
	}
	return true
}

func compareConsumers(group string, a, b []*model.StreamConsumer, out *[]Loss) {
	ma := make(map[string]*model.StreamConsumer, len(a))
	mb := make(map[string]*model.StreamConsumer, len(b))
	for _, c := range a {
		ma[c.Name] = c
	}
	for _, c := range b {
		mb[c.Name] = c
	}
	if len(ma) != len(mb) {
		*out = append(*out, Loss{Code: "stream-consumer-count-mismatch", Detail: fmt.Sprintf("group %s consumers %d -> %d", group, len(ma), len(mb))})
	}
	for name, x := range ma {
		y, ok := mb[name]
		if !ok {
			*out = append(*out, Loss{Code: "stream-consumer-missing", Detail: fmt.Sprintf("group %s consumer %s", group, name)})
			continue
		}
		if x.SeenTime != y.SeenTime || x.ActiveTime != y.ActiveTime {
			*out = append(*out, Loss{Code: "stream-consumer-time-mismatch", Detail: fmt.Sprintf("consumer %s seen/active time differs", name)})
		}
		if !idSetEqual(x.Pending, y.Pending) {
			*out = append(*out, Loss{Code: "stream-consumer-pel-mismatch", Detail: fmt.Sprintf("consumer %s owned pending ids differ", name)})
		}
	}
	for name := range mb {
		if _, ok := ma[name]; !ok {
			*out = append(*out, Loss{Code: "stream-consumer-added", Detail: fmt.Sprintf("group %s consumer %s synthesized", group, name)})
		}
	}
}

func idSetEqual(a, b []*model.StreamId) bool {
	if len(a) != len(b) {
		return false
	}
	x := make([]string, len(a))
	y := make([]string, len(b))
	for i, id := range a {
		x[i] = formatID(id)
	}
	for i, id := range b {
		y[i] = formatID(id)
	}
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func streamIDEqual(a, b *model.StreamId) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Ms == b.Ms && a.Sequence == b.Sequence
}

func formatID(id *model.StreamId) string {
	if id == nil {
		return "nil"
	}
	return fmt.Sprintf("%d-%d", id.Ms, id.Sequence)
}
