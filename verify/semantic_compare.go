package verify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hdt3213/rdb/model"
)

func bytesEqual(a, b []byte) bool {
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

func compareByteLists(prefix string, a, b [][]byte, addDiff func(string, ...interface{})) {
	if len(a) != len(b) {
		addDiff("list length mismatch: %d vs %d", len(a), len(b))
		return
	}
	for i := range a {
		if !bytesEqual(a[i], b[i]) {
			addDiff("list element %d mismatch (list order is significant): %q vs %q", i, a[i], b[i])
			return
		}
	}
}

// multiset counts raw-byte member occurrences.
func multiset(members [][]byte) map[string]int {
	out := map[string]int{}
	for _, m := range members {
		out[string(m)]++
	}
	return out
}

func sameMultiset(a, b map[string]int) bool {
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

func sortedMembers(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		for i := 0; i < v; i++ {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func joinBytes(ss []string) string {
	quoted := make([]string, len(ss))
	for i, s := range ss {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(quoted, ",")
}

func compareHash(prefix string, a, b *model.HashObject, addDiff func(string, ...interface{})) {
	if len(a.Hash) != len(b.Hash) {
		addDiff("hash size mismatch: %d vs %d", len(a.Hash), len(b.Hash))
		return
	}
	for field, va := range a.Hash {
		vb, ok := b.Hash[field]
		if !ok {
			addDiff("hash field %q dropped", field)
			continue
		}
		if !bytesEqual(va, vb) {
			addDiff("hash field %q value mismatch: %q vs %q", field, va, vb)
		}
	}
	for field := range b.Hash {
		if _, ok := a.Hash[field]; !ok {
			addDiff("hash field %q appeared after re-encode", field)
		}
	}
	// Field-level expiration (HFE): exact field set and millisecond values.
	if len(a.FieldExpirations) != len(b.FieldExpirations) {
		addDiff("HFE field-expiration count mismatch: %d vs %d", len(a.FieldExpirations), len(b.FieldExpirations))
		return
	}
	for field, ta := range a.FieldExpirations {
		tb, ok := b.FieldExpirations[field]
		if !ok {
			addDiff("HFE expiration for field %q dropped", field)
			continue
		}
		// 0 means "no field expiration" (persistent field); it must be preserved.
		if ta != tb {
			addDiff("HFE expiration precision loss for field %q: %d vs %d", field, ta, tb)
		}
	}
}

func compareZSet(prefix string, a, b *model.ZSetObject, addDiff func(string, ...interface{})) {
	if len(a.Entries) != len(b.Entries) {
		addDiff("zset size mismatch: %d vs %d", len(a.Entries), len(b.Entries))
		return
	}
	// Redis zsets are ordered by score; members carry raw bytes and scores are
	// doubles, so compare in score order but tolerate equal-score reordering.
	ma := zsetMap(a.Entries)
	mb := zsetMap(b.Entries)
	if len(ma) != len(mb) {
		addDiff("zset distinct member count mismatch: %d vs %d", len(ma), len(mb))
		return
	}
	for member, sa := range ma {
		sb, ok := mb[member]
		if !ok {
			addDiff("zset member %q dropped", member)
			continue
		}
		if sa != sb {
			addDiff("zset member %q score mismatch: %v vs %v", member, sa, sb)
		}
	}
}

func zsetMap(entries []*model.ZSetEntry) map[string]float64 {
	out := make(map[string]float64, len(entries))
	for _, e := range entries {
		out[e.Member] = e.Score
	}
	return out
}

func compareStream(prefix string, a, b *model.StreamObject, addDiff func(string, ...interface{})) {
	if a.Length != b.Length {
		addDiff("stream length mismatch: %d vs %d", a.Length, b.Length)
	}
	if !sameID(a.LastId, b.LastId) {
		addDiff("stream lastId mismatch: %s vs %s", fmtID(a.LastId), fmtID(b.LastId))
	}
	if a.Version >= 2 || b.Version >= 2 {
		if !sameID(a.FirstId, b.FirstId) {
			addDiff("stream firstId mismatch: %s vs %s", fmtID(a.FirstId), fmtID(b.FirstId))
		}
		if !sameID(a.MaxDeletedId, b.MaxDeletedId) {
			addDiff("stream maxDeletedId mismatch: %s vs %s", fmtID(a.MaxDeletedId), fmtID(b.MaxDeletedId))
		}
		if a.AddedEntriesCount != b.AddedEntriesCount {
			addDiff("stream addedEntriesCount mismatch: %d vs %d", a.AddedEntriesCount, b.AddedEntriesCount)
		}
	}

	// Flatten listpack nodes into a sequence of messages keyed by exact stream ID.
	// IDs are identity for stream messages; field maps are unordered.
	msgsA := flattenStream(a.Entries)
	msgsB := flattenStream(b.Entries)
	if len(msgsA) != len(msgsB) {
		addDiff("stream message count mismatch: %d vs %d", len(msgsA), len(msgsB))
	}
	for id, fa := range msgsA {
		fb, ok := msgsB[id]
		if !ok {
			addDiff("stream message %s dropped", id)
			continue
		}
		if len(fa) != len(fb) {
			addDiff("stream message %s field count mismatch: %d vs %d", id, len(fa), len(fb))
			continue
		}
		for k, va := range fa {
			if vb, ok := fb[k]; !ok {
				addDiff("stream message %s field %q dropped", id, k)
			} else if va != vb {
				addDiff("stream message %s field %q value mismatch: %q vs %q", id, k, va, vb)
			}
		}
	}
	for id := range msgsB {
		if _, ok := msgsA[id]; !ok {
			addDiff("stream message %s appeared after re-encode", id)
		}
	}
	// Deleted-message markers matter (they occupy ID space).
	deletedA := deletedSet(a.Entries)
	deletedB := deletedSet(b.Entries)
	if !sameStringSet(deletedA, deletedB) {
		addDiff("stream deleted-message set mismatch: %v vs %v", deletedA, deletedB)
	}

	// Consumer groups / consumers / PEL ownership must survive exactly.
	if len(a.Groups) != len(b.Groups) {
		addDiff("stream group count mismatch: %d vs %d", len(a.Groups), len(b.Groups))
		return
	}
	groupsA := groupMap(a.Groups)
	groupsB := groupMap(b.Groups)
	for name, ga := range groupsA {
		gb, ok := groupsB[name]
		if !ok {
			addDiff("stream group %q dropped", name)
			continue
		}
		compareStreamGroup(prefix, name, ga, gb, addDiff)
	}
}

func sameID(a, b *model.StreamId) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Ms == b.Ms && a.Sequence == b.Sequence
}

func fmtID(id *model.StreamId) string {
	if id == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d-%d", id.Ms, id.Sequence)
}

func flattenStream(entries []*model.StreamEntry) map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, node := range entries {
		for _, msg := range node.Msgs {
			out[fmtID(msg.Id)] = msg.Fields
		}
	}
	return out
}

func deletedSet(entries []*model.StreamEntry) map[string]struct{} {
	out := map[string]struct{}{}
	for _, node := range entries {
		for _, msg := range node.Msgs {
			if msg.Deleted {
				out[fmtID(msg.Id)] = struct{}{}
			}
		}
	}
	return out
}

func sameStringSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func groupMap(groups []*model.StreamGroup) map[string]*model.StreamGroup {
	out := make(map[string]*model.StreamGroup, len(groups))
	for _, g := range groups {
		out[g.Name] = g
	}
	return out
}

func compareStreamGroup(prefix, name string, a, b *model.StreamGroup, addDiff func(string, ...interface{})) {
	gp := prefix + fmt.Sprintf(" group %q", name)
	if !sameID(a.LastId, b.LastId) {
		addDiff("%s lastId mismatch: %s vs %s", gp, fmtID(a.LastId), fmtID(b.LastId))
	}
	if a.EntriesRead != b.EntriesRead {
		addDiff("%s entriesRead mismatch: %d vs %d", gp, a.EntriesRead, b.EntriesRead)
	}
	// PEL: each pending message ID, delivery time and delivery count.
	if len(a.Pending) != len(b.Pending) {
		addDiff("%s PEL length mismatch: %d vs %d", gp, len(a.Pending), len(b.Pending))
	} else {
		pa := pelMap(a.Pending)
		pb := pelMap(b.Pending)
		for id, na := range pa {
			nb, ok := pb[id]
			if !ok {
				addDiff("%s PEL entry %s dropped", gp, id)
				continue
			}
			if na.DeliveryTime != nb.DeliveryTime || na.DeliveryCount != nb.DeliveryCount {
				addDiff("%s PEL entry %s ownership mismatch: deliveryTime/deliveryCount %d/%d vs %d/%d",
					gp, id, na.DeliveryTime, na.DeliveryCount, nb.DeliveryTime, nb.DeliveryCount)
			}
		}
		for id := range pb {
			if _, ok := pa[id]; !ok {
				addDiff("%s PEL entry %s appeared after re-encode", gp, id)
			}
		}
	}
	// Consumers with seen time, active time and their owned pending IDs.
	if len(a.Consumers) != len(b.Consumers) {
		addDiff("%s consumer count mismatch: %d vs %d", gp, len(a.Consumers), len(b.Consumers))
		return
	}
	ca := consumerMap(a.Consumers)
	cb := consumerMap(b.Consumers)
	for cname, conA := range ca {
		conB, ok := cb[cname]
		if !ok {
			addDiff("%s consumer %q dropped", gp, cname)
			continue
		}
		if conA.SeenTime != conB.SeenTime {
			addDiff("%s consumer %q seenTime mismatch: %d vs %d", gp, cname, conA.SeenTime, conB.SeenTime)
		}
		if conA.ActiveTime != conB.ActiveTime {
			addDiff("%s consumer %q activeTime mismatch: %d vs %d", gp, cname, conA.ActiveTime, conB.ActiveTime)
		}
		pendingA := idSet(conA.Pending)
		pendingB := idSet(conB.Pending)
		if !sameStringSet(pendingA, pendingB) {
			addDiff("%s consumer %q PEL ownership mismatch: %v vs %v", gp, cname, sortedSet(pendingA), sortedSet(pendingB))
		}
	}
}

func pelMap(nacks []*model.StreamNAck) map[string]*model.StreamNAck {
	out := make(map[string]*model.StreamNAck, len(nacks))
	for _, n := range nacks {
		out[fmtID(n.Id)] = n
	}
	return out
}

func consumerMap(cs []*model.StreamConsumer) map[string]*model.StreamConsumer {
	out := make(map[string]*model.StreamConsumer, len(cs))
	for _, c := range cs {
		out[c.Name] = c
	}
	return out
}

func idSet(ids []*model.StreamId) map[string]struct{} {
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		out[fmtID(id)] = struct{}{}
	}
	return out
}

func sortedSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
