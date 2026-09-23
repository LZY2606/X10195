package main

import (
	"fmt"
	"sort"

	"github.com/hdt3213/rdb/model"
)

// compareStream compares two streams structurally, including the encoding
// version, entry ids, group/consumer/PEL ownership and delivery metadata.
func compareStream(fixture, desc string, src, dst *model.StreamObject) []finding {
	var findings []finding
	add := func(code, format string, args ...interface{}) {
		findings = append(findings, finding{
			Fixture: fixture, Stage: stageCompare, Kind: kindError, Check: checkStream, Code: code,
			Message: fmt.Sprintf(format, args...),
		})
	}
	if src.Version != dst.Version {
		add("stream-version-mismatch",
			"%s stream encoding version changed from %d to %d", desc, src.Version, dst.Version)
	}
	if src.Length != dst.Length {
		add("stream-mismatch", "%s stream length changed from %d to %d", desc, src.Length, dst.Length)
	}
	if !streamIdEqual(src.LastId, dst.LastId) {
		add("stream-mismatch", "%s stream last id changed from %s to %s", desc,
			formatStreamId(src.LastId), formatStreamId(dst.LastId))
	}
	if src.Version >= 2 {
		if !streamIdEqual(src.FirstId, dst.FirstId) {
			add("stream-mismatch", "%s stream first id changed from %s to %s", desc,
				formatStreamId(src.FirstId), formatStreamId(dst.FirstId))
		}
		if !streamIdEqual(src.MaxDeletedId, dst.MaxDeletedId) {
			add("stream-mismatch", "%s stream max-deleted id changed from %s to %s", desc,
				formatStreamId(src.MaxDeletedId), formatStreamId(dst.MaxDeletedId))
		}
		if src.AddedEntriesCount != dst.AddedEntriesCount {
			add("stream-mismatch", "%s stream added-entries count changed from %d to %d",
				desc, src.AddedEntriesCount, dst.AddedEntriesCount)
		}
	}
	findings = append(findings, compareStreamEntries(fixture, desc, src.Entries, dst.Entries)...)
	findings = append(findings, compareStreamGroups(fixture, desc, src.Groups, dst.Groups)...)
	return findings
}

func compareStreamEntries(fixture, desc string, src, dst []*model.StreamEntry) []finding {
	var findings []finding
	add := func(format string, args ...interface{}) {
		findings = append(findings, finding{
			Fixture: fixture, Stage: stageCompare, Kind: kindError, Check: checkStream, Code: "stream-entry-mismatch",
			Message: fmt.Sprintf(format, args...),
		})
	}
	if len(src) != len(dst) {
		add("%s stream entry node count changed from %d to %d", desc, len(src), len(dst))
		return findings
	}
	srcSorted := sortEntries(src)
	dstSorted := sortEntries(dst)
	for i := range srcSorted {
		se, de := srcSorted[i], dstSorted[i]
		entryDesc := fmt.Sprintf("%s entry %s", desc, formatStreamId(se.FirstMsgId))
		if !streamIdEqual(se.FirstMsgId, de.FirstMsgId) {
			add("%s entry node id changed from %s to %s", desc,
				formatStreamId(se.FirstMsgId), formatStreamId(de.FirstMsgId))
			continue
		}
		if !stringSliceEqual(se.Fields, de.Fields) {
			add("%s master fields changed from %q to %q", entryDesc, se.Fields, de.Fields)
		}
		srcMsgs := sortMessages(se.Msgs)
		dstMsgs := sortMessages(de.Msgs)
		if len(srcMsgs) != len(dstMsgs) {
			add("%s message count changed from %d to %d", entryDesc, len(srcMsgs), len(dstMsgs))
			continue
		}
		for j := range srcMsgs {
			sm, dm := srcMsgs[j], dstMsgs[j]
			msgDesc := fmt.Sprintf("%s message %s", entryDesc, formatStreamId(sm.Id))
			if !streamIdEqual(sm.Id, dm.Id) {
				add("%s message id changed from %s to %s", entryDesc,
					formatStreamId(sm.Id), formatStreamId(dm.Id))
				continue
			}
			if sm.Deleted != dm.Deleted {
				add("%s deleted flag changed from %v to %v", msgDesc, sm.Deleted, dm.Deleted)
			}
			if !stringMapEqual(sm.Fields, dm.Fields) {
				add("%s fields changed from %q to %q", msgDesc, sm.Fields, dm.Fields)
			}
		}
	}
	return findings
}

func compareStreamGroups(fixture, desc string, src, dst []*model.StreamGroup) []finding {
	var findings []finding
	add := func(format string, args ...interface{}) {
		findings = append(findings, finding{
			Fixture: fixture, Stage: stageCompare, Kind: kindError, Check: checkStream, Code: "stream-group-mismatch",
			Message: fmt.Sprintf(format, args...),
		})
	}
	srcGroups := sortGroups(src)
	dstGroups := sortGroups(dst)
	if len(srcGroups) != len(dstGroups) {
		add("%s consumer group count changed from %d to %d", desc, len(srcGroups), len(dstGroups))
		return findings
	}
	for i := range srcGroups {
		sg, dg := srcGroups[i], dstGroups[i]
		groupDesc := fmt.Sprintf("%s group %q", desc, sg.Name)
		if sg.Name != dg.Name {
			add("%s group name changed from %q to %q", desc, sg.Name, dg.Name)
			continue
		}
		if !streamIdEqual(sg.LastId, dg.LastId) {
			add("%s last-delivered id changed from %s to %s", groupDesc,
				formatStreamId(sg.LastId), formatStreamId(dg.LastId))
		}
		if sg.EntriesRead != dg.EntriesRead {
			add("%s entries-read changed from %d to %d", groupDesc, sg.EntriesRead, dg.EntriesRead)
		}
		// group-level PEL
		srcPending := sortNAcks(sg.Pending)
		dstPending := sortNAcks(dg.Pending)
		if len(srcPending) != len(dstPending) {
			add("%s PEL size changed from %d to %d", groupDesc, len(srcPending), len(dstPending))
		} else {
			for j := range srcPending {
				sp, dp := srcPending[j], dstPending[j]
				pelDesc := fmt.Sprintf("%s PEL entry %s", groupDesc, formatStreamId(sp.Id))
				if !streamIdEqual(sp.Id, dp.Id) {
					add("%s PEL id changed from %s to %s", groupDesc,
						formatStreamId(sp.Id), formatStreamId(dp.Id))
					continue
				}
				if sp.DeliveryTime != dp.DeliveryTime {
					add("%s delivery time changed from %d to %d", pelDesc, sp.DeliveryTime, dp.DeliveryTime)
				}
				if sp.DeliveryCount != dp.DeliveryCount {
					add("%s delivery count changed from %d to %d", pelDesc, sp.DeliveryCount, dp.DeliveryCount)
				}
			}
		}
		// consumers and their PEL ownership
		srcConsumers := sortConsumers(sg.Consumers)
		dstConsumers := sortConsumers(dg.Consumers)
		if len(srcConsumers) != len(dstConsumers) {
			add("%s consumer count changed from %d to %d", groupDesc, len(srcConsumers), len(dstConsumers))
			continue
		}
		for j := range srcConsumers {
			sc, dc := srcConsumers[j], dstConsumers[j]
			consumerDesc := fmt.Sprintf("%s consumer %q", groupDesc, sc.Name)
			if sc.Name != dc.Name {
				add("%s consumer name changed from %q to %q", groupDesc, sc.Name, dc.Name)
				continue
			}
			if sc.SeenTime != dc.SeenTime {
				add("%s seen time changed from %d to %d", consumerDesc, sc.SeenTime, dc.SeenTime)
			}
			if sc.ActiveTime != dc.ActiveTime {
				add("%s active time changed from %d to %d", consumerDesc, sc.ActiveTime, dc.ActiveTime)
			}
			srcIds := sortStreamIds(sc.Pending)
			dstIds := sortStreamIds(dc.Pending)
			if len(srcIds) != len(dstIds) {
				add("%s owned PEL size changed from %d to %d", consumerDesc, len(srcIds), len(dstIds))
				continue
			}
			for k := range srcIds {
				if !streamIdEqual(srcIds[k], dstIds[k]) {
					add("%s owned PEL id %s changed to %s", consumerDesc,
						formatStreamId(srcIds[k]), formatStreamId(dstIds[k]))
				}
			}
		}
	}
	return findings
}

func streamIdEqual(a, b *model.StreamId) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Ms == b.Ms && a.Sequence == b.Sequence
}

func formatStreamId(id *model.StreamId) string {
	if id == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d-%d", id.Ms, id.Sequence)
}

func streamIdLess(a, b *model.StreamId) bool {
	if a.Ms != b.Ms {
		return a.Ms < b.Ms
	}
	return a.Sequence < b.Sequence
}

func sortEntries(entries []*model.StreamEntry) []*model.StreamEntry {
	sorted := make([]*model.StreamEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return streamIdLess(sorted[i].FirstMsgId, sorted[j].FirstMsgId)
	})
	return sorted
}

func sortMessages(msgs []*model.StreamMessage) []*model.StreamMessage {
	sorted := make([]*model.StreamMessage, len(msgs))
	copy(sorted, msgs)
	sort.Slice(sorted, func(i, j int) bool {
		return streamIdLess(sorted[i].Id, sorted[j].Id)
	})
	return sorted
}

func sortGroups(groups []*model.StreamGroup) []*model.StreamGroup {
	sorted := make([]*model.StreamGroup, len(groups))
	copy(sorted, groups)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	return sorted
}

func sortNAcks(nacks []*model.StreamNAck) []*model.StreamNAck {
	sorted := make([]*model.StreamNAck, len(nacks))
	copy(sorted, nacks)
	sort.Slice(sorted, func(i, j int) bool {
		return streamIdLess(sorted[i].Id, sorted[j].Id)
	})
	return sorted
}

func sortConsumers(consumers []*model.StreamConsumer) []*model.StreamConsumer {
	sorted := make([]*model.StreamConsumer, len(consumers))
	copy(sorted, consumers)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	return sorted
}

func sortStreamIds(ids []*model.StreamId) []*model.StreamId {
	sorted := make([]*model.StreamId, len(ids))
	copy(sorted, ids)
	sort.Slice(sorted, func(i, j int) bool {
		return streamIdLess(sorted[i], sorted[j])
	})
	return sorted
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
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
