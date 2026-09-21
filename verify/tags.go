package verify

import (
	"sort"
	"time"

	"github.com/hdt3213/rdb/model"
)

// tagForFixture derives coverage tags from the decoded objects. The gate
// requires an independent, observable result for each scenario called out by
// the project: listpack encodings, stream v1/v2/v3, HFE, empty collections,
// already-expired keys and unknown opcodes (the last via negative fixtures).
func tagForFixture(objs []model.RedisObject, now time.Time) []string {
	tags := map[string]struct{}{}
	keyCount := 0
	hasExpired := false
	for _, o := range objs {
		if isMetaObject(o) {
			continue
		}
		keyCount++
		if e := o.GetExpiration(); e != nil && e.UnixNano()/1e6 <= now.UnixNano()/1e6 {
			hasExpired = true
		}
		switch q := o.(type) {
		case *model.StreamObject:
			switch q.Version {
			case 2:
				tags["stream-v2"] = struct{}{}
			case 3:
				tags["stream-v3"] = struct{}{}
			default:
				tags["stream-v1"] = struct{}{}
			}
			if len(q.Groups) > 0 {
				tags["stream-groups"] = struct{}{}
			}
			for _, g := range q.Groups {
				if len(g.Pending) > 0 {
					tags["stream-pel"] = struct{}{}
				}
				for _, c := range g.Consumers {
					if len(c.Pending) > 0 {
						tags["stream-consumer-pel"] = struct{}{}
					}
				}
			}
		case *model.HashObject:
			if len(q.FieldExpirations) > 0 {
				tags["hfe"] = struct{}{}
			}
		}
		enc := o.GetEncoding()
		switch enc {
		case model.ListPackEncoding, model.QuickList2Encoding, model.ListPackExEncoding:
			tags["listpack"] = struct{}{}
		case model.IntSetEncoding:
			tags["intset"] = struct{}{}
		case model.ZipMapEncoding, model.ZipListEncoding, model.QuickListEncoding:
			tags["legacy-compact-encoding"] = struct{}{}
		}
		if isEmptyContainer(o) {
			tags["empty-collection"] = struct{}{}
		}
	}
	if keyCount == 0 {
		tags["empty-database"] = struct{}{}
	}
	if hasExpired {
		tags["expired-key"] = struct{}{}
	}
	out := make([]string, 0, len(tags))
	for t := range tags {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func isEmptyContainer(o model.RedisObject) bool {
	switch q := o.(type) {
	case *model.ListObject:
		return len(q.Values) == 0
	case *model.SetObject:
		return len(q.Members) == 0
	case *model.HashObject:
		return len(q.Hash) == 0
	case *model.ZSetObject:
		return len(q.Entries) == 0
	case *model.StreamObject:
		return len(q.Entries) == 0 && q.Length == 0
	}
	return false
}
