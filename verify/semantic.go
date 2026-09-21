package verify

import (
	"fmt"
	"time"

	"github.com/hdt3213/rdb/model"
)

// CompareObjects checks that two decodings of the same original fixture carry
// identical user-visible semantics.
//
// Order in hashes/sets is normalized (Redis maps and sets are unordered), but
// nothing else is relaxed: DB index, raw key bytes, millisecond expiry
// precision, list order, zset order/scores, stream IDs, group/consumer/PEL
// ownership and the serialized function payload must all match exactly.
//
// Size/encoding are intentionally not compared: they depend on the physical
// encoding the encoder chose and say nothing about semantics.
//
// Each returned SemanticLoss is an expected-class gap (metadata the encoder
// legitimately cannot represent); each SemanticDiff is a hard mismatch.
func CompareObjects(original, reencoded []model.RedisObject, now time.Time) ([]SemanticLoss, []SemanticDiff) {
	var losses []SemanticLoss
	var diffs []SemanticDiff

	addDiff := func(format string, args ...interface{}) {
		diffs = append(diffs, SemanticDiff{Detail: fmt.Sprintf(format, args...)})
	}

	// Split special/global objects from key-bearing objects and index the
	// latter by (db index, raw key). That identity must be preserved exactly.
	auxA, funcsA, keysA := indexObjects(original)
	auxB, funcsB, keysB := indexObjects(reencoded)

	// --- AUX fields (ordered comparison is fine; both sides come from a parse) ---
	auxKeys := map[string]struct{}{}
	for k := range auxA {
		auxKeys[k] = struct{}{}
	}
	for k := range auxB {
		auxKeys[k] = struct{}{}
	}
	for k := range auxKeys {
		va, oka := auxA[k]
		vb, okb := auxB[k]
		if oka && !okb {
			losses = append(losses, SemanticLoss{Reason: LossAuxField, Detail: fmt.Sprintf("aux %q dropped", k)})
			continue
		}
		if !oka && okb {
			addDiff("aux %q appeared only after re-encode (value=%q)", k, vb)
			continue
		}
		if va != vb {
			addDiff("aux %q value mismatch: %q vs %q", k, va, vb)
		}
	}

	// --- Function libraries: payload must survive byte-for-byte ---
	if funcsA != "" || funcsB != "" {
		if funcsA != "" && funcsB == "" {
			losses = append(losses, SemanticLoss{Reason: LossFunctionLibrary, Detail: "function library payload dropped"})
		} else if funcsA != funcsB {
			addDiff("function library payload mismatch: %d bytes vs %d bytes", len(funcsA), len(funcsB))
		}
	}

	// --- Key-bearing objects ---
	all := map[keyID]struct{}{}
	for id := range keysA {
		all[id] = struct{}{}
	}
	for id := range keysB {
		all[id] = struct{}{}
	}
	for id := range all {
		a, oka := keysA[id]
		b, okb := keysB[id]
		if !oka {
			addDiff("db %d key %q exists only after re-encode", id.db, id.key)
			continue
		}
		if !okb {
			addDiff("db %d key %q dropped on re-encode", id.db, id.key)
			continue
		}
		compareKeyObject(id, a, b, now, &losses, &diffs)
	}
	return losses, diffs
}

type keyID struct {
	db  int
	key string
}

func indexObjects(objs []model.RedisObject) (map[string]string, string, map[keyID]model.RedisObject) {
	aux := map[string]string{}
	var funcs string
	keys := map[keyID]model.RedisObject{}
	for _, o := range objs {
		switch q := o.(type) {
		case *model.AuxObject:
			aux[q.Key] = q.Value
		case *model.FunctionsObject:
			funcs = q.FunctionsLua
		case *model.DBSizeObject:
			// advisory hints, not user semantics
		default:
			keys[keyID{db: o.GetDBIndex(), key: o.GetKey()}] = o
		}
	}
	return aux, funcs, keys
}

func compareKeyObject(id keyID, a, b model.RedisObject, now time.Time, losses *[]SemanticLoss, diffs *[]SemanticDiff) {
	prefix := fmt.Sprintf("db %d key %q", id.db, id.key)
	addDiff := func(format string, args ...interface{}) {
		*diffs = append(*diffs, SemanticDiff{Path: id.key, Detail: prefix + ": " + fmt.Sprintf(format, args...)})
	}

	if a.GetType() != b.GetType() {
		addDiff("type mismatch: %s vs %s", a.GetType(), b.GetType())
		return
	}
	// Expiration precision: compare at millisecond granularity exactly.
	// Presence/absence must also match (persistent vs expiring).
	ea, eb := a.GetExpiration(), b.GetExpiration()
	switch {
	case ea == nil && eb != nil:
		addDiff("expiration appeared after re-encode: %s", eb.Format(time.RFC3339Nano))
	case ea != nil && eb == nil:
		addDiff("expiration dropped: %s", ea.Format(time.RFC3339Nano))
	case ea != nil && eb != nil:
		ma := ea.UnixNano() / int64(time.Millisecond)
		mb := eb.UnixNano() / int64(time.Millisecond)
		if ma != mb {
			addDiff("expiration precision loss: %d ms vs %d ms", ma, mb)
		}
	}
	// LRU/LFU eviction metadata is a known, separately-classified loss.
	if ev, ok := a.(model.EvictionInfo); ok {
		evB, _ := b.(model.EvictionInfo)
		if ev.GetIdleTime() >= 0 && (evB == nil || evB.GetIdleTime() != ev.GetIdleTime()) {
			*losses = append(*losses, SemanticLoss{Reason: LossEviction, Detail: prefix + ": LRU idle time not preserved"})
		}
		if ev.GetFreq() >= 0 && (evB == nil || evB.GetFreq() != ev.GetFreq()) {
			*losses = append(*losses, SemanticLoss{Reason: LossEviction, Detail: prefix + ": LFU frequency not preserved"})
		}
	}

	switch x := a.(type) {
	case *model.StringObject:
		y := b.(*model.StringObject)
		if !bytesEqual(x.Value, y.Value) {
			addDiff("string value mismatch: %x vs %x", x.Value, y.Value)
		}
	case *model.ListObject:
		y := b.(*model.ListObject)
		compareByteLists(prefix, x.Values, y.Values, addDiff)
	case *model.SetObject:
		y := b.(*model.SetObject)
		// sets are unordered: compare as multisets of raw bytes
		sa, sb := multiset(x.Members), multiset(y.Members)
		if !sameMultiset(sa, sb) {
			addDiff("set members mismatch: %s vs %s", joinBytes(sortedMembers(sa)), joinBytes(sortedMembers(sb)))
		}
	case *model.HashObject:
		y := b.(*model.HashObject)
		compareHash(prefix, x, y, addDiff)
	case *model.ZSetObject:
		y := b.(*model.ZSetObject)
		compareZSet(prefix, x, y, addDiff)
	case *model.StreamObject:
		y := b.(*model.StreamObject)
		compareStream(prefix, x, y, addDiff)
	default:
		addDiff("unsupported object type for semantic comparison: %T", a)
	}
}
