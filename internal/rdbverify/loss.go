package rdbverify

import "errors"

// Structured expected-loss codes.
//
// A loss is a known, explainable difference between the first and second
// decode that is caused by an encoder representational limit (or by RDB
// dialect/version metadata). Losses must never be a substitute for a proper
// round-trip: every code names exactly what is not preserved.
const (
	// LossRDBVersion means the file dialect/version header changed (the
	// encoder emits a fixed version). Byte-only metadata, never key data.
	LossRDBVersion = "rdb-version"
	// LossAuxField means an AUX field present in the source is absent or
	// different after re-encoding.
	LossAuxField = "aux-field"
	// LossFunctionLibrary means a function library record cannot be emitted
	// by the encoder.
	LossFunctionLibrary = "function-library"
	// LossEvictionMeta means LRU idle / LFU frequency metadata is dropped.
	LossEvictionMeta = "eviction-meta"
	// LossEmptyDB means a database that held no keyed objects cannot be
	// represented by the encoder (which forbids empty DB headers).
	LossEmptyDB = "empty-db"
	// LossStreamGroup means stream consumer groups / PEL ownership data
	// produced by re-encoding differ from the source.
	LossStreamGroup = "stream-group"
	// LossHFE means hash field-level expiration semantics changed.
	LossHFE = "hfe"
)

// ErrUnsupportedEncoding is returned by the re-encoder for objects it has no
// writer for at all (e.g. module types / unknown opcodes).
var ErrUnsupportedEncoding = errors.New("encoder does not support object")

// ExpectedLoss is one explained representational loss.
type ExpectedLoss struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}
