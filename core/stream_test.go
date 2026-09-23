package core

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/hdt3213/rdb/model"
)

func TestWriteStreamObject(t *testing.T) {
	// Create a minimal test stream object
	stream := &model.StreamObject{
		BaseObject: &model.BaseObject{
			Key: "astream",
		},
		Version: 1, // Use version 1 for simplicity
		Length:  0, // Empty stream
		LastId: &model.StreamId{
			Ms:       0,
			Sequence: 0,
		},
		Entries: []*model.StreamEntry{}, // Empty entries
		Groups:  []*model.StreamGroup{}, // Empty groups
	}

	// Encode the stream object
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)

	err := encoder.WriteHeader()
	if err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	err = encoder.WriteDBHeader(0, 1, 0)
	if err != nil {
		t.Fatalf("Failed to write DB header: %v", err)
	}

	err = encoder.WriteStreamObject("astream", stream)
	if err != nil {
		t.Fatalf("Failed to write stream object: %v", err)
	}

	err = encoder.WriteEnd()
	if err != nil {
		t.Fatalf("Failed to write end: %v", err)
	}

	// Decode the stream object
	decodeStreamObject(t, &buf, stream)
}

func TestWriteStreamObjectWithEntries(t *testing.T) {
	stream := &model.StreamObject{
		BaseObject: &model.BaseObject{
			Key: "astream",
		},
		Version: 1,
		Length:  1,
		LastId: &model.StreamId{
			Ms:       1640995200000,
			Sequence: 0,
		},
		Entries: []*model.StreamEntry{
			{
				FirstMsgId: &model.StreamId{
					Ms:       1640995200000,
					Sequence: 0,
				},
				Fields: []string{"field1"},
				Msgs: []*model.StreamMessage{
					{
						Id: &model.StreamId{
							Ms:       1640995200000,
							Sequence: 0,
						},
						Fields: map[string]string{
							"field1": "value1",
						},
						Deleted: false,
					},
				},
			},
		},
		Groups: []*model.StreamGroup{},
	}

	// Test that encoding doesn't crash
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)

	err := encoder.WriteHeader()
	if err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	err = encoder.WriteDBHeader(0, 1, 0)
	if err != nil {
		t.Fatalf("Failed to write DB header: %v", err)
	}

	err = encoder.WriteStreamObject("astream", stream)
	if err != nil {
		t.Fatalf("Failed to write stream object: %v", err)
	}

	err = encoder.WriteEnd()
	if err != nil {
		t.Fatalf("Failed to write end: %v", err)
	}

	// Verify that we can at least write the data without errors
	if buf.Len() == 0 {
		t.Error("No data was written")
	}

	// Decode the stream object
	decodeStreamObject(t, &buf, stream)
}

func TestWriteStreamObjectVersion2(t *testing.T) {
	// Test with version 2 stream structure (similar to stream_listpacks_2.rdb)
	stream := &model.StreamObject{
		BaseObject: &model.BaseObject{
			Key: "astream",
		},
		Version: 2,
		Length:  2,
		LastId: &model.StreamId{
			Ms:       1681085312465,
			Sequence: 0,
		},
		FirstId: &model.StreamId{
			Ms:       1681085300799,
			Sequence: 0,
		},
		MaxDeletedId: &model.StreamId{
			Ms:       0,
			Sequence: 0,
		},
		AddedEntriesCount: 2,
		Entries: []*model.StreamEntry{
			{
				FirstMsgId: &model.StreamId{
					Ms:       1681085300799,
					Sequence: 0,
				},
				Fields: []string{"a", "b", "c"},
				Msgs: []*model.StreamMessage{
					{
						Id: &model.StreamId{
							Ms:       1681085300799,
							Sequence: 0,
						},
						Fields: map[string]string{
							"a": "1",
							"b": "2",
							"c": "3",
						},
						Deleted: false,
					},
					{
						Id: &model.StreamId{
							Ms:       1681085312465,
							Sequence: 0,
						},
						Fields: map[string]string{
							"a": "2",
							"b": "3",
							"c": "4",
						},
						Deleted: false,
					},
				},
			},
		},
		Groups: []*model.StreamGroup{},
	}

	// Encode the stream object
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)

	err := encoder.WriteHeader()
	if err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	err = encoder.WriteDBHeader(0, 1, 0)
	if err != nil {
		t.Fatalf("Failed to write DB header: %v", err)
	}

	err = encoder.WriteStreamObject("astream", stream)
	if err != nil {
		t.Fatalf("Failed to write stream object: %v", err)
	}

	err = encoder.WriteEnd()
	if err != nil {
		t.Fatalf("Failed to write end: %v", err)
	}

	// Verify that we can at least write the data without errors
	if buf.Len() == 0 {
		t.Error("No data was written")
	}

	// Decode the stream object
	decodeStreamObject(t, &buf, stream)
}

func TestWriteStreamObjectVersion3(t *testing.T) {
	// Test with version 3 stream structure
	stream := &model.StreamObject{
		BaseObject: &model.BaseObject{
			Key: "astream",
		},
		Version: 3,
		Length:  2,
		LastId: &model.StreamId{
			Ms:       0,
			Sequence: 0,
		},
		FirstId: &model.StreamId{
			Ms:       0,
			Sequence: 0,
		},
		MaxDeletedId: &model.StreamId{
			Ms:       0,
			Sequence: 0,
		},
		AddedEntriesCount: 0,
		Entries: []*model.StreamEntry{
			{
				FirstMsgId: &model.StreamId{
					Ms:       1681085300799,
					Sequence: 0,
				},
				Fields: []string{"a", "b", "c"},
				Msgs: []*model.StreamMessage{
					{
						Id: &model.StreamId{
							Ms:       1681085300799,
							Sequence: 0,
						},
						Fields: map[string]string{
							"a": "1",
							"b": "2",
							"c": "3",
						},
						Deleted: false,
					},
					{
						Id: &model.StreamId{
							Ms:       1681085312465,
							Sequence: 0,
						},
						Fields: map[string]string{
							"a": "2",
							"b": "3",
							"c": "4",
						},
						Deleted: false,
					},
				},
			},
		},
		Groups: []*model.StreamGroup{
			{
				Name: "test-group",
				LastId: &model.StreamId{
					Ms:       0,
					Sequence: 0,
				},
				EntriesRead: 0,
				Pending:     []*model.StreamNAck{},
				Consumers: []*model.StreamConsumer{
					{
						Name:       "test-consumer",
						SeenTime:   1640995200000,
						ActiveTime: 1640995200000,
						Pending:    []*model.StreamId{},
					},
				},
			},
		},
	}

	// Encode the stream object
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)

	err := encoder.WriteHeader()
	if err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	err = encoder.WriteDBHeader(0, 1, 0)
	if err != nil {
		t.Fatalf("Failed to write DB header: %v", err)
	}

	err = encoder.WriteStreamObject("astream", stream)
	if err != nil {
		t.Fatalf("Failed to write stream object: %v", err)
	}

	err = encoder.WriteEnd()
	if err != nil {
		t.Fatalf("Failed to write end: %v", err)
	}

	// Verify that we can at least write the data without errors
	if buf.Len() == 0 {
		t.Error("No data was written")
	}

	// Decode the stream object
	decodeStreamObject(t, &buf, stream)
}

func decodeStreamObject(t *testing.T, buf *bytes.Buffer, stream *model.StreamObject) {
	// Decode the stream object
	decoder := NewDecoder(buf)

	// Read objects until we find our stream
	var decodedStream *model.StreamObject
	var err = decoder.Parse(func(obj model.RedisObject) bool {
		if streamObj, ok := obj.(*model.StreamObject); ok && streamObj.GetKey() == "astream" {
			decodedStream = streamObj
			return false // stop parsing
		}
		return true // continue parsing
	})
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if decodedStream == nil {
		t.Fatal("Failed to decode stream object")
	}

	// Verify the decoded stream matches the original
	if decodedStream.Version != stream.Version {
		t.Errorf("Version mismatch: expected %d, got %d", stream.Version, decodedStream.Version)
	}

	if decodedStream.Length != stream.Length {
		t.Errorf("Length mismatch: expected %d, got %d", stream.Length, decodedStream.Length)
	}

	if decodedStream.LastId.Ms != stream.LastId.Ms || decodedStream.LastId.Sequence != stream.LastId.Sequence {
		t.Errorf("LastId mismatch: expected %d-%d, got %d-%d",
			stream.LastId.Ms, stream.LastId.Sequence,
			decodedStream.LastId.Ms, decodedStream.LastId.Sequence)
	}

	if len(decodedStream.Entries) != len(stream.Entries) {
		t.Errorf("Entries count mismatch: expected %d, got %d", len(stream.Entries), len(decodedStream.Entries))
	}

	if len(decodedStream.Groups) != len(stream.Groups) {
		t.Errorf("Groups count mismatch: expected %d, got %d", len(stream.Groups), len(decodedStream.Groups))
	}

	for i, entry := range decodedStream.Entries {
		if !reflect.DeepEqual(entry.Fields, stream.Entries[i].Fields) {
			t.Errorf("Fields mismatch at index %d: expected %v, got %v", i, stream.Entries[i].Fields, entry.Fields)
		}
		if !reflect.DeepEqual(entry.Msgs, stream.Entries[i].Msgs) {
			t.Errorf("Msgs mismatch at index %d: expected %v, got %v", i, stream.Entries[i].Msgs, entry.Msgs)
		}
	}
}

func TestWriteStreamObjectLargeIdDiff(t *testing.T) {
	// regression test: listpack int13 holds signed 13 bit values (-4096..4095),
	// larger ms/seq diffs must fall back to wider encodings without corruption
	stream := &model.StreamObject{
		BaseObject: &model.BaseObject{
			Key: "astream",
		},
		Version: 1,
		Length:  2,
		LastId:  &model.StreamId{Ms: 1528466284783, Sequence: 3},
		Entries: []*model.StreamEntry{
			{
				FirstMsgId: &model.StreamId{Ms: 1528466280444, Sequence: 0},
				Fields:     []string{"k"},
				Msgs: []*model.StreamMessage{
					{
						Id:     &model.StreamId{Ms: 1528466280444, Sequence: 0},
						Fields: map[string]string{"k": "v"},
					},
					{
						// ms diff 4339 exceeds the int13 positive range
						Id:     &model.StreamId{Ms: 1528466284783, Sequence: 3},
						Fields: map[string]string{"k": "v2"},
					},
				},
			},
		},
		Groups: []*model.StreamGroup{},
	}

	var buf bytes.Buffer
	encoder := NewEncoder(&buf)
	if err := encoder.WriteHeader(); err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}
	if err := encoder.WriteDBHeader(0, 1, 0); err != nil {
		t.Fatalf("Failed to write db header: %v", err)
	}
	if err := encoder.WriteStreamObject("astream", stream); err != nil {
		t.Fatalf("Failed to write stream: %v", err)
	}
	if err := encoder.WriteEnd(); err != nil {
		t.Fatalf("Failed to write end: %v", err)
	}

	decoder := NewDecoder(&buf)
	var decoded *model.StreamObject
	err := decoder.Parse(func(obj model.RedisObject) bool {
		if s, ok := obj.(*model.StreamObject); ok {
			decoded = s
			return false
		}
		return true
	})
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if decoded == nil || len(decoded.Entries) != 1 || len(decoded.Entries[0].Msgs) != 2 {
		t.Fatalf("decoded stream structure mismatch: %+v", decoded)
	}
	for i, msg := range stream.Entries[0].Msgs {
		got := decoded.Entries[0].Msgs[i].Id
		if got.Ms != msg.Id.Ms || got.Sequence != msg.Id.Sequence {
			t.Errorf("msg %d id mismatch: expected %d-%d, got %d-%d",
				i, msg.Id.Ms, msg.Id.Sequence, got.Ms, got.Sequence)
		}
	}
}
