package repository_test

import (
	"time"
)

// prevent weirdness with pointers
func copyAndAppend(i []*MessageEnvelope, vals ...*MessageEnvelope) []*MessageEnvelope {
	j := make([]*MessageEnvelope, len(i), len(i)+len(vals))
	copy(j, i)
	return append(j, vals...)
}

var mockMessages = []*MessageEnvelope{{
	GlobalPosition: 3,
	MessageID:      "abc-123",
	Type:           "some_type",
	Stream:         "some_type-12345",
	StreamType:     "some_type",
	CorrelationID:  "qwerty-asdfg",
	CausedByID:     "",
	UserID:         "hjkl",
	Position:       0,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Timestamp:      time.Unix(1545539339, 0),
}, {
	GlobalPosition: 4,
	MessageID:      "def-246",
	Type:           "some_type",
	Stream:         "some_type-23456",
	StreamType:     "some_type",
	CorrelationID:  "asdfg-qwerty",
	CausedByID:     "",
	UserID:         "ghjk",
	Position:       0,
	Data:           []byte("{a:false, b:123}"),
	Timestamp:      time.Unix(1546773906, 0),
}, {
	GlobalPosition: 5,
	MessageID:      "dag-2346",
	Type:           "some_other_type",
	Stream:         "some_other_type-23456",
	StreamType:     "some_other_type",
	CorrelationID:  "asdfg-xzcvb",
	CausedByID:     "22993-asdff",
	UserID:         "ghjk",
	Position:       0,
	Data:           []byte("{d:\"a\"}"),
	Timestamp:      time.Unix(1546773907, 0),
}, {
	GlobalPosition: 6,
	MessageID:      "daf-3346",
	Type:           "some_other_other_type",
	Stream:         "some_other_other_type-23456",
	StreamType:     "some_other_other_type",
	CorrelationID:  "asdfg-asdfl-xzcvb",
	CausedByID:     "22993-asdfl-asdff",
	UserID:         "ghjk",
	Position:       0,
	Data:           []byte("{d:\"a\"}"),
	Timestamp:      time.Unix(1546773907, 0),
}, {
	GlobalPosition: 7,
	MessageID:      "abc-456",
	Type:           "some_type",
	Stream:         "some_type-12345",
	StreamType:     "some_type",
	CorrelationID:  "qwerty-asdfg-some-real-guid",
	CausedByID:     "",
	UserID:         "hjklm",
	Position:       1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Timestamp:      time.Unix(1545549339, 0),
}}
