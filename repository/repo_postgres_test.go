package repository

import (
	"encoding/json"
	"time"
)

func metadataJSON(message *MessageEnvelope) []byte {
	metadata := struct {
		CorrelationID string `json:"correlation_id,omitempty" db:"correlation_id"`
		CausedByID    string `json:"caused_by_id,omitempty" db:"caused_by_id"`
		UserID        string `json:"user_id,omitempty" db:"user_id"`
	}{message.CorrelationID, message.CausedByID, message.UserID}
	bytes, _ := json.Marshal(metadata)

	return bytes
}

var mockMessagesWithNoMetaData = []*MessageEnvelope{{
	GlobalPosition: 5,
	MessageID:      "dag-2346",
	Type:           "some_other_type",
	Stream:         "some_other_type-23456",
	StreamType:     "some_other_type",
	CorrelationID:  "",
	CausedByID:     "",
	UserID:         "",
	Position:       0,
	Data:           []byte("{d:\"a\"}"),
	Timestamp:      time.Unix(1546773907, 0),
}, {
	GlobalPosition: 6,
	MessageID:      "daf-3346",
	Type:           "some_other_other_type",
	Stream:         "some_other_other_type-23456",
	StreamType:     "some_other_other_type",
	CorrelationID:  "",
	CausedByID:     "",
	UserID:         "",
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

var mockMessageNoID = &MessageEnvelope{
	GlobalPosition: 7,
	Type:           "some_type",
	Stream:         "some_type-12345",
	StreamType:     "some_type",
	CorrelationID:  "",
	CausedByID:     "",
	UserID:         "",
	Position:       1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Timestamp:      time.Unix(1545549339, 0),
}

var mockMessageNoStream = &MessageEnvelope{
	GlobalPosition: 7,
	MessageID:      "abc-456",
	Type:           "some_type",
	StreamType:     "some_type",
	CorrelationID:  "",
	CausedByID:     "",
	UserID:         "",
	Position:       1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Timestamp:      time.Unix(1545549339, 0),
}
