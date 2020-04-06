package postgres_test

import (
	"time"

	"github.com/blackhatbrigade/gomessagestore/repository"
)

var mockMessagesWithNoMetaData = []*repository.MessageEnvelope{{
	GlobalPosition: 5,
	ID:             uuid3,
	MessageType:    "some_other_type",
	StreamName:     "some_other_type-23456",
	StreamCategory: "some_other_type",
	Version:        0,
	Data:           []byte("{d:\"a\"}"),
	Time:           time.Unix(1546773907, 0),
}, {
	GlobalPosition: 6,
	ID:             uuid4,
	MessageType:    "some_other_other_type",
	StreamName:     "some_other_other_type-23456",
	StreamCategory: "some_other_other_type",
	Version:        0,
	Data:           []byte("{d:\"a\"}"),
	Time:           time.Unix(1546773907, 0),
}, {
	GlobalPosition: 7,
	ID:             uuid5,
	MessageType:    "some_type",
	StreamName:     "some_type-12345",
	StreamCategory: "some_type",
	Version:        1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Time:           time.Unix(1545549339, 0),
}}

var mockMessageNoID = &repository.MessageEnvelope{
	GlobalPosition: 7,
	MessageType:    "some_type",
	StreamName:     "some_type-12345",
	StreamCategory: "some_type",
	Version:        1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Time:           time.Unix(1545549339, 0),
}

var mockMessageNoStream = &repository.MessageEnvelope{
	GlobalPosition: 7,
	ID:             uuid5,
	MessageType:    "some_type",
	StreamCategory: "some_type",
	Version:        1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Time:           time.Unix(1545549339, 0),
}
