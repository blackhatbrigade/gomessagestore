package postgres_test

import (
	"io/ioutil"
	"time"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/sirupsen/logrus"
)

// disable logging during tests
func init() {
	logrus.SetOutput(ioutil.Discard)
}

// prevent weirdness with pointers
func copyAndAppend(i []*repository.MessageEnvelope, vals ...*repository.MessageEnvelope) []*repository.MessageEnvelope {
	j := make([]*repository.MessageEnvelope, len(i), len(i)+len(vals))
	copy(j, i)
	return append(j, vals...)
}

var (
	uuid1 = uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001"))
	uuid2 = uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))
	uuid3 = uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000003"))
	uuid4 = uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000004"))
	uuid5 = uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000005"))
)

var mockMessages = []*repository.MessageEnvelope{{
	GlobalPosition: 3,
	ID:             uuid1,
	MessageType:    "some_type",
	StreamName:     "some_type-12345",
	StreamCategory: "other_type",
	Version:        0,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Time:           time.Unix(1545539339, 0),
}, {
	GlobalPosition: 4,
	ID:             uuid2,
	MessageType:    "some_type",
	StreamName:     "some_type-23456",
	StreamCategory: "other_type",
	Version:        0,
	Data:           []byte("{a:false, b:123}"),
	Time:           time.Unix(1546773906, 0),
}, {
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
	StreamCategory: "other_type",
	Version:        1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Time:           time.Unix(1545549339, 0),
}}
