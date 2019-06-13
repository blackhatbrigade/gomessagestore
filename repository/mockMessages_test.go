package repository_test

import (
	"io/ioutil"
	"time"

	. "github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

// disable logging during tests
func init() {
	logrus.SetOutput(ioutil.Discard)
}

// prevent weirdness with pointers
func copyAndAppend(i []*MessageEnvelope, vals ...*MessageEnvelope) []*MessageEnvelope {
	j := make([]*MessageEnvelope, len(i), len(i)+len(vals))
	copy(j, i)
	return append(j, vals...)
}

var mockMessages = []*MessageEnvelope{{
	GlobalPosition: 3,
	ID:             "abc-123",
	MessageType:    "some_type",
	StreamName:     "some_type-12345",
	StreamCategory: "other_type",
	Version:        0,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Time:           time.Unix(1545539339, 0),
}, {
	GlobalPosition: 4,
	ID:             "def-246",
	MessageType:    "some_type",
	StreamName:     "some_type-23456",
	StreamCategory: "other_type",
	Version:        0,
	Data:           []byte("{a:false, b:123}"),
	Time:           time.Unix(1546773906, 0),
}, {
	GlobalPosition: 5,
	ID:             "dag-2346",
	MessageType:    "some_other_type",
	StreamName:     "some_other_type-23456",
	StreamCategory: "some_other_type",
	Version:        0,
	Data:           []byte("{d:\"a\"}"),
	Time:           time.Unix(1546773907, 0),
}, {
	GlobalPosition: 6,
	ID:             "daf-3346",
	MessageType:    "some_other_other_type",
	StreamName:     "some_other_other_type-23456",
	StreamCategory: "some_other_other_type",
	Version:        0,
	Data:           []byte("{d:\"a\"}"),
	Time:           time.Unix(1546773907, 0),
}, {
	GlobalPosition: 7,
	ID:             "abc-456",
	MessageType:    "some_type",
	StreamName:     "some_type-12345",
	StreamCategory: "other_type",
	Version:        1,
	Data:           []byte("{a:{b:1}, c:\"123\"}"),
	Time:           time.Unix(1545549339, 0),
}}
