package gomessagestore_test

import (
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	. "github.com/blackhatbrigade/gomessagestore"
	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/sirupsen/logrus"
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

type dummyData struct {
	Field1 string // more than 1 field here breaks idempotency of tests because of json marshalling from a map[string]interface{} type
}

// disable logging during tests
func init() {
	logrus.SetOutput(ioutil.Discard)
}

func getSampleCommand() *Command {
	packed, err := Pack(dummyData{"a"})
	panicIf(err)
	return &Command{
		MessageType:    "test type",
		StreamCategory: "test cat",
		Position:       10,
		GlobalPosition: 10,
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Data:           packed,
		Time:           time.Unix(1, 0),
		Metadata:       packed,
	}
}

func getSampleEvent() *Event {
	packed, err := Pack(dummyData{"a"})
	panicIf(err)
	return &Event{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		MessageType:    "test type",
		EntityID:       "544477d6-453f-4b48-8460-0a6e4d6f98e5",
		Position:       9,
		GlobalPosition: 9,
		StreamCategory: "test cat",
		Data:           packed,
		Metadata:       packed,
		Time:           time.Unix(1, 0),
	}
}

func getSampleCommands() []*Command {
	packed, err := Pack(dummyData{"a"})
	panicIf(err)
	return []*Command{
		&Command{
			ID:             "544477d6-453f-4b48-8460-1a6e4d6f97d5",
			MessageType:    "Command MessageType 2",
			StreamCategory: "test cat",
			Position:       1,
			GlobalPosition: 1,
			Data:           packed,
			Metadata:       packed,
			Time:           time.Unix(1, 0),
		}, &Command{
			ID:             "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			MessageType:    "Command MessageType 1",
			StreamCategory: "test cat",
			Position:       2,
			GlobalPosition: 2,
			Data:           packed,
			Metadata:       packed,
			Time:           time.Unix(1, 0),
		}}
}

func getSampleEvents() []*Event {
	packed, err := Pack(dummyData{"a"})
	panicIf(err)
	return []*Event{
		&Event{
			ID:             "544477d6-453f-4b48-8460-1a6e4d6f97d5",
			MessageType:    "Event MessageType 2",
			EntityID:       "544477d6-453f-4b48-8460-0a6e4d6f98e5",
			StreamCategory: "test cat",
			Position:       4,
			GlobalPosition: 4,
			Data:           packed,
			Metadata:       packed,
			Time:           time.Unix(1, 0),
		}, &Event{
			ID:             "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			MessageType:    "Event MessageType 1",
			EntityID:       "544477d6-453f-4b48-8460-0a6e4d6f98e5",
			Position:       3,
			GlobalPosition: 3,
			StreamCategory: "test cat",
			Data:           packed,
			Metadata:       packed,
			Time:           time.Unix(1, 0),
		}}
}

func getSampleEventAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Position:       9,
		GlobalPosition: 9,
		MessageType:    "test type",
		StreamName:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
		StreamCategory: "test cat",
		Data:           []byte(`{"Field1":"a"}`),
		Metadata:       []byte(`{"Field1":"a"}`),
		Time:           time.Unix(1, 0),
	}

	return msgEnv
}

func getSampleEventsAsEnvelopes() []*repository.MessageEnvelope {
	return []*repository.MessageEnvelope{
		&repository.MessageEnvelope{
			ID:             "544477d6-453f-4b48-8460-1a6e4d6f97d5",
			MessageType:    "Event MessageType 2",
			StreamName:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
			StreamCategory: "test cat",
			Position:       4,
			GlobalPosition: 4,
			Data:           []byte(`{"Field1":"a"}`),
			Metadata:       []byte(`{"Field1":"a"}`),
			Time:           time.Unix(1, 0),
		}, &repository.MessageEnvelope{
			ID:             "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			MessageType:    "Event MessageType 1",
			StreamName:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
			Position:       3,
			GlobalPosition: 3,
			StreamCategory: "test cat",
			Data:           []byte(`{"Field1":"a"}`),
			Metadata:       []byte(`{"Field1":"a"}`),
			Time:           time.Unix(1, 0),
		}}
}

func getSampleCommandAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		MessageType:    "test type",
		Position:       10,
		GlobalPosition: 10,
		StreamName:     "test cat:command",
		StreamCategory: "test cat",
		Data:           []byte(`{"Field1":"a"}`),
		Metadata:       []byte(`{"Field1":"a"}`),
		Time:           time.Unix(1, 0),
	}

	return msgEnv
}

func getSampleCommandsAsEnvelopes() []*repository.MessageEnvelope {
	return []*repository.MessageEnvelope{
		&repository.MessageEnvelope{
			ID:             "544477d6-453f-4b48-8460-1a6e4d6f97d5",
			MessageType:    "Command MessageType 2",
			StreamName:     "test cat:command",
			StreamCategory: "test cat",
			Position:       1,
			GlobalPosition: 1,
			Data:           []byte(`{"Field1":"a"}`),
			Metadata:       []byte(`{"Field1":"a"}`),
			Time:           time.Unix(1, 0),
		}, &repository.MessageEnvelope{
			ID:             "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			MessageType:    "Command MessageType 1",
			StreamName:     "test cat:command",
			Position:       2,
			GlobalPosition: 2,
			StreamCategory: "test cat",
			Data:           []byte(`{"Field1":"a"}`),
			Metadata:       []byte(`{"Field1":"a"}`),
			Time:           time.Unix(1, 0),
		}}
}

func assertMessageMatchesCommand(t *testing.T, msgEnv Message, msg *Command) {
	switch command := msgEnv.(type) {
	case *Command:
		if command.ID != msg.ID {
			t.Error("ID in message does not match")
		}
		if command.MessageType != msg.MessageType {
			t.Error("MessageType in message does not match")
		}
		if command.StreamCategory != msg.StreamCategory {
			t.Error("StreamCategory in message does not match")
		}
		data := new(dummyData)
		err := Unpack(command.Data, data)
		if err != nil {
			t.Error("Couldn't unpack data from message")
		}
		if !reflect.DeepEqual(&dummyData{"a"}, data) {
			t.Error("Messages are not correct")
		}
	default:
		t.Error("Unknown type of Message")
	}
}

func assertMessageMatchesEvent(t *testing.T, msgEnv Message, msg *Event) {
	switch event := msgEnv.(type) {
	case *Event:
		if event.ID != msg.ID {
			t.Error("ID in message does not match")
		}
		if event.MessageType != msg.MessageType {
			t.Error("MessageType in message does not match")
		}
		if event.EntityID != msg.EntityID {
			t.Error("EntityID in message does not match")
		}
		if event.StreamCategory != msg.StreamCategory {
			t.Error("StreamCategory in message does not match")
		}
		data := new(dummyData)
		err := Unpack(event.Data, data)
		if err != nil {
			t.Error("Couldn't unpack data from message")
		}
		if !reflect.DeepEqual(&dummyData{"a"}, data) {
			t.Error("Messages are not correct")
		}
	default:
		t.Error("Unknown type of Message")
	}
}

type mockDataStructure struct {
	MockReducer1Called bool
	MockReducer2Called bool
}

type mockReducer1 struct {
	PreviousState   interface{}
	ReceivedMessage Message
}

func (red *mockReducer1) Reduce(msg Message, previousState interface{}) interface{} {
	switch state := previousState.(type) {
	case mockDataStructure:
		state.MockReducer1Called = true
		return state
	}
	return nil
}

func (red *mockReducer1) Type() string {
	return "Event MessageType 1"
}

type mockReducer2 struct {
	PreviousState   interface{}
	ReceivedMessage Message
}

func (red *mockReducer2) Reduce(msg Message, previousState interface{}) interface{} {
	switch state := previousState.(type) {
	case mockDataStructure:
		state.MockReducer2Called = true
		return state
	}
	return nil
}

func (red *mockReducer2) Type() string {
	return "Event MessageType 2"
}

func commandsToMessageSlice(commands []*Command) []Message {
	newMsgs := make([]Message, len(commands))
	for i, command := range commands {
		newMsgs[i] = command
	}

	return newMsgs
}
func eventsToMessageSlice(events []*Event) []Message {
	newMsgs := make([]Message, len(events))
	for i, event := range events {
		newMsgs[i] = event
	}

	return newMsgs
}
