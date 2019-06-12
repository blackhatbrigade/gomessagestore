package gomessagestore_test

import (
	"io/ioutil"
	"reflect"
	"testing"

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
		Type:       "test type",
		Category:   "test cat",
		NewID:      "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		Data:       packed,
	}
}

func getSampleEvent() *Event {
	packed, err := Pack(dummyData{"a"})
	panicIf(err)
	return &Event{
		NewID:      "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:       "test type",
		CategoryID: "544477d6-453f-4b48-8460-0a6e4d6f98e5",
		Category:   "test cat",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		Data:       packed,
	}
}

func getSampleEvents() []*Event {
	packed, err := Pack(dummyData{"a"})
	panicIf(err)
	return []*Event{
		&Event{
			NewID:      "544477d6-453f-4b48-8460-1a6e4d6f97d5",
			Type:       "Event Type 2",
			CategoryID: "544477d6-453f-4b48-8460-0a6e4d6f98e5",
			Category:   "test cat",
			CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
			OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
			Data:       packed,
		}, &Event{
			NewID:      "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			Type:       "Event Type 1",
			CategoryID: "544477d6-453f-4b48-8460-0a6e4d6f98e5",
			Category:   "test cat",
			CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
			OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
			Data:       packed,
		}}
}

func getSampleEventAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		MessageID:  "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:       "test type",
		Stream:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
		StreamType: "test cat",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		Data:       []byte(`{"Field1":"a"}`),
	}

	return msgEnv
}

func getSampleEventsAsEnvelopes() []*repository.MessageEnvelope {
	return []*repository.MessageEnvelope{
		&repository.MessageEnvelope{
			MessageID:  "544477d6-453f-4b48-8460-1a6e4d6f97d5",
			Type:       "Event Type 2",
			Stream:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
			StreamType: "test cat",
			OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
			CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
			Data:       []byte(`{"Field1":"a"}`),
		}, &repository.MessageEnvelope{
			MessageID:  "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			Type:       "Event Type 1",
			Stream:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
			StreamType: "test cat",
			OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
			CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
			Data:       []byte(`{"Field1":"a"}`),
		}}
}

func getSampleCommandAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		MessageID:  "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Type:       "test type",
		Stream:     "test cat:command",
		StreamType: "test cat",
		OwnerID:    "544477d6-453f-4b48-8460-0a6e4d6f97e5",
		CausedByID: "544477d6-453f-4b48-8460-0a6e4d6f97d7",
		Data:       []byte(`{"Field1":"a"}`),
	}

	return msgEnv
}

func assertMessageMatchesCommand(t *testing.T, msgEnv Message, msg *Command) {
	switch command := msgEnv.(type) {
	case *Command:
		if command.NewID != msg.NewID {
			t.Error("NewID in message does not match")
		}
		if command.Type != msg.Type {
			t.Error("Type in message does not match")
		}
		if command.Category != msg.Category {
			t.Error("Category in message does not match")
		}
		if command.CausedByID != msg.CausedByID {
			t.Error("CausedByID in message does not match")
		}
		if command.OwnerID != msg.OwnerID {
			t.Error("OwnerID in message does not match")
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
		if event.NewID != msg.NewID {
			t.Error("NewID in message does not match")
		}
		if event.Type != msg.Type {
			t.Error("Type in message does not match")
		}
		if event.CategoryID != msg.CategoryID {
			t.Error("CategoryID in message does not match")
		}
		if event.Category != msg.Category {
			t.Error("Category in message does not match")
		}
		if event.CausedByID != msg.CausedByID {
			t.Error("CausedByID in message does not match")
		}
		if event.OwnerID != msg.OwnerID {
			t.Error("OwnerID in message does not match")
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
	return "Event Type 1"
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
	return "Event Type 2"
}
