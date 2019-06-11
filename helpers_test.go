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
