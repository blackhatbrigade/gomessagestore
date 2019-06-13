package gomessagestore_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
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
	packedMeta, err := Pack(dummyData{"b"})
	panicIf(err)
	return &Command{
		MessageType:    "test type",
		StreamCategory: "test cat",
		Version:        10,
		GlobalPosition: 10,
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Data:           packed,
		Time:           time.Unix(1, 0),
		Metadata:       packedMeta,
	}
}

func getSampleEvent() *Event {
	packed, err := Pack(dummyData{"a"})
	packedMeta, err := Pack(dummyData{"b"})
	panicIf(err)
	return &Event{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		MessageType:    "test type",
		EntityID:       "544477d6-453f-4b48-8460-0a6e4d6f98e5",
		Version:        9,
		GlobalPosition: 9,
		StreamCategory: "test cat",
		Data:           packed,
		Metadata:       packedMeta,
		Time:           time.Unix(1, 0),
	}
}

func getSampleOtherMessage() *otherMessage {
	packed, err := Pack(dummyData{"a"})
	packedMeta, err := Pack(dummyData{"b"})
	panicIf(err)
	return &otherMessage{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		MessageType:    "test type",
		EntityID:       "544477d6-453f-4b48-8460-0a6e4d6f98e5",
		Version:        9,
		GlobalPosition: 9,
		StreamCategory: "test cat",
		Data:           packed,
		Metadata:       packedMeta,
		Time:           time.Unix(1, 0),
	}
}

func getSampleCommands() []*Command {
	packed1, err := Pack(dummyData{"a"})
	packed2, err := Pack(dummyData{"c"})
	packedMeta1, err := Pack(dummyData{"b"})
	packedMeta2, err := Pack(dummyData{"d"})
	panicIf(err)
	return []*Command{
		&Command{
			ID:             "544477d6-453f-4b48-8460-1a6e4d6f97d5",
			MessageType:    "Command MessageType 2",
			StreamCategory: "test cat",
			Version:        1,
			GlobalPosition: 1,
			Data:           packed1,
			Metadata:       packedMeta1,
			Time:           time.Unix(1, 1),
		}, &Command{
			ID:             "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			MessageType:    "Command MessageType 1",
			StreamCategory: "test cat",
			Version:        2,
			GlobalPosition: 2,
			Data:           packed2,
			Metadata:       packedMeta2,
			Time:           time.Unix(1, 2),
		}}
}

func getSampleEvents() []*Event {
	packed1, err := Pack(dummyData{"a"})
	packed2, err := Pack(dummyData{"c"})
	packedMeta1, err := Pack(dummyData{"b"})
	packedMeta2, err := Pack(dummyData{"d"})
	panicIf(err)
	return []*Event{
		&Event{
			ID:             "544477d6-453f-4b48-8460-1a6e4d6f97d5",
			MessageType:    "Event MessageType 2",
			EntityID:       "544477d6-453f-4b48-8460-0a6e4d6f98e5",
			StreamCategory: "test cat",
			Version:        4,
			GlobalPosition: 4,
			Data:           packed1,
			Metadata:       packedMeta1,
			Time:           time.Unix(1, 3),
		}, &Event{
			ID:             "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			MessageType:    "Event MessageType 1",
			EntityID:       "544477d6-453f-4b48-8460-0a6e4d6f98e5",
			Version:        3,
			GlobalPosition: 3,
			StreamCategory: "test cat",
			Data:           packed2,
			Metadata:       packedMeta2,
			Time:           time.Unix(1, 4),
		}}
}

func getSampleEventAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		Version:        9,
		GlobalPosition: 9,
		MessageType:    "test type",
		StreamName:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
		StreamCategory: "test cat",
		Data:           []byte(`{"Field1":"a"}`),
		Metadata:       []byte(`{"Field1":"b"}`),
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
			Version:        4,
			GlobalPosition: 4,
			Data:           []byte(`{"Field1":"a"}`),
			Metadata:       []byte(`{"Field1":"b"}`),
			Time:           time.Unix(1, 3),
		}, &repository.MessageEnvelope{
			ID:             "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			MessageType:    "Event MessageType 1",
			StreamName:     "test cat-544477d6-453f-4b48-8460-0a6e4d6f98e5",
			Version:        3,
			GlobalPosition: 3,
			StreamCategory: "test cat",
			Data:           []byte(`{"Field1":"c"}`),
			Metadata:       []byte(`{"Field1":"d"}`),
			Time:           time.Unix(1, 4),
		}}
}

func getSampleCommandAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		ID:             "544477d6-453f-4b48-8460-0a6e4d6f97d5",
		MessageType:    "test type",
		Version:        10,
		GlobalPosition: 10,
		StreamName:     "test cat:command",
		StreamCategory: "test cat",
		Data:           []byte(`{"Field1":"a"}`),
		Metadata:       []byte(`{"Field1":"b"}`),
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
			Version:        1,
			GlobalPosition: 1,
			Data:           []byte(`{"Field1":"a"}`),
			Metadata:       []byte(`{"Field1":"b"}`),
			Time:           time.Unix(1, 1),
		}, &repository.MessageEnvelope{
			ID:             "544477d6-453f-4b48-8460-3a6e4d6f97d5",
			MessageType:    "Command MessageType 1",
			StreamName:     "test cat:command",
			Version:        2,
			GlobalPosition: 2,
			StreamCategory: "test cat",
			Data:           []byte(`{"Field1":"c"}`),
			Metadata:       []byte(`{"Field1":"d"}`),
			Time:           time.Unix(1, 2),
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

func assertMessageMatchesOtherMessage(t *testing.T, msgEnv Message, msg *otherMessage) {
	switch other := msgEnv.(type) {
	case *otherMessage:
		if other.ID != msg.ID {
			t.Error("ID in message does not match")
		}
		if other.MessageType != msg.MessageType {
			t.Error("MessageType in message does not match")
		}
		if other.EntityID != msg.EntityID {
			t.Error("EntityID in message does not match")
		}
		if other.StreamCategory != msg.StreamCategory {
			t.Error("StreamCategory in message does not match")
		}
		data := new(dummyData)
		err := Unpack(other.Data, data)
		if err != nil {
			t.Error("Couldn't unpack data from message")
		}
		if !reflect.DeepEqual(&dummyData{"a"}, data) {
			t.Error("Messages are not correct")
		}
	default:
		t.Errorf("Unknown type of Message %T", msgEnv)
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

// this is all just the same as Event
type otherMessage struct {
	ID             string //ID
	EntityID       string //EntityID
	StreamCategory string //StreamCategory
	MessageType    string
	Version        int64
	GlobalPosition int64
	Data           map[string]interface{}
	Metadata       map[string]interface{}
	Time           time.Time
}

func (other *otherMessage) ToEnvelope() (*repository.MessageEnvelope, error) {
	if other.MessageType == "" {
		return nil, ErrMissingMessageType
	}

	if strings.Contains(other.StreamCategory, "-") {
		return nil, ErrInvalidMessageCategory
	}

	if other.Data == nil {
		return nil, ErrMissingMessageData
	}

	if other.ID == "" {
		return nil, ErrMessageNoID
	}

	if other.EntityID == "" {
		return nil, ErrMissingMessageCategoryID
	}

	if other.StreamCategory == "" {
		return nil, ErrMissingMessageCategory
	}

	data, err := json.Marshal(other.Data)
	metadata, errm := json.Marshal(other.Metadata)
	if err != nil || errm != nil {
		return nil, ErrUnserializableData
	}

	msgEnv := &repository.MessageEnvelope{
		ID:             other.ID,
		MessageType:    other.MessageType,
		StreamName:     fmt.Sprintf("%s-%s", other.StreamCategory, other.EntityID),
		StreamCategory: other.StreamCategory,
		Data:           data,
		Metadata:       metadata,
		Time:           other.Time,
		Version:        other.Version,
		GlobalPosition: other.GlobalPosition,
	}

	return msgEnv, nil
}

func convertEnvelopeToOtherMessage(messageEnvelope *repository.MessageEnvelope) (Message, error) {

	fmt.Print("I've been called")
	data := make(map[string]interface{})
	if err := json.Unmarshal(messageEnvelope.Data, &data); err != nil {
		logrus.WithError(err).Error("Can't unmarshal JSON from message envelope data")
	}
	metadata := make(map[string]interface{})
	if err := json.Unmarshal(messageEnvelope.Metadata, &metadata); err != nil {
		logrus.WithError(err).Error("Can't unmarshal JSON from message envelope metadata")
	}

	category, id := "", ""
	cats := strings.SplitN(messageEnvelope.StreamName, "-", 2)
	if len(cats) > 0 {
		category = cats[0]
		if len(cats) == 2 {
			id = cats[1]
		}
	}
	other := &otherMessage{
		ID:             messageEnvelope.ID,
		Version:        messageEnvelope.Version,
		GlobalPosition: messageEnvelope.GlobalPosition,
		MessageType:    messageEnvelope.MessageType,
		StreamCategory: category,
		EntityID:       id,
		Data:           data,
		Metadata:       metadata,
		Time:           messageEnvelope.Time,
	}

	return other, nil
}
