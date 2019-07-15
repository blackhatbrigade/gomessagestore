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
	"github.com/blackhatbrigade/gomessagestore/uuid"
	"github.com/sirupsen/logrus"
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

var (
	uuid1 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000001"))
	uuid2 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000002"))
	uuid3 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000003"))
	uuid4 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000004"))
	uuid5 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000005"))
	uuid6 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000006"))
	uuid7 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000007"))
	uuid8 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000008"))
	uuid9 = uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000009"))
)

type dummyData struct {
	SomeField string // more than 1 field here breaks idempotency of tests because of json marshalling from a map[string]interface{} type
}

// disable logging during tests
func init() {
	logrus.SetOutput(ioutil.Discard)
}

func getSampleCommand() *Command {
	packed := map[string]interface{}{
		"SomeField": "a",
	}
	packedMeta := map[string]interface{}{
		"SomeField": "b",
	}
	return &Command{
		MessageType:    "test type",
		StreamCategory: "test cat",
		MessageVersion: 10,
		GlobalPosition: 8,
		ID:             uuid1,
		Data:           packed,
		Time:           time.Unix(1, 0),
		Metadata:       packedMeta,
	}
}

func getSampleEvent() *Event {
	packed := map[string]interface{}{
		"SomeField": "a",
	}
	packedMeta := map[string]interface{}{
		"SomeField": "b",
	}
	return &Event{
		ID:             uuid2,
		MessageType:    "test type",
		EntityID:       uuid8,
		MessageVersion: 9,
		GlobalPosition: 7,
		StreamCategory: "test cat",
		Data:           packed,
		Metadata:       packedMeta,
		Time:           time.Unix(1, 0),
	}
}

func getSampleOtherMessage() *otherMessage {
	packed := map[string]interface{}{
		"someField": "a", // don't test that these get marshalled correctly
	}
	packedMeta := map[string]interface{}{
		"someField": "b",
	}
	return &otherMessage{
		ID:             uuid3,
		MessageType:    "test type",
		EntityID:       uuid9,
		MessageVersion: 9,
		GlobalPosition: 7,
		StreamCategory: "test cat",
		Data:           packed,
		Metadata:       packedMeta,
		Time:           time.Unix(1, 0),
	}
}

func getSampleCommands() []*Command {
	packed1 := map[string]interface{}{
		"SomeField": "a",
	}
	packed2 := map[string]interface{}{
		"SomeField": "c",
	}
	packedMeta1 := map[string]interface{}{
		"SomeField": "b",
	}
	packedMeta2 := map[string]interface{}{
		"SomeField": "d",
	}
	return []*Command{
		&Command{
			ID:             uuid4,
			MessageType:    "Command MessageType 2",
			StreamCategory: "test cat",
			MessageVersion: 1,
			GlobalPosition: 1011,
			Data:           packed1,
			Metadata:       packedMeta1,
			Time:           time.Unix(1, 1),
		}, &Command{
			ID:             uuid6,
			MessageType:    "Command MessageType 1",
			StreamCategory: "test cat",
			MessageVersion: 2,
			GlobalPosition: 1012,
			Data:           packed2,
			Metadata:       packedMeta2,
			Time:           time.Unix(1, 2),
		}}
}

// for when we want to test the MsgEnvelopsToMessages rather than ToEnvelope or Pack
func getSampleCommandsLowerCaseValues() []*Command {
	packed1 := map[string]interface{}{
		"someField": "a",
	}
	packed2 := map[string]interface{}{
		"someField": "c",
	}
	packedMeta1 := map[string]interface{}{
		"someField": "b",
	}
	packedMeta2 := map[string]interface{}{
		"someField": "d",
	}
	return []*Command{
		&Command{
			ID:             uuid4,
			MessageType:    "Command MessageType 2",
			StreamCategory: "test cat",
			MessageVersion: 1,
			GlobalPosition: 1011,
			Data:           packed1,
			Metadata:       packedMeta1,
			Time:           time.Unix(1, 1),
		}, &Command{
			ID:             uuid6,
			MessageType:    "Command MessageType 1",
			StreamCategory: "test cat",
			MessageVersion: 2,
			GlobalPosition: 1012,
			Data:           packed2,
			Metadata:       packedMeta2,
			Time:           time.Unix(1, 2),
		}}
}

func getSampleEvents() []*Event {
	packed1 := map[string]interface{}{
		"SomeField": "a",
	}
	packed2 := map[string]interface{}{
		"SomeField": "c",
	}
	packedMeta1 := map[string]interface{}{
		"SomeField": "b",
	}
	packedMeta2 := map[string]interface{}{
		"SomeField": "d",
	}
	return []*Event{
		&Event{
			ID:             uuid5,
			MessageType:    "Event MessageType 2",
			EntityID:       uuid8,
			StreamCategory: "test cat",
			MessageVersion: 4,
			GlobalPosition: 345,
			Data:           packed1,
			Metadata:       packedMeta1,
			Time:           time.Unix(1, 3),
		}, &Event{
			ID:             uuid7,
			MessageType:    "Event MessageType 1",
			EntityID:       uuid8,
			MessageVersion: 8,
			GlobalPosition: 349,
			StreamCategory: "test cat",
			Data:           packed2,
			Metadata:       packedMeta2,
			Time:           time.Unix(1, 4),
		}}
}

// for when we want to test the MsgEnvelopsToMessages rather than ToEnvelope or Pack
func getSampleEventsLowerCaseValues() []*Event {
	packed1 := map[string]interface{}{
		"someField": "a",
	}
	packed2 := map[string]interface{}{
		"someField": "c",
	}
	packedMeta1 := map[string]interface{}{
		"someField": "b",
	}
	packedMeta2 := map[string]interface{}{
		"someField": "d",
	}
	return []*Event{
		&Event{
			ID:             uuid5,
			MessageType:    "Event MessageType 2",
			EntityID:       uuid8,
			StreamCategory: "test cat",
			MessageVersion: 4,
			GlobalPosition: 345,
			Data:           packed1,
			Metadata:       packedMeta1,
			Time:           time.Unix(1, 3),
		}, &Event{
			ID:             uuid7,
			MessageType:    "Event MessageType 1",
			EntityID:       uuid8,
			MessageVersion: 8,
			GlobalPosition: 349,
			StreamCategory: "test cat",
			Data:           packed2,
			Metadata:       packedMeta2,
			Time:           time.Unix(1, 4),
		}}
}

func getLotsOfSampleEvents(amount, startingAt int) []*Event {
	packed := map[string]interface{}{
		"SomeField": "a",
	}
	packedMeta := map[string]interface{}{
		"SomeField": "b",
	}
	events := make([]*Event, amount)
	for index, _ := range events {
		events[index] = &Event{
			ID:             uuid.Must(uuid.Parse(fmt.Sprintf("10000000-0000-0000-0000-%012d", startingAt+index))),
			MessageType:    fmt.Sprintf("Event MessageType %d", (startingAt+index)%2+1), // be a 1 or a 2
			EntityID:       uuid8,
			StreamCategory: "test cat",
			MessageVersion: int64(4 + startingAt + index),
			GlobalPosition: int64(500 + startingAt + index),
			Data:           packed,
			Metadata:       packedMeta,
			Time:           time.Unix(1, 3),
		}
	}

	return events
}

func getSampleEventAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		ID:             uuid2,
		Version:        9,
		GlobalPosition: 7,
		MessageType:    "test type",
		StreamName:     "test cat-" + uuid8.String(),
		StreamCategory: "test cat",
		Data:           []byte(`{"someField":"a"}`),
		Metadata:       []byte(`{"someField":"b"}`),
		Time:           time.Unix(1, 0),
	}

	return msgEnv
}

func getSampleOtherMessageAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		ID:             uuid3,
		Version:        9,
		GlobalPosition: 7,
		MessageType:    "test type",
		StreamName:     "test cat-" + uuid9.String(),
		StreamCategory: "test cat",
		Data:           []byte(`{"someField":"a"}`),
		Metadata:       []byte(`{"someField":"b"}`),
		Time:           time.Unix(1, 0),
	}

	return msgEnv
}

func getLotsOfSampleEventsAsEnvelopes(amount, startingAt int) []*repository.MessageEnvelope {
	events := make([]*repository.MessageEnvelope, amount)
	for index, _ := range events {
		events[index] = &repository.MessageEnvelope{
			ID:             uuid.Must(uuid.Parse(fmt.Sprintf("10000000-0000-0000-0000-%012d", startingAt+index))),
			MessageType:    fmt.Sprintf("Event MessageType %d", (startingAt+index)%2+1), // be a 1 or a 2
			StreamName:     "test cat-" + uuid8.String(),
			StreamCategory: "test cat",
			Version:        int64(4 + startingAt + index),
			GlobalPosition: int64(500 + startingAt + index),
			Data:           []byte(`{"someField":"a"}`),
			Metadata:       []byte(`{"someField":"b"}`),
			Time:           time.Unix(1, 3),
		}
	}

	return events
}

func getSampleEventsAsEnvelopes() []*repository.MessageEnvelope {
	return []*repository.MessageEnvelope{
		&repository.MessageEnvelope{
			ID:             uuid5,
			MessageType:    "Event MessageType 2",
			StreamName:     "test cat-" + uuid8.String(),
			StreamCategory: "test cat",
			Version:        4,
			GlobalPosition: 345,
			Data:           []byte(`{"someField":"a"}`),
			Metadata:       []byte(`{"someField":"b"}`),
			Time:           time.Unix(1, 3),
		}, &repository.MessageEnvelope{
			ID:             uuid7,
			MessageType:    "Event MessageType 1",
			StreamName:     "test cat-" + uuid8.String(),
			Version:        8,
			GlobalPosition: 349,
			StreamCategory: "test cat",
			Data:           []byte(`{"someField":"c"}`),
			Metadata:       []byte(`{"someField":"d"}`),
			Time:           time.Unix(1, 4),
		}}
}

func getSampleCommandAsEnvelope() *repository.MessageEnvelope {
	msgEnv := &repository.MessageEnvelope{
		ID:             uuid1,
		MessageType:    "test type",
		Version:        10,
		GlobalPosition: 8,
		StreamName:     "test cat:command",
		StreamCategory: "test cat",
		Data:           []byte(`{"someField":"a"}`),
		Metadata:       []byte(`{"someField":"b"}`),
		Time:           time.Unix(1, 0),
	}

	return msgEnv
}

func getSampleCommandsAsEnvelopes() []*repository.MessageEnvelope {
	return []*repository.MessageEnvelope{
		&repository.MessageEnvelope{
			ID:             uuid4,
			MessageType:    "Command MessageType 2",
			StreamName:     "test cat:command",
			StreamCategory: "test cat",
			Version:        1,
			GlobalPosition: 1011,
			Data:           []byte(`{"someField":"a"}`),
			Metadata:       []byte(`{"someField":"b"}`),
			Time:           time.Unix(1, 1),
		}, &repository.MessageEnvelope{
			ID:             uuid6,
			MessageType:    "Command MessageType 1",
			StreamName:     "test cat:command",
			Version:        2,
			GlobalPosition: 1012,
			StreamCategory: "test cat",
			Data:           []byte(`{"someField":"c"}`),
			Metadata:       []byte(`{"someField":"d"}`),
			Time:           time.Unix(1, 2),
		}}
}

func getSampleSnakeCaseCommandsAsEnvelopes() []*repository.MessageEnvelope {
	return []*repository.MessageEnvelope{
		&repository.MessageEnvelope{
			ID:             uuid4,
			MessageType:    "Command MessageType 2",
			StreamName:     "test cat:command",
			StreamCategory: "test cat",
			Version:        1,
			GlobalPosition: 1011,
			Data:           []byte(`{"some_field":"a"}`),
			Metadata:       []byte(`{"some_field":"b"}`),
			Time:           time.Unix(1, 1),
		}, &repository.MessageEnvelope{
			ID:             uuid6,
			MessageType:    "Command MessageType 1",
			StreamName:     "test cat:command",
			Version:        2,
			GlobalPosition: 1012,
			StreamCategory: "test cat",
			Data:           []byte(`{"some_field":"c"}`),
			Metadata:       []byte(`{"some_field":"d"}`),
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
			t.Errorf("ID in message does not match\nHave: %s\nWant: %s\n", msg.ID, other.ID)
		}
		if other.MessageType != msg.MessageType {
			t.Errorf("MessageType in message does not match\nHave: %s\nWant: %s\n", msg.MessageType, other.MessageType)
		}
		if other.EntityID != msg.EntityID {
			t.Errorf("EntityID in message does not match\nHave: %s\nWant: %s\n", msg.EntityID, other.EntityID)
		}
		if other.StreamCategory != msg.StreamCategory {
			t.Errorf("StreamCategory in message does not match\nHave: %s\nWant: %s\n", msg.StreamCategory, other.StreamCategory)
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
	MockReducer1Called    bool
	MockReducer2Called    bool
	MockReducer1CallCount int
	MockReducer2CallCount int
}

type mockReducer1 struct {
	PreviousState   interface{}
	ReceivedMessage Message
}

func (red *mockReducer1) Reduce(msg Message, previousState interface{}) interface{} {
	switch state := previousState.(type) {
	case mockDataStructure:
		state.MockReducer1Called = true
		state.MockReducer1CallCount++
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
		state.MockReducer2CallCount++
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
	ID             uuid.UUID
	EntityID       uuid.UUID
	StreamCategory string
	MessageType    string
	MessageVersion int64
	GlobalPosition int64
	Data           map[string]interface{}
	Metadata       map[string]interface{}
	Time           time.Time
}

func (other *otherMessage) Type() string {
	return other.MessageType
}

func (other *otherMessage) Version() int64 {
	return other.MessageVersion
}

func (other *otherMessage) Position() int64 {
	return other.GlobalPosition
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

	if other.ID == NilUUID {
		return nil, ErrMessageNoID
	}

	if other.EntityID == NilUUID {
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
		Version:        other.MessageVersion,
		GlobalPosition: other.GlobalPosition,
	}

	return msgEnv, nil
}

func convertEnvelopeToOtherMessage(messageEnvelope *repository.MessageEnvelope) (Message, error) {

	data := make(map[string]interface{})
	if err := json.Unmarshal(messageEnvelope.Data, &data); err != nil {
		logrus.WithError(err).Error("Can't unmarshal JSON from message envelope data")
	}
	metadata := make(map[string]interface{})
	if err := json.Unmarshal(messageEnvelope.Metadata, &metadata); err != nil {
		logrus.WithError(err).Error("Can't unmarshal JSON from message envelope metadata")
	}

	category := ""
	var id uuid.UUID
	cats := strings.SplitN(messageEnvelope.StreamName, "-", 2)
	if len(cats) > 0 {
		category = cats[0]
		if len(cats) == 2 {
			var err error
			id, err = uuid.Parse(cats[1])
			if err != nil {
				return nil, err
			}
		}
	}
	other := &otherMessage{
		ID:             messageEnvelope.ID,
		MessageVersion: messageEnvelope.Version,
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
