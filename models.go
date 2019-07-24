package gomessagestore

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
)

var NilUUID = uuid.Nil

// NewID creates a new UUID.
func NewID() uuid.UUID {
	return uuid.NewRandom()
}

type Command struct {
	ID             uuid.UUID // ID for the command
	StreamCategory string    // Name of the stream category
	MessageType    string    // Name of the message type
	MessageVersion int64     // version number of the message
	GlobalPosition int64     // global position of the command
	Data           map[string]interface{}
	Metadata       map[string]interface{}
	Time           time.Time
}

//Type returns the type of the command
func (cmd *Command) Type() string {
	return cmd.MessageType
}

//Version returns the version of the command
func (cmd *Command) Version() int64 {
	return cmd.MessageVersion
}

//Position returns the global position of the command
func (cmd *Command) Position() int64 {
	return cmd.GlobalPosition
}

//ToEnvelope converts the command to a Message Envelope that is returned
func (cmd *Command) ToEnvelope() (*repository.MessageEnvelope, error) {
	// check to ensure all needed fields on the command are valid
	if cmd.MessageType == "" {
		return nil, ErrMissingMessageType
	}

	if cmd.StreamCategory == "" {
		return nil, ErrMissingMessageCategory
	}

	if strings.Contains(cmd.StreamCategory, "-") {
		return nil, ErrInvalidMessageCategory
	}

	if cmd.ID == NilUUID {
		return nil, ErrMessageNoID
	}

	if cmd.Data == nil {
		return nil, ErrMissingMessageData
	}

	data, err := json.Marshal(cmd.Data)
	metadata, errm := json.Marshal(cmd.Metadata)
	if err != nil || errm != nil {
		return nil, ErrUnserializableData
	}

	// create a new MessageEnvelope based on the command
	msgEnv := &repository.MessageEnvelope{
		ID:             cmd.ID,
		MessageType:    cmd.MessageType,
		StreamName:     fmt.Sprintf("%s:command", cmd.StreamCategory),
		StreamCategory: cmd.StreamCategory,
		Data:           data,
		Metadata:       metadata,
		Time:           cmd.Time,
		Version:        cmd.MessageVersion,
		GlobalPosition: cmd.GlobalPosition,
	}
	return msgEnv, nil
}

type Event struct {
	ID             uuid.UUID // ID of the event
	EntityID       uuid.UUID // ID of the entity the event is associated with
	StreamCategory string    // the name of the category of the stream
	MessageType    string    // the message type of the event
	MessageVersion int64     // the version number of the message
	GlobalPosition int64     // the global position of the event
	Data           map[string]interface{}
	Metadata       map[string]interface{}
	Time           time.Time
}

//Type returns the type of the event
func (event *Event) Type() string {
	return event.MessageType
}

//Version returns the version of the event
func (event *Event) Version() int64 {
	return event.MessageVersion
}

//Position returns the global position of the event
func (event *Event) Position() int64 {
	return event.GlobalPosition
}

//ToEnvelope converts the event to a MessageEnvelope which is then returned
func (event *Event) ToEnvelope() (*repository.MessageEnvelope, error) {
	// check to ensure that all required fields of the event are valid
	if event.MessageType == "" {
		return nil, ErrMissingMessageType
	}

	if strings.Contains(event.StreamCategory, "-") {
		return nil, ErrInvalidMessageCategory
	}

	if event.Data == nil {
		return nil, ErrMissingMessageData
	}

	if event.ID == NilUUID {
		return nil, ErrMessageNoID
	}

	if event.EntityID == NilUUID {
		return nil, ErrMissingMessageCategoryID
	}

	if event.StreamCategory == "" {
		return nil, ErrMissingMessageCategory
	}

	data, err := json.Marshal(event.Data)
	metadata, errm := json.Marshal(event.Metadata)
	if err != nil || errm != nil {
		return nil, ErrUnserializableData
	}

	// create a new MessageEnvelope based on the event
	msgEnv := &repository.MessageEnvelope{
		ID:             event.ID,
		MessageType:    event.MessageType,
		StreamName:     fmt.Sprintf("%s-%s", event.StreamCategory, event.EntityID),
		StreamCategory: event.StreamCategory,
		Data:           data,
		Metadata:       metadata,
		Time:           event.Time,
		Version:        event.MessageVersion,
		GlobalPosition: event.GlobalPosition,
	}

	return msgEnv, nil
}
