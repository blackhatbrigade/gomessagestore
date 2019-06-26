package gomessagestore

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
)

//So users don't have to import gomessagestore/uuid
var NilUUID = uuid.Nil

//NewID creates a new UUID
func NewID() uuid.UUID {
	return uuid.NewRandom()
}

//Command the model for writing a command to the Message Store
type Command struct {
	ID             uuid.UUID
	StreamCategory string
	MessageType    string
	MessageVersion int64
	GlobalPosition int64
	Data           map[string]interface{}
	Metadata       map[string]interface{}
	Time           time.Time
}

//Type returns the type of this message (business action taking place)
func (cmd *Command) Type() string {
	return cmd.MessageType
}

//Version gets the command's Version field
func (cmd *Command) Version() int64 {
	return cmd.MessageVersion
}

//Position gets the command's Position field
func (cmd *Command) Position() int64 {
	return cmd.GlobalPosition
}

//ToEnvelope Allows for exporting to a MessageEnvelope type.
func (cmd *Command) ToEnvelope() (*repository.MessageEnvelope, error) {
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

//Event the model for writing an event to the Message Store
type Event struct {
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

//Type returns the type of this message (business action taking place)
func (event *Event) Type() string {
	return event.MessageType
}

//Version gets the event's Version field
func (event *Event) Version() int64 {
	return event.MessageVersion
}

//Position gets the events's Position field
func (event *Event) Position() int64 {
	return event.GlobalPosition
}

//ToEnvelope Allows for exporting to a MessageEnvelope type.
func (event *Event) ToEnvelope() (*repository.MessageEnvelope, error) {
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
