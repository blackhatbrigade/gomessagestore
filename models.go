package gomessagestore

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blackhatbrigade/gomessagestore/repository"
)

//Command the model for writing a command to the Message Store
type Command struct {
	ID             string
	StreamCategory string
	MessageType    string
	Version        int64
	GlobalPosition int64
	Data           map[string]interface{}
	Metadata       map[string]interface{}
	Time           time.Time
}

//Type returns the type of this message (business action taking place)
func (cmd *Command) Type() string {
	return cmd.MessageType
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

	if cmd.ID == "" {
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
		Version:        cmd.Version,
		GlobalPosition: cmd.GlobalPosition,
	}
	return msgEnv, nil
}

//Event the model for writing an event to the Message Store
type Event struct {
	ID             string
	EntityID       string
	StreamCategory string
	MessageType    string
	Version        int64
	GlobalPosition int64
	Data           map[string]interface{}
	Metadata       map[string]interface{}
	Time           time.Time
}

//Type returns the type of this message (business action taking place)
func (event *Event) Type() string {
	return event.MessageType
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

	if event.ID == "" {
		return nil, ErrMessageNoID
	}

	if event.EntityID == "" {
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
		Version:        event.Version,
		GlobalPosition: event.GlobalPosition,
	}

	return msgEnv, nil
}

type positionMessage struct {
	ID           string
	Position     int64
	SubscriberID string
	Version      int64
}

type positionData struct {
	Position int64 `json:"position"`
}

func (posMsg *positionMessage) Type() string {
	return "PositionCommitted"
}

func (posMsg *positionMessage) ToEnvelope() (*repository.MessageEnvelope, error) {
	messageType := posMsg.Type()

	if messageType == "" {
		return nil, ErrMissingMessageType
	}

	if posMsg.ID == "" {
		return nil, ErrMessageNoID
	}

	//new error
	if posMsg.SubscriberID == "" {
	}

	//another new error
	if posMsg.Version < 0 {
	}

	posData := positionData{posMsg.Position}

	data, err := json.Marshal(posData)
	if err != nil {
		return nil, ErrUnserializableData
	}

	msgEnv := &repository.MessageEnvelope{
		ID:          posMsg.ID,
		MessageType: messageType,
		StreamName:  fmt.Sprintf("%s+position", posMsg.ID),
		Data:        data,
		Version:     posMsg.Version,
	}

	return msgEnv, nil
}
