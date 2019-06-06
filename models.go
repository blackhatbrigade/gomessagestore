package gomessagestore

import (
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/blackhatbrigade/gomessagestore/repository"
)

//Message Defines an interface that can consume Commands or Events.
type Message interface {
	ToEnvelope() (*MessageEnvelope, error)
}

//Command the model for writing a command to the Message Store
type Command struct {
	NewID      string
	Type       string
	Category   string
	CausedByID string
	OwnerID    string
	Data       map[string]interface{}
}

//ToEnvelope Allows for exporting to a MessageEnvelope type.
func (cmd *Command) ToEnvelope() (*MessageEnvelope, error) {
	if cmd.Type == "" {
		return nil, ErrMissingMessageType
	}

	if cmd.Category == "" {
		return nil, ErrMissingMessageCategory
	}

	if strings.Contains(cmd.Category, "-") {
		return nil, ErrInvalidMessageCategory
	}

	if cmd.NewID == "" {
		return nil, ErrMessageNoID
	}

	if cmd.Data == nil {
		return nil, ErrMissingMessageData
	}

	data, err := json.Marshal(cmd.Data)
	if err != nil {
		return nil, ErrUnserializableData
	}

	msgEnv := &MessageEnvelope{
		MessageID:  cmd.NewID,
		Type:       cmd.Type,
		Stream:     fmt.Sprintf("%s:command", cmd.Category),
		StreamType: cmd.Category,
		OwnerID:    cmd.OwnerID,
		CausedByID: cmd.CausedByID,
		Data:       data,
	}
	return msgEnv, nil
}

//Event the model for writing an event to the Message Store
type Event struct {
	NewID      string
	Type       string
	CategoryID string
	Category   string
	CausedByID string
	OwnerID    string
	Data       map[string]interface{}
}

//ToEnvelope Allows for exporting to a MessageEnvelope type.
func (event *Event) ToEnvelope() (*MessageEnvelope, error) {
	if event.Type == "" {
		return nil, ErrMissingMessageType
	}

	if strings.Contains(event.Category, "-") {
		return nil, ErrInvalidMessageCategory
	}

	if event.Data == nil {
		return nil, ErrMissingMessageData
	}

	if event.NewID == "" {
		return nil, ErrMessageNoID
	}

	if event.CategoryID == "" {
		return nil, ErrMissingMessageCategoryID
	}

	if event.Category == "" {
		return nil, ErrMissingMessageCategory
	}

	data, err := json.Marshal(event.Data)

	if err != nil {
		return nil, ErrUnserializableData
	}

	msgEnv := &MessageEnvelope{
		MessageID:  event.NewID,
		Type:       event.Type,
		Stream:     fmt.Sprintf("%s-%s", event.Category, event.CategoryID),
		StreamType: event.Category,
		OwnerID:    event.OwnerID,
		CausedByID: event.CausedByID,
		Data:       data,
	}

	return msgEnv, nil
}
