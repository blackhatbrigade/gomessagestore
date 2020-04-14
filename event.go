package gomessagestore

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
)

// Event implements the Message interface; returned by get function
type Event struct {
	ID             uuid.UUID // ID of the event
	EntityID       uuid.UUID // ID of the entity the event is associated with
	StreamCategory string    // the name of the category of the stream
	MessageType    string    // the message type of the event
	MessageVersion int64     // the version number of the message
	GlobalPosition int64     // the global position of the event
	Data           []byte
	Metadata       []byte
	Time           time.Time
}

func NewEvent(id uuid.UUID, entId uuid.UUID, category string, msgType string, data []byte, metadata []byte) Event {
	evt := Event{
		ID:             id,
		EntityID:       entId,
		StreamCategory: category,
		MessageType:    msgType,
		Data:           data,
		Metadata:       metadata,
	}

	return evt
}

// Type returns the type of the event
func (event Event) Type() string {
	return event.MessageType
}

// Version returns the version of the event
func (event Event) Version() int64 {
	return event.MessageVersion
}

// Position returns the global position of the event
func (event Event) Position() int64 {
	return event.GlobalPosition
}

// ToEnvelope converts the event to a MessageEnvelope which is then returned
func (event Event) ToEnvelope() (*repository.MessageEnvelope, error) {
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

	// create a new MessageEnvelope based on the event
	msgEnv := &repository.MessageEnvelope{
		ID:             event.ID,
		MessageType:    event.MessageType,
		StreamName:     fmt.Sprintf("%s-%s", event.StreamCategory, event.EntityID),
		StreamCategory: event.StreamCategory,
		Data:           event.Data,
		Metadata:       event.Metadata,
		Time:           event.Time,
		Version:        event.MessageVersion,
		GlobalPosition: event.GlobalPosition,
	}

	return msgEnv, nil
}

//MarshalJSON allows for easier debugging (converts the byte slices to strings first)
func (e Event) MarshalJSON() ([]byte, error) {
	holder := map[string]interface{}{
		"id":             e.ID,
		"entityId":       e.EntityID,
		"streamCategory": e.StreamCategory,
		"messageType":    e.MessageType,
		"messageVersion": e.MessageVersion,
		"globalPosition": e.GlobalPosition,
		"data":           json.RawMessage(string(e.Data)),
		"metadata":       json.RawMessage(string(e.Metadata)),
		"time":           e.Time,
	}

	return json.Marshal(holder)
}
