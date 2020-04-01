package gomessagestore

import (
	"fmt"
	"time"

	"github.com/blackhatbrigade/gomessagestore/repository"
	"github.com/blackhatbrigade/gomessagestore/uuid"
)

// Command implements the Message interface; returned by get function
type Command struct {
	ID             uuid.UUID // ID for the command
	EntityID       uuid.UUID
	StreamCategory string // Name of the stream category
	MessageType    string // Name of the message type
	MessageVersion int64  // version number of the message
	GlobalPosition int64  // global position of the command
	Data           []byte
	Metadata       []byte
	Time           time.Time
}

func NewCommand(id uuid.UUID, entId uuid.UUID, category string, msgType string, data []byte, metadata []byte) Command {
	cmd := Command{
		ID:             id,
		EntityID:       entId,
		StreamCategory: category,
		MessageType:    msgType,
		Data:           data,
		Metadata:       metadata,
	}

	return cmd
}

// Type returns the type of the command
func (cmd Command) Type() string {
	return cmd.MessageType
}

// Version returns the version of the command
func (cmd Command) Version() int64 {
	return cmd.MessageVersion
}

// Position returns the global position of the command
func (cmd Command) Position() int64 {
	return cmd.GlobalPosition
}

// ToEnvelope converts the command to a Message Envelope that is returned
func (cmd Command) ToEnvelope() (*repository.MessageEnvelope, error) {
	// check to ensure all needed fields on the command are valid
	if cmd.MessageType == "" {
		return nil, ErrMissingMessageType
	}

	if cmd.StreamCategory == "" {
		return nil, ErrMissingMessageCategory
	}

	if cmd.ID == NilUUID {
		return nil, ErrMessageNoID
	}

	if cmd.Data == nil {
		return nil, ErrMissingMessageData
	}

	var msgEnv *repository.MessageEnvelope
	if cmd.EntityID == NilUUID {
		// create a new MessageEnvelope based on the command
		msgEnv = &repository.MessageEnvelope{
			ID:             cmd.ID,
			EntityID:       cmd.EntityID,
			MessageType:    cmd.MessageType,
			StreamName:     fmt.Sprintf("%s:command", cmd.StreamCategory),
			StreamCategory: cmd.StreamCategory,
			Data:           cmd.Data,
			Metadata:       cmd.Metadata,
			Time:           cmd.Time,
			Version:        cmd.MessageVersion,
			GlobalPosition: cmd.GlobalPosition,
		}
	} else {
		msgEnv = &repository.MessageEnvelope{
			ID:             cmd.ID,
			EntityID:       cmd.EntityID,
			MessageType:    cmd.MessageType,
			StreamName:     fmt.Sprintf("%s:command-%s", cmd.StreamCategory, cmd.EntityID),
			StreamCategory: cmd.StreamCategory,
			Data:           cmd.Data,
			Metadata:       cmd.Metadata,
			Time:           cmd.Time,
			Version:        cmd.MessageVersion,
			GlobalPosition: cmd.GlobalPosition,
		}
	}
	return msgEnv, nil
}
