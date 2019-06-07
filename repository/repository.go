package repository

import (
	"errors"
	"time"

	"golang.org/x/net/context"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore/repository Repository > mocks/repository.go"

//Repository the storage implementation for messagestore
type Repository interface {
	// writes
	WriteMessage(ctx context.Context, message *MessageEnvelope) error
	WriteMessageWithExpectedPosition(ctx context.Context, message *MessageEnvelope, position int64) error
	// reads from stream
	FindAllMessagesInStream(ctx context.Context, streamID string) ([]*MessageEnvelope, error)
	FindAllMessagesInStreamSince(ctx context.Context, streamID string, globalPosition int64) ([]*MessageEnvelope, error)
	FindLastMessageInStream(ctx context.Context, streamID string) (*MessageEnvelope, error)
	// reads from category
	FindAllMessagesInCategory(ctx context.Context, category string) ([]*MessageEnvelope, error)
	FindAllMessagesInCategorySince(ctx context.Context, category string, globalPosition int64) ([]*MessageEnvelope, error)
}

//MessageEnvelope the model for data read from the Message Store
type MessageEnvelope struct {
	GlobalPosition int64     `json:"global_position" db:"global_position"`
	MessageID      string    `json:"message_id" db:"message_id"`
	Type           string    `json:"type" db:"type"`
	Stream         string    `json:"stream" db:"stream"`
	StreamType     string    `json:"stream_type" db:"stream_type"`
	CorrelationID  string    `json:"correlation_id" db:"correlation_id"`
	CausedByID     string    `json:"caused_by_id" db:"caused_by_id"`
	UserID         string    `json:"user_id" db:"user_id"`
	OwnerID        string    `json:"owner_id" db:"owner_id"`
	Position       int64     `json:"position" db:"position"`
	Data           []byte    `json:"data" db:"data"`
	Timestamp      time.Time `json:"timestamp" db:"timestamp"`
}

//Message Defines an interface that can consume Commands or Events.
type Message interface {
	ToEnvelope() (*MessageEnvelope, error)
}

//Errors
var (
	ErrInvalidSubscriberID       = errors.New("Subscriber ID cannot be blank")
	ErrInvalidStreamID           = errors.New("Stream ID cannot be blank")
	ErrBlankCategory             = errors.New("Category cannot be blank")
	ErrInvalidCategory           = errors.New("Category cannot contain a hyphen")
	ErrInvalidSubscriberPosition = errors.New("Subscriber position must be greater than or equal to -1")
	ErrNilMessage                = errors.New("Message cannot be nil")
	ErrMessageNoID               = errors.New("Message cannot be written without a new UUID")
	ErrInvalidPosition           = errors.New("position must be greater than equal to -1")
)
