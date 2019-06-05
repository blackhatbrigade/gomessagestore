package repository

import (
	"golang.org/x/net/context"
  "time"
  "errors"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore/repository Repository > mocks/repository.go"

//Repository the storage implementation for messagestore
type Repository interface {
	FindAllMessagesSince(ctx context.Context, position int64) ([]*MessageEnvelope, error)
	FindAllMessagesInStream(ctx context.Context, streamID string) ([]*MessageEnvelope, error)
  FindLastMessageInStream(ctx context.Context, streamID string) (*MessageEnvelope, error)
	FindSubscriberPosition(ctx context.Context, subscriberID string) (int64, error)
	SetSubscriberPosition(ctx context.Context, subscriberID string, position int64) error
	WriteMessage(ctx context.Context, message *MessageEnvelope) error
  WriteMessageWithExpectedPosition(ctx context.Context, message *MessageEnvelope, position int64) error
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

var (
	ErrInvalidSubscriberID             = errors.New("Subscriber ID cannot be blank")
	ErrInvalidStreamID                 = errors.New("Stream ID cannot be blank")
	ErrInvalidSubscriberPosition       = errors.New("Subscriber position must be greater than or equal to -1")
	ErrNilMessage                      = errors.New("Message cannot be nil")
	ErrMessageNoID                     = errors.New("Message cannot be written without a new UUID")
	ErrInvalidPosition                 = errors.New("position must be greater than equal to -1")
)
