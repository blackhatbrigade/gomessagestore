package repository

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/blackhatbrigade/gomessagestore/message"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore/repository Repository > mocks/repository.go"

//Repository the storage implementation for messagestore
type Repository interface {
	// writes
	WriteMessage(ctx context.Context, message *message.MessageEnvelope) error
	WriteMessageWithExpectedPosition(ctx context.Context, message *message.MessageEnvelope, position int64) error
	// reads from stream
	FindAllMessagesInStream(ctx context.Context, streamID string) ([]*message.MessageEnvelope, error)
	FindAllMessagesInStreamSince(ctx context.Context, streamID string, globalPosition int64) ([]*message.MessageEnvelope, error)
	FindLastMessageInStream(ctx context.Context, streamID string) (*message.MessageEnvelope, error)
	// reads from category
	FindAllMessagesInCategory(ctx context.Context, category string) ([]*message.MessageEnvelope, error)
	FindAllMessagesInCategorySince(ctx context.Context, category string, globalPosition int64) ([]*message.MessageEnvelope, error)
}

//Errors
var (
	ErrInvalidSubscriberID       = errors.New("Subscriber ID cannot be blank")
	ErrInvalidStreamID           = errors.New("Stream ID cannot be blank")
	ErrBlankCategory             = errors.New("Category cannot be blank")
	ErrInvalidCategory           = errors.New("Category cannot contain a hyphen")
	ErrInvalidSubscriberPosition = errors.New("Subscriber position must be greater than or equal to -1")
	ErrNilMessage                = errors.New("Message cannot be nil")
	ErrInvalidPosition           = errors.New("position must be greater than equal to -1")
)
