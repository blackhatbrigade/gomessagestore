package repository

import (
	"errors"

	"golang.org/x/net/context"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore/repository Repository > mocks/repository.go"

//Repository the storage implementation for messagestore
type Repository interface {
	// writes
	WriteMessage(ctx context.Context, message *MessageEnvelope) error
	WriteMessageWithExpectedPosition(ctx context.Context, message *MessageEnvelope, position int64) error
	// reads from stream
	GetAllMessagesInStream(ctx context.Context, streamID string) ([]*MessageEnvelope, error)
	GetAllMessagesInStreamSince(ctx context.Context, streamID string, globalPosition int64) ([]*MessageEnvelope, error)
	GetLastMessageInStream(ctx context.Context, streamID string) (*MessageEnvelope, error)
	// reads from category
	GetAllMessagesInCategory(ctx context.Context, category string) ([]*MessageEnvelope, error)
	GetAllMessagesInCategorySince(ctx context.Context, category string, globalPosition int64) ([]*MessageEnvelope, error)

	// TODO: number of messages returned should be a) capped, and b) optional
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
