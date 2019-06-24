package repository

import (
	"context"
	"errors"
)

//go:generate bash -c "${GOPATH}/bin/mockgen github.com/blackhatbrigade/gomessagestore/repository Repository > mocks/repository.go"

//Repository the storage implementation for messagestore
type Repository interface {
	// writes
	WriteMessage(ctx context.Context, message *MessageEnvelope) error
	WriteMessageWithExpectedPosition(ctx context.Context, message *MessageEnvelope, position int64) error
	// reads from stream
	GetAllMessagesInStream(ctx context.Context, streamName string, batchSize int) ([]*MessageEnvelope, error)
	GetAllMessagesInStreamSince(ctx context.Context, streamName string, globalPosition int64, batchSize int) ([]*MessageEnvelope, error)
	GetLastMessageInStream(ctx context.Context, streamName string) (*MessageEnvelope, error)
	// reads from category
	GetAllMessagesInCategory(ctx context.Context, category string, batchSize int) ([]*MessageEnvelope, error)
	GetAllMessagesInCategorySince(ctx context.Context, category string, globalPosition int64, batchSize int) ([]*MessageEnvelope, error)
}

//Errors
var (
	ErrInvalidSubscriberID       = errors.New("Subscriber ID cannot be blank")
	ErrInvalidStreamName         = errors.New("Stream Name cannot be blank")
	ErrBlankCategory             = errors.New("Category cannot be blank")
	ErrInvalidCategory           = errors.New("Category cannot contain a hyphen")
	ErrInvalidSubscriberPosition = errors.New("Subscriber position must be greater than or equal to -1")
	ErrNilMessage                = errors.New("Message cannot be nil")
	ErrInvalidPosition           = errors.New("position must be greater than equal to -1")
)
